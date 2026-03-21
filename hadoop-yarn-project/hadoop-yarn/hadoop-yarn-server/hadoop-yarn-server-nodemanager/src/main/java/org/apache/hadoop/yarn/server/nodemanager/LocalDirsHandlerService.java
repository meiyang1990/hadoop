// 这个文件已经全部加上中文注释
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.nodemanager;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.util.DiskChecker.DiskErrorException;
import org.apache.hadoop.util.DiskValidator;
import org.apache.hadoop.util.DiskValidatorFactory;
import org.apache.hadoop.yarn.server.nodemanager.health.HealthReporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.nodemanager.DirectoryCollection.DirsChangeListener;
import org.apache.hadoop.yarn.server.nodemanager.metrics.NodeManagerMetrics;

/**
 * 提供NodeManager本地目录健康检查功能，定期检测NM本地工作目录和日志目录的健康状态
 * 管理健康目录列表，为目录分配器提供最新的可用目录信息
 */
public class LocalDirsHandlerService extends AbstractService
    implements HealthReporter {

  private static final Logger LOG =
       LoggerFactory.getLogger(LocalDirsHandlerService.class);

  private static final String diskCapacityExceededErrorMsg =  "usable space is below configured utilization percentage/no more usable space";

  /**
   * Good local directories, use internally,
   * initial value is the same as NM_LOCAL_DIRS.
   */
  @Private
  static final String NM_GOOD_LOCAL_DIRS =
      YarnConfiguration.NM_PREFIX + "good-local-dirs";

  /**
   * Good log directories, use internally,
   * initial value is the same as NM_LOG_DIRS.
   */
  @Private
  static final String NM_GOOD_LOG_DIRS =
      YarnConfiguration.NM_PREFIX + "good-log-dirs";

  /** 调度磁盘健康检查任务的定时器 */
  private Timer dirsHandlerScheduler;
  /** 磁盘健康检查间隔时间（毫秒） */
  private long diskHealthCheckInterval;
  /** 是否启用磁盘健康检查功能 */
  private boolean isDiskHealthCheckerEnabled;
  /**
   * 节点被判定为健康所需的最小健康磁盘比例，同时适用于本地工作目录和日志目录
   */
  private float minNeededHealthyDisksFactor;

  private MonitoringTimerTask monitoringTimerTask;

  /** 存储本地化文件的本地目录集合 */
  private DirectoryCollection localDirs = null;

  /** 存储容器日志的目录集合 */
  private DirectoryCollection logDirs = null;

  /**
   * 本地目录分配器，所有对NM_LOCAL_DIRS的读写都应通过此实例，避免重复创建分配器
   */ 
  private LocalDirAllocator localDirsAllocator;
  /**
   * 日志目录分配器，所有对NM_LOG_DIRS的读写都应通过此实例，避免重复创建分配器
   */ 
  private LocalDirAllocator logDirsAllocator;

  /** 上次执行磁盘健康检查的时间戳 */
  private long lastDisksCheckTime;
  
  private static String FILE_SCHEME = "file";

  /** NodeManager指标收集器 */
  private NodeManagerMetrics nodeManagerMetrics = null;

  /**
   * 被定时器调用，定期执行磁盘健康检查任务的内部类
   */
  private final class MonitoringTimerTask extends TimerTask {

    public MonitoringTimerTask(Configuration conf) throws YarnRuntimeException {
      // 获取磁盘利用率高水位阈值
      float highUsableSpacePercentagePerDisk =
          conf.getFloat(
            YarnConfiguration.NM_MAX_PER_DISK_UTILIZATION_PERCENTAGE,
            YarnConfiguration.DEFAULT_NM_MAX_PER_DISK_UTILIZATION_PERCENTAGE);
      // 获取磁盘利用率低水位阈值（用于工作预留）
      float lowUsableSpacePercentagePerDisk =
          conf.getFloat(
              YarnConfiguration.NM_WM_LOW_PER_DISK_UTILIZATION_PERCENTAGE,
              highUsableSpacePercentagePerDisk);
      // 参数校验：低水位不能高于高水位，不合法则使用高水位值
      if (lowUsableSpacePercentagePerDisk > highUsableSpacePercentagePerDisk) {
        LOG.warn("Using " + YarnConfiguration.
            NM_MAX_PER_DISK_UTILIZATION_PERCENTAGE + " as " +
            YarnConfiguration.NM_WM_LOW_PER_DISK_UTILIZATION_PERCENTAGE +
            ", because " + YarnConfiguration.
            NM_WM_LOW_PER_DISK_UTILIZATION_PERCENTAGE +
            " is not configured properly.");
        lowUsableSpacePercentagePerDisk = highUsableSpacePercentagePerDisk;
      }
      // 获取最低空闲空间低水位阈值（MB）
      long lowMinFreeSpacePerDiskMB =
          conf.getLong(YarnConfiguration.NM_MIN_PER_DISK_FREE_SPACE_MB,
              YarnConfiguration.DEFAULT_NM_MIN_PER_DISK_FREE_SPACE_MB);
      // 获取最低空闲空间高水位阈值（MB）
      long highMinFreeSpacePerDiskMB =
          conf.getLong(YarnConfiguration.NM_WM_HIGH_PER_DISK_FREE_SPACE_MB,
              lowMinFreeSpacePerDiskMB);
      // 参数校验：高水位不能低于低水位，不合法则使用低水位值
      if (highMinFreeSpacePerDiskMB < lowMinFreeSpacePerDiskMB) {
        LOG.warn("Using " + YarnConfiguration.
            NM_MIN_PER_DISK_FREE_SPACE_MB + " as " +
            YarnConfiguration.NM_WM_HIGH_PER_DISK_FREE_SPACE_MB +
            ", because " + YarnConfiguration.
            NM_WM_HIGH_PER_DISK_FREE_SPACE_MB +
            " is not configured properly.");
        highMinFreeSpacePerDiskMB = lowMinFreeSpacePerDiskMB;
      }

      // 初始化本地工作目录集合
      localDirs =
          new DirectoryCollection(
              validatePaths(conf
                  .getTrimmedStrings(YarnConfiguration.NM_LOCAL_DIRS)),
              highUsableSpacePercentagePerDisk,
              lowUsableSpacePercentagePerDisk,
              lowMinFreeSpacePerDiskMB,
              highMinFreeSpacePerDiskMB);
      // 初始化日志目录集合
      logDirs =
          new DirectoryCollection(
              validatePaths(conf
                  .getTrimmedStrings(YarnConfiguration.NM_LOG_DIRS)),
              highUsableSpacePercentagePerDisk,
              lowUsableSpacePercentagePerDisk,
              lowMinFreeSpacePerDiskMB,
              highMinFreeSpacePerDiskMB);

      // 将初始完整目录列表写入配置，供分配器使用
      String local = conf.get(YarnConfiguration.NM_LOCAL_DIRS);
      conf.set(NM_GOOD_LOCAL_DIRS,
          (local != null) ? local : "");
      // 获取磁盘校验器实现类名
      String diskValidatorName = conf.get(YarnConfiguration.DISK_VALIDATOR,
              YarnConfiguration.DEFAULT_DISK_VALIDATOR);
      try {
        // 创建磁盘校验器实例
        DiskValidator diskValidator =
            DiskValidatorFactory.getInstance(diskValidatorName);
        // 初始化本地目录分配器
        localDirsAllocator = new LocalDirAllocator(
                NM_GOOD_LOCAL_DIRS, diskValidator);
        String log = conf.get(YarnConfiguration.NM_LOG_DIRS);
        conf.set(NM_GOOD_LOG_DIRS,
                (log != null) ? log : "");
        // 初始化日志目录分配器
        logDirsAllocator = new LocalDirAllocator(
                NM_GOOD_LOG_DIRS, diskValidator);
      } catch (DiskErrorException e) {
        throw new YarnRuntimeException(
            "Failed to create DiskValidator of type " + diskValidatorName + "!",
            e);
      }
    }

    @Override
    public void run() {
      try {
        // 执行磁盘健康检查
        checkDirs();
      } catch (Throwable t) {
        // 捕获异常避免终止定时器线程
        LOG.warn("Error while checking local directories: ", t);
      }
    }
  }

  /**
   * 无参构造函数
   */
  public LocalDirsHandlerService() {
    this(null);
  }

  /**
   * 带指标收集器的构造函数
   * @param nodeManagerMetrics NodeManager指标收集器
   */
  public LocalDirsHandlerService(NodeManagerMetrics nodeManagerMetrics) {
    super(LocalDirsHandlerService.class.getName());
    this.nodeManagerMetrics = nodeManagerMetrics;
  }

  /**
   * 服务初始化方法，创建检查任务并初始化目录
   */
  @Override
  protected void serviceInit(Configuration config) throws Exception {
    // 克隆配置，因为我们会修改其中的目录列表配置
    Configuration conf = new Configuration(config);
    // 读取磁盘健康检查间隔配置
    diskHealthCheckInterval = conf.getLong(
        YarnConfiguration.NM_DISK_HEALTH_CHECK_INTERVAL_MS,
        YarnConfiguration.DEFAULT_NM_DISK_HEALTH_CHECK_INTERVAL_MS);
    // 创建检查任务实例
    monitoringTimerTask = new MonitoringTimerTask(conf);
    // 读取是否启用磁盘健康检查配置
    isDiskHealthCheckerEnabled = conf.getBoolean(
        YarnConfiguration.NM_DISK_HEALTH_CHECK_ENABLE, true);
    // 读取最小健康磁盘比例配置
    minNeededHealthyDisksFactor = conf.getFloat(
        YarnConfiguration.NM_MIN_HEALTHY_DISKS_FRACTION,
        YarnConfiguration.DEFAULT_NM_MIN_HEALTHY_DISKS_FRACTION);
    // 记录初始化时间作为初始检查时间
    lastDisksCheckTime = System.currentTimeMillis();
    super.serviceInit(conf);

    FileContext localFs;
    try {
      // 获取本地文件上下文
      localFs = FileContext.getLocalFSFileContext(config);
    } catch (IOException e) {
      throw new YarnRuntimeException("Unable to get the local filesystem", e);
    }
    // 设置目录权限为0755
    FsPermission perm = new FsPermission((short)0755);
    // 创建不存在的目录
    boolean createSucceeded = localDirs.createNonExistentDirs(localFs, perm);
    createSucceeded &= logDirs.createNonExistentDirs(localFs, perm);
    // 如果创建目录失败，更新配置中的健康目录列表
    if (!createSucceeded) {
      updateDirsAfterTest();
    }

    // 立即执行一次磁盘健康检查，在其他组件使用目录前过滤出坏盘
    checkDirs();
  }

  /**
   * 服务启动方法，启动定时磁盘健康检查
   */
  @Override
  protected void serviceStart() throws Exception {
    if (isDiskHealthCheckerEnabled) {
      // 创建后台定时线程
      dirsHandlerScheduler = new Timer("DiskHealthMonitor-Timer", true);
      // 按固定间隔调度检查任务
      dirsHandlerScheduler.scheduleAtFixedRate(monitoringTimerTask,
          diskHealthCheckInterval, diskHealthCheckInterval);
    }
    super.serviceStart();
  }

  /**
   * 服务停止方法，终止定时检查
   */
  @Override
  protected void serviceStop() throws Exception {
    if (dirsHandlerScheduler != null) {
      // 取消定时器
      dirsHandlerScheduler.cancel();
    }
    super.serviceStop();
  }

  /**
   * 注册本地目录变更监听器
   * @param listener 目录变更监听器
   */
  public void registerLocalDirsChangeListener(DirsChangeListener listener) {
    localDirs.registerDirsChangeListener(listener);
  }

  /**
   * 注册日志目录变更监听器
   * @param listener 目录变更监听器
   */
  public void registerLogDirsChangeListener(DirsChangeListener listener) {
    logDirs.registerDirsChangeListener(listener);
  }

  /**
   * 注销本地目录变更监听器
   * @param listener 目录变更监听器
   */
  public void deregisterLocalDirsChangeListener(DirsChangeListener listener) {
    localDirs.deregisterDirsChangeListener(listener);
  }

  /**
   * 注销日志目录变更监听器
   * @param listener 目录变更监听器
   */
  public void deregisterLogDirsChangeListener(DirsChangeListener listener) {
    logDirs.deregisterDirsChangeListener(listener);
  }

  /**
   * @return 当前健康的本地工作目录列表
   */
  public List<String> getLocalDirs() {
    return localDirs.getGoodDirs();
  }

  /**
   * @return 当前健康的日志目录列表
   */
  public List<String> getLogDirs() {
    return logDirs.getGoodDirs();
  }

  /**
   * @return 容量已满的本地工作目录列表
   */
  public List<String> getDiskFullLocalDirs() {
    return localDirs.getFullDirs();
  }

  /**
   * @return 容量已满的日志目录列表
   */
  public List<String> getDiskFullLogDirs() {
    return logDirs.getFullDirs();
  }

  /**
   * 获取可用于读取已有文件的本地目录列表，包含健康目录和已满目录
   * @return 可读本地目录列表
   */
  public List<String> getLocalDirsForRead() {
    return DirectoryCollection.concat(localDirs.getGoodDirs(),
        localDirs.getFullDirs());
  }

  /**
   * 获取可用于资源清理的本地目录列表，包含健康目录和已满目录
   * @return 可清理本地目录列表
   */
  public List<String> getLocalDirsForCleanup() {
    return DirectoryCollection.concat(localDirs.getGoodDirs(),
        localDirs.getFullDirs());
  }

  /**
   * 获取可用于读取已有日志文件的目录列表，包含健康目录和已满目录
   * @return 可读日志目录列表
   */
  public List<String> getLogDirsForRead() {
    return DirectoryCollection.concat(logDirs.getGoodDirs(),
        logDirs.getFullDirs());
  }

  /**
   * 获取可用于资源清理的日志目录列表，包含健康目录和已满目录
   * @return 可清理日志目录列表
   */
  public List<String> getLogDirsForCleanup() {
    return DirectoryCollection.concat(logDirs.getGoodDirs(),
        logDirs.getFullDirs());
  }

  /**
   * 生成磁盘健康状态报告
   * @param listGoodDirs true返回健康目录信息，false返回异常目录信息
   * @return 格式化的健康报告字符串
   */
  public String getDisksHealthReport(boolean listGoodDirs) {
    if (!isDiskHealthCheckerEnabled) {
      return "";
    }

    StringBuilder report = new StringBuilder();
    List<String> erroredLocalDirsList = localDirs.getErroredDirs();
    List<String> erroredLogDirsList = logDirs.getErroredDirs();
    List<String> diskFullLocalDirsList = localDirs.getFullDirs();
    List<String> diskFullLogDirsList = logDirs.getFullDirs();
    List<String> goodLocalDirsList = localDirs.getGoodDirs();
    List<String> goodLogDirsList = logDirs.getGoodDirs();

    int numLocalDirs = goodLocalDirsList.size() + erroredLocalDirsList.size() + diskFullLocalDirsList.size();
    int numLogDirs = goodLogDirsList.size() + erroredLogDirsList.size() + diskFullLogDirsList.size();
    if (!listGoodDirs) {
      if (!erroredLocalDirsList.isEmpty()) {
        report.append(erroredLocalDirsList.size() + "/" + numLocalDirs
            + " local-dirs have errors: "
            + buildDiskErrorReport(erroredLocalDirsList, localDirs));
      }
      if (!diskFullLocalDirsList.isEmpty()) {
        report.append(diskFullLocalDirsList.size() + "/" + numLocalDirs
            + " local-dirs " + diskCapacityExceededErrorMsg
            + buildDiskErrorReport(diskFullLocalDirsList, localDirs) + "; ");
      }

      if (!erroredLogDirsList.isEmpty()) {
        report.append(erroredLogDirsList.size() + "/" + numLogDir