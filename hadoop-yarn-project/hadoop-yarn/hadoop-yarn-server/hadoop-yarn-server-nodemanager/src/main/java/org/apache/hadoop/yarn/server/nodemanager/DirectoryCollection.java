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
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.DiskChecker;
import org.apache.hadoop.util.DiskValidator;
import org.apache.hadoop.util.DiskValidatorFactory;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableList;

/**
 * 管理NodeManager本地存储目录集合，维护健康目录和故障目录列表，定期检查目录健康状态
 */
public class DirectoryCollection {
  private static final Logger LOG =
       LoggerFactory.getLogger(DirectoryCollection.class);

  private final Configuration conf;
  private final DiskValidator diskValidator;

  private boolean diskUtilizationThresholdEnabled;
  private boolean diskFreeSpaceThresholdEnabled;
  private boolean subAccessibilityValidationEnabled;
  /**
   * 磁盘错误类型枚举
   */
  public enum DiskErrorCause {
    DISK_FULL, OTHER
  }

  /**
   * 存储磁盘错误信息，包含错误原因和错误描述
   */
  static class DiskErrorInformation {
    DiskErrorCause cause;
    String message;

    DiskErrorInformation(DiskErrorCause cause, String message) {
      this.cause = cause;
      this.message = message;
    }
  }

  /**
   * 目录列表变更回调接口，当健康目录列表发生变化时触发通知
   */
  public interface DirsChangeListener {
    void onDirsChanged();
  }

  /**
   * 合并两个字符串列表为新列表
   * @param l1 第一个列表
   * @param l2 第二个列表
   * @return 合并后的新列表
   */
  static List<String> concat(List<String> l1, List<String> l2) {
    List<String> ret = new ArrayList<String>(l1.size() + l2.size());
    ret.addAll(l1);
    ret.addAll(l2);
    return ret;
  }

  // 状态正常可用的本地存储目录
  private List<String> localDirs;
  // 发生错误（非满盘）的目录
  private List<String> errorDirs;
  // 磁盘空间已满的目录
  private List<String> fullDirs;
  // 存储每个故障目录的错误信息
  private Map<String, DiskErrorInformation> directoryErrorInfo;

  // 目录列表读写锁，保证并发访问安全
  private final ReadLock readLock;
  private final WriteLock writeLock;

  // 累计发生的目录故障总数
  private int numFailures;

  // 磁盘使用率高低阈值（百分比）
  private float diskUtilizationPercentageCutoffHigh;
  private float diskUtilizationPercentageCutoffLow;
  // 可用空间高低阈值（MB）
  private long diskFreeSpaceCutoffLow;
  private long diskFreeSpaceCutoffHigh;

  // 所有健康目录的平均磁盘使用率
  private int goodDirsDiskUtilizationPercentage;

  // 目录变更监听器集合
  private Set<DirsChangeListener> dirsChangeListeners;

  /**
   * 构造目录集合，不检查磁盘空间
   * 
   * @param dirs 需要监控的目录数组
   */
  public DirectoryCollection(String[] dirs) {
    this(dirs, 100.0F, 100.0F, 0, 0);
  }

  /**
   * 构造目录集合，指定最大磁盘使用率阈值，不检查最小可用空间
   * 
   * @param dirs 需要监控的目录数组
   * @param utilizationPercentageCutOff 目录被移出健康列表的磁盘使用率阈值
   * 
   */
  public DirectoryCollection(String[] dirs, float utilizationPercentageCutOff) {
    this(dirs, utilizationPercentageCutOff, utilizationPercentageCutOff, 0, 0);
  }

  /**
   * 构造目录集合，指定最小可用空间阈值，不检查使用率
   * 
   * @param dirs 需要监控的目录数组
   * @param utilizationSpaceCutOff 目录标记为健康所需的最小可用空间（MB）
   * 
   */
  public DirectoryCollection(String[] dirs, long utilizationSpaceCutOff) {
    this(dirs, 100.0F, 100.0F, utilizationSpaceCutOff, utilizationSpaceCutOff);
  }

  /**
   * 构造目录集合，指定可用空间的高低阈值
   *
   * @param dirs 需要监控的目录数组
   * @param utilizationSpaceCutOffLow 目录移出健康列表的最小可用空间阈值（MB）
   * @param utilizationSpaceCutOffHigh 目录从故障列表转回健康列表的最小可用空间阈值（MB）
   */
  public DirectoryCollection(String[] dirs, long utilizationSpaceCutOffLow,
      long utilizationSpaceCutOffHigh) {
    this(dirs, 100.0F, 100.0F, utilizationSpaceCutOffLow,
        utilizationSpaceCutOffHigh);
  }

  /**
   * 构造目录集合，指定使用率阈值和空间阈值，使用同一阈值处理进出健康列表
   *
   * @param dirs 需要监控的目录数组
   * @param utilizationPercentageCutOffHigh 目录移出健康列表的磁盘使用率阈值
   * @param utilizationPercentageCutOffLow 目录从故障列表转回健康列表的磁盘使用率阈值
   * @param utilizationSpaceCutOff 目录标记为健康所需的最小可用空间（MB）
   */
  public DirectoryCollection(String[] dirs,
      float utilizationPercentageCutOffHigh,
      float utilizationPercentageCutOffLow, long utilizationSpaceCutOff) {
    this(dirs, utilizationPercentageCutOffHigh,
        utilizationPercentageCutOffLow, utilizationSpaceCutOff,
        utilizationSpaceCutOff);
  }

  /**
   * 完整构造目录集合，分别指定使用率和可用空间的高低阈值
   *
   * @param dirs 需要监控的目录数组
   * @param utilizationPercentageCutOffHigh 目录移出健康列表的磁盘使用率阈值
   * @param utilizationPercentageCutOffLow 目录从故障列表转回健康列表的磁盘使用率阈值
   * @param utilizationSpaceCutOffLow 目录移出健康列表的最小可用空间阈值（MB）
   * @param utilizationSpaceCutOffHigh 目录从故障列表转回健康列表的最小可用空间阈值（MB）
   */
  public DirectoryCollection(String[] dirs,
      float utilizationPercentageCutOffHigh,
      float utilizationPercentageCutOffLow,
      long utilizationSpaceCutOffLow,
      long utilizationSpaceCutOffHigh) {
    conf = new YarnConfiguration();
    try {
      // 从配置加载磁盘检测器实现类
      String diskValidatorName = conf.get(YarnConfiguration.DISK_VALIDATOR,
          YarnConfiguration.DEFAULT_DISK_VALIDATOR);
      diskValidator = DiskValidatorFactory.getInstance(diskValidatorName);
      LOG.info("Disk Validator '" + diskValidatorName + "' is loaded.");
    } catch (Exception e) {
      throw new YarnRuntimeException(e);
    }

    // 加载各项检查功能配置开关
    diskUtilizationThresholdEnabled = conf.getBoolean(
        YarnConfiguration.NM_DISK_UTILIZATION_THRESHOLD_ENABLED,
        YarnConfiguration.DEFAULT_NM_DISK_UTILIZATION_THRESHOLD_ENABLED);
    diskFreeSpaceThresholdEnabled = conf.getBoolean(
        YarnConfiguration.NM_DISK_FREE_SPACE_THRESHOLD_ENABLED,
        YarnConfiguration.DEFAULT_NM_DISK_FREE_SPACE_THRESHOLD_ENABLED);
    subAccessibilityValidationEnabled = conf.getBoolean(
        YarnConfiguration.NM_WORKING_DIR_CONTENT_ACCESSIBILITY_VALIDATION_ENABLED,
        YarnConfiguration.DEFAULT_NM_WORKING_DIR_CONTENT_ACCESSIBILITY_VALIDATION_ENABLED);

    // 初始化目录分类列表
    localDirs = new ArrayList<>(Arrays.asList(dirs));
    errorDirs = new ArrayList<>();
    fullDirs = new ArrayList<>();
    directoryErrorInfo = new ConcurrentHashMap<>();

    // 初始化读写锁
    ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    this.readLock = lock.readLock();
    this.writeLock = lock.writeLock();

    // 设置阈值参数
    setDiskUtilizationPercentageCutoff(utilizationPercentageCutOffHigh,
        utilizationPercentageCutOffLow);
    setDiskUtilizationSpaceCutoff(utilizationSpaceCutOffLow,
        utilizationSpaceCutOffHigh);

    // 初始化监听器集合，支持并发访问
    dirsChangeListeners = Collections.newSetFromMap(
        new ConcurrentHashMap<DirsChangeListener, Boolean>());
  }

  /**
   * 注册目录变更监听器，注册完成后立即触发一次变更通知
   * @param listener 监听器对象
   */
  void registerDirsChangeListener(
      DirsChangeListener listener) {
    if (dirsChangeListeners.add(listener)) {
      listener.onDirsChanged();
    }
  }

  /**
   * 注销目录变更监听器
   * @param listener 监听器对象
   */
  void deregisterDirsChangeListener(
      DirsChangeListener listener) {
    dirsChangeListeners.remove(listener);
  }

  /**
   * 获取当前所有健康可用目录的不可变列表
   * @return 健康目录列表
   */
  List<String> getGoodDirs() {
    this.readLock.lock();
    try {
      return ImmutableList.copyOf(localDirs);
    } finally {
      this.readLock.unlock();
    }
  }

  /**
   * 获取所有故障目录（包含错误目录和满盘目录）的不可变列表
   * @return 故障目录列表
   */
  List<String> getFailedDirs() {
    this.readLock.lock();
    try {
      return Collections.unmodifiableList(
          DirectoryCollection.concat(errorDirs, fullDirs));
    } finally {
      this.readLock.unlock();
    }
  }

  /**
   * 获取所有空间已满目录的不可变列表
   * @return 满盘目录列表
   */
  List<String> getFullDirs() {
    this.readLock.lock();
    try {
      return ImmutableList.copyOf(fullDirs);
    } finally {
      this.readLock.unlock();
    }
  }

  /**
   * 获取所有发生错误（非满盘）目录的不可变列表
   * @return 错误目录列表
   */
  @InterfaceStability.Evolving
  List<String> getErroredDirs() {
    this.readLock.lock();
    try {
      return ImmutableList.copyOf(errorDirs);
    } finally {
      this.readLock.unlock();
    }
  }

  /**
   * 获取累计发生的目录故障总数
   * @return 累计故障数
   */
  int getNumFailures() {
    this.readLock.lock();
    try {
      return numFailures;
    }finally {
      this.readLock.unlock();
    }
  }

  /**
   * 获取指定目录的错误诊断信息
   * @param dirName 目录绝对路径
   * @return 错误信息，如果目录健康则返回null
   */
  @InterfaceStability.Evolving
  DiskErrorInformation getDirectoryErrorInfo(String dirName) {
    this.readLock.lock();
    try {
      return directoryErrorInfo.get(dirName);
    } finally {
      this.readLock.unlock();
    }
  }

  /**
   * 检查指定目录关联的磁盘是否不健康
   * @param dirName 目录绝对路径
   * @return true表示磁盘不健康，false表示健康
   */
  @InterfaceStability.Evolving
  boolean isDiskUnHealthy(String dirName) {
    this.readLock.lock();
    try {
      return directoryErrorInfo.containsKey(dirName);
    } finally {
      this.readLock.unlock();
    }
  }

  /**
   * 创建不存在的目录及其父目录，如有必要更新健康目录列表
   * @param localFs 本地文件系统上下文
   * @param perm 创建目录使用的权限
   * @return 所有目录都创建成功返回true，至少一个创建失败返回false
   */
  boolean createNonExistentDirs(FileContext localFs,
      FsPermission perm) {
    boolean failed = false;
    List<String> localDirectories = null;
    this.readLock.lock();
    try {
      // 复制当前健康目录列表，避免长时间持有读锁
      localDirectories = new ArrayList<>(localDirs);
    } finally {
      this.readLock.unlock();
    }
    // 遍历创建每个目录
    for (final String dir : localDirectories) {
      try {
        createDir(localFs, new Path(dir), perm);
      } catch (IOException e) {
        // 创建失败，将目录移出健康列表
        LOG.warn("Unable to create directory " + dir + " error " +
            e.getMessage() + ", removing from the list of valid directories.");
        this.writeLock.lock();
        try {
          localDirs.remove(dir);
          errorDirs.add(dir);
          directoryErrorInfo.put(dir,
              new DiskErrorInformation(DiskErrorCause.OTHER,
                  "Cannot create directory : " + dir + ", error " + e.getMessage()));
          numFailures++;
        } finally {
          this.writeLock.unlock();
        }
        failed = true;
      }
    }
    return !failed;
  }

  /**
   * 检查所有目录（包含健康和故障）的健康状态，如有必要更新健康目录列表
   *
   * @return 本次检查发现目录状态变更返回true，否则返回false
   */
  boolean checkDirs() {
    boolean setChanged = false;
    Set<String> preCheckGoodDirs = null;
    Set<String> preCheckFullDirs = null;
    Set<String> preCheckOtherErrorDirs = null;
    List<String> failedDirs = null;
    List<String> allLocalDirs = null;
    this.readLock.lock();
    try {
      // 保存检查前状态，用于后续比较变更
      preCheckGoodDirs = new HashSet<String>(localDirs);
      preCheckFullDirs = new HashSet<String>(fullDirs);
      preCheckOtherErrorDirs = new HashSet<String>(errorDirs);
      failedDirs = DirectoryCollection.concat(errorDirs, fullDirs);
      allLocalDirs = DirectoryCollection.concat(localDirs, failedDirs);
    } finally {
      this.readLock.unlock();
    }

    // 目录检查在锁外执行，避免IO阻塞长时间占用锁
    Map<String, DiskErrorInformation> dirsFailedCheck = testDirs(allLocalDirs, preCheckGoodDirs);

    this.writeLock.lock();
    try {
      // 清空现有分类，重新划分
      localDirs.clear();
      errorDirs.clear();
      fullDirs.clear();
      directoryErrorInfo.clear();

      // 按错误原因分类故障目录
      for (Map.Entry<String, DiskErrorInformation> entry : dirsFailedCheck
          .entrySet()) {
        String dir = entry.getKey();
        DiskErrorInformation errorInformation = entry.getValue();

        switch (entry.getValue().cause) {
        case DISK_FULL:
          fullDirs.add(entry.getKey());
          break;
        case OTHER:
          errorDirs.add(entry.getKey());
          break;
        default:
          LOG.warn(entry.getValue().cause + " is unknown for disk error.");
          break;
        }
        directoryErrorInfo.put(entry.getKey(), errorInformation);

        // 原本健康的目录变成故障，计数加1并标记变更
        if (preCheckGoodDirs.contains(dir)) {
          LOG.warn("Directory " + dir + " error, " + errorInformation.message
              + ", removing from list of valid directories");
          setChanged = true;
          numFailures++;
        }
      }
      //