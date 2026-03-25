// 这个文件已经全部加上中文注释
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdfs.server.datanode.checker;

import static org.apache.hadoop.hdfs.DFSConfigKeys.*;

import org.apache.hadoop.thirdparty.com.google.common.collect.Maps;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ListenableFuture;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.StorageLocation;
import org.apache.hadoop.hdfs.server.datanode.StorageLocation.CheckContext;
import org.apache.hadoop.util.DiskChecker.DiskErrorException;
import org.apache.hadoop.util.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * 文件级注释：HDFS DataNode存储目录健康检查工具类，负责在DataNode启动阶段批量验证所有配置的存储位置，
 * 筛选出健康可用的存储目录，并控制可容忍的磁盘故障数量，保证DataNode在部分磁盘故障时仍能正常启动。
 * 该类代码从原DataNode类中提取重构而来，实现异步并发检查提升启动速度。
 *
 * 用于封装DataNode启动阶段对存储位置的健康检查逻辑。
 * 部分代码从DataNode类中提取而来。
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class StorageLocationChecker {
  public static final Logger LOG = LoggerFactory.getLogger(
      StorageLocationChecker.class);
  // 异步检查执行器，负责调度存储位置检查任务
  private final AsyncChecker<CheckContext, VolumeCheckResult> delegateChecker;
  // 计时器，用于计算检查超时
  private final Timer timer;

  /**
   * Max allowed time for a disk check in milliseconds. If the check
   * doesn't complete within this time we declare the disk as dead.
   */
  // 单次磁盘检查最大允许时间（毫秒），超时则判定磁盘故障
  private final long maxAllowedTimeForCheckMs;


  /**
   * Expected filesystem permissions on the storage directory.
   */
  // 存储目录预期的文件权限
  private final FsPermission expectedPermission;

  /**
   * Maximum number of volume failures that can be tolerated without
   * declaring a fatal error.
   */
  // 启动阶段可容忍的最大磁盘故障数量，超过该值则DataNode启动失败
  private final int maxVolumeFailuresTolerated;

  /**
   * 构造函数：从配置中初始化存储位置检查器，加载检查超时、权限、故障容忍数等配置，创建异步检查线程池
   * @param conf HDFS配置对象
   * @param timer 计时器工具
   * @throws DiskErrorException 如果配置参数非法抛出异常
   */
  public StorageLocationChecker(Configuration conf, Timer timer)
      throws DiskErrorException {
    maxAllowedTimeForCheckMs = conf.getTimeDuration(
        DFS_DATANODE_DISK_CHECK_TIMEOUT_KEY,
        DFS_DATANODE_DISK_CHECK_TIMEOUT_DEFAULT,
        TimeUnit.MILLISECONDS);

    if (maxAllowedTimeForCheckMs <= 0) {
      throw new HadoopIllegalArgumentException("Invalid value configured for "
          + DFS_DATANODE_DISK_CHECK_TIMEOUT_KEY + " - "
          + maxAllowedTimeForCheckMs + " (should be > 0)");
    }

    expectedPermission = new FsPermission(
        conf.get(DFS_DATANODE_DATA_DIR_PERMISSION_KEY,
            DFS_DATANODE_DATA_DIR_PERMISSION_DEFAULT));

    maxVolumeFailuresTolerated = conf.getInt(
        DFS_DATANODE_FAILED_VOLUMES_TOLERATED_KEY,
        DFS_DATANODE_FAILED_VOLUMES_TOLERATED_DEFAULT);

    if (maxVolumeFailuresTolerated < DataNode.MAX_VOLUME_FAILURE_TOLERATED_LIMIT) {
      throw new HadoopIllegalArgumentException("Invalid value configured for "
          + DFS_DATANODE_FAILED_VOLUMES_TOLERATED_KEY + " - "
          + maxVolumeFailuresTolerated + " "
          + DataNode.MAX_VOLUME_FAILURES_TOLERATED_MSG);
    }

    this.timer = timer;

    delegateChecker = new ThrottledAsyncChecker<>(
        timer,
        conf.getTimeDuration(
            DFSConfigKeys.DFS_DATANODE_DISK_CHECK_MIN_GAP_KEY,
            DFSConfigKeys.DFS_DATANODE_DISK_CHECK_MIN_GAP_DEFAULT,
            TimeUnit.MILLISECONDS),
        0,
        Executors.newCachedThreadPool(
            new ThreadFactoryBuilder()
                .setNameFormat("StorageLocationChecker thread %d")
                .setDaemon(true)
                .build()));
  }

  /**
   * 批量检查所有配置的存储位置，返回检查后健康可用的存储目录列表
   * 输入和输出顺序保持一致，兼容现有单元测试
   * @param conf HDFS配置对象
   * @param dataDirs 待检查的存储位置集合
   * @return 健康可用的存储位置列表，空列表表示无健康存储目录
   * @throws InterruptedException 检查过程被中断抛出
   * @throws IOException 故障磁盘数超过容忍阈值或无健康磁盘抛出
   */
  public List<StorageLocation> check(
      final Configuration conf,
      final Collection<StorageLocation> dataDirs)
      throws InterruptedException, IOException {

    final HashMap<StorageLocation, Boolean> goodLocations =
        new LinkedHashMap<>();
    final Set<StorageLocation> failedLocations = new HashSet<>();
    final Map<StorageLocation, ListenableFuture<VolumeCheckResult>> futures =
        Maps.newHashMap();
    final LocalFileSystem localFS = FileSystem.getLocal(conf);
    final CheckContext context = new CheckContext(localFS, expectedPermission);

    // 为所有存储位置启动异步并行检查
    for (StorageLocation location : dataDirs) {
      goodLocations.put(location, true);
      Optional<ListenableFuture<VolumeCheckResult>> olf =
          delegateChecker.schedule(location, context);
      if (olf.isPresent()) {
        futures.put(location, olf.get());
      }
    }

    // 检查配置合法性：容忍故障数不能大于等于总磁盘数，否则必然启动失败
    if (maxVolumeFailuresTolerated >= dataDirs.size()) {
      throw new HadoopIllegalArgumentException("Invalid value configured for "
          + DFS_DATANODE_FAILED_VOLUMES_TOLERATED_KEY + " - "
          + maxVolumeFailuresTolerated + ". Value configured is >= "
          + "to the number of configured volumes (" + dataDirs.size() + ").");
    }

    // 记录检查开始时间，用于计算剩余超时时间
    final long checkStartTimeMs = timer.monotonicNow();

    // 遍历所有异步任务，获取检查结果
    for (Map.Entry<StorageLocation,
             ListenableFuture<VolumeCheckResult>> entry : futures.entrySet()) {

      // 计算当前检查剩余可用超时时间，累计不超过最大允许超时
      final long waitSoFarMs = (timer.monotonicNow() - checkStartTimeMs);
      final long timeLeftMs = Math.max(0,
          maxAllowedTimeForCheckMs - waitSoFarMs);
      final StorageLocation location = entry.getKey();

      try {
        // 获取检查结果，等待剩余超时时间
        final VolumeCheckResult result =
            entry.getValue().get(timeLeftMs, TimeUnit.MILLISECONDS);
        // 根据结果处理存储位置状态
        switch (result) {
        case HEALTHY:
          // 健康，保留在合格列表中
          break;
        case DEGRADED:
          // 降级，输出警告但保留
          LOG.warn("StorageLocation {} appears to be degraded.", location);
          break;
        case FAILED:
          // 失败，加入失败列表，从合格列表移除
          LOG.warn("StorageLocation {} detected as failed.", location);
          failedLocations.add(location);
          goodLocations.remove(location);
          break;
        default:
          // 未知结果，输出错误日志
          LOG.error("Unexpected health check result {} for StorageLocation {}",
              result, location);
        }
      } catch (ExecutionException|TimeoutException e) {
        // 执行异常或超时，判定为磁盘失败
        LOG.warn("Exception checking StorageLocation " + location,
            e.getCause());
        failedLocations.add(location);
        goodLocations.remove(location);
      }
    }

    // 检查失败磁盘数是否超过容忍阈值
    if (maxVolumeFailuresTolerated == DataNode.MAX_VOLUME_FAILURE_TOLERATED_LIMIT) {
      // 特殊情况：容忍所有磁盘失败，只有当全部磁盘都失败时才抛出异常
      if (dataDirs.size() == failedLocations.size()) {
        throw new DiskErrorException("Too many failed volumes - "
            + "current valid volumes: " + goodLocations.size()
            + ", volumes configured: " + dataDirs.size()
            + ", volumes failed: " + failedLocations.size()
            + ", volume failures tolerated: " + maxVolumeFailuresTolerated);
      }
    } else {
      // 普通情况：失败数超过容忍阈值则抛出异常
      if (failedLocations.size() > maxVolumeFailuresTolerated) {
        throw new DiskErrorException("Too many failed volumes - "
            + "current valid volumes: " + goodLocations.size()
            + ", volumes configured: " + dataDirs.size()
            + ", volumes failed: " + failedLocations.size()
            + ", volume failures tolerated: " + maxVolumeFailuresTolerated);
      }
    }

    // 无任何合格存储目录，启动失败
    if (goodLocations.size() == 0) {
      throw new DiskErrorException("All directories in "
          + DFS_DATANODE_DATA_DIR_KEY + " are invalid: "
          + failedLocations);
    }

    // 返回合格存储目录列表，保持输入顺序
    return new ArrayList<>(goodLocations.keySet());
  }

  /**
   * 关闭检查器，等待所有正在执行的检查任务完成，释放线程池资源
   * @param gracePeriod 优雅关闭等待时间
   * @param timeUnit 等待时间单位
   */
  public void shutdownAndWait(int gracePeriod, TimeUnit timeUnit) {
    try {
      delegateChecker.shutdownAndWait(gracePeriod, timeUnit);
    } catch (InterruptedException e) {
      LOG.warn("StorageLocationChecker interrupted during shutdown.");
      Thread.currentThread().interrupt();
    }
  }
}