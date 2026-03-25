// 这个文件已经全部加上中文注释
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package org.apache.hadoop.hdfs.server.datanode;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.util.Preconditions;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi
    .FsVolumeReferences;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.server.datanode.DiskBalancerWorkStatus
    .DiskBalancerWorkEntry;
import org.apache.hadoop.hdfs.server.datanode.DiskBalancerWorkStatus.Result;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;
import org.apache.hadoop.hdfs.server.diskbalancer.DiskBalancerConstants;
import org.apache.hadoop.hdfs.server.diskbalancer.DiskBalancerException;
import org.apache.hadoop.hdfs.server.diskbalancer.planner.NodePlan;
import org.apache.hadoop.hdfs.server.diskbalancer.planner.Step;
import org.apache.hadoop.hdfs.web.JsonUtil;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 文件级注释：DataNode磁盘均衡器核心执行器，负责接收并执行客户端提交的磁盘均衡计划，
 * 在DataNode本地不同磁盘间移动数据块，实现磁盘使用率均衡，保障集群存储负载分布均匀。
 * 
 * Worker class for Disk Balancer.
 * <p>
 * Here is the high level logic executed by this class. Users can submit disk
 * balancing plans using submitPlan calls. After a set of sanity checks the plan
 * is admitted and put into workMap.
 * <p>
 * The executePlan launches a thread that picks up work from workMap and hands
 * it over to the BlockMover#copyBlocks function.
 * <p>
 * Constraints :
 * <p>
 * Only one plan can be executing in a datanode at any given time. This is
 * ensured by checking the future handle of the worker thread in submitPlan.
 */
@InterfaceAudience.Private
public class DiskBalancer {

  @VisibleForTesting
  public static final Logger LOG = LoggerFactory.getLogger(DiskBalancer
      .class);
  private final FsDatasetSpi<?> dataset;
  private final String dataNodeUUID;
  private final BlockMover blockMover;
  private final ReentrantLock lock;
  private final ConcurrentHashMap<VolumePair, DiskBalancerWorkItem> workMap;
  private volatile boolean isDiskBalancerEnabled = false;
  private ExecutorService scheduler;
  private Future future;
  private String planID;
  private String planFile;
  private DiskBalancerWorkStatus.Result currentResult;
  private long bandwidth;
  private volatile long planValidityInterval;
  private final Configuration config;

  /**
   * 构造磁盘均衡器实例，初始化配置、线程池和工作队列，准备执行均衡计划。
   * Constructs a Disk Balancer object. This object takes care of reading a
   * NodePlan and executing it against a set of volumes.
   *
   * @param dataNodeUUID - Data node UUID
   * @param conf         - Hdfs Config
   * @param blockMover   - Object that supports moving blocks.
   */
  public DiskBalancer(String dataNodeUUID,
                      Configuration conf, BlockMover blockMover) {
    this.config = conf;
    this.currentResult = Result.NO_PLAN;
    this.blockMover = blockMover;
    this.dataset = this.blockMover.getDataset();
    this.dataNodeUUID = dataNodeUUID;
    scheduler = Executors.newSingleThreadExecutor();
    lock = new ReentrantLock();
    workMap = new ConcurrentHashMap<>();
    this.planID = "";  // to keep protobuf happy.
    this.planFile = "";  // to keep protobuf happy.
    this.isDiskBalancerEnabled = conf.getBoolean(
        DFSConfigKeys.DFS_DISK_BALANCER_ENABLED,
        DFSConfigKeys.DFS_DISK_BALANCER_ENABLED_DEFAULT);
    this.bandwidth = conf.getInt(
        DFSConfigKeys.DFS_DISK_BALANCER_MAX_DISK_THROUGHPUT,
        DFSConfigKeys.DFS_DISK_BALANCER_MAX_DISK_THROUGHPUT_DEFAULT);
    this.planValidityInterval = conf.getTimeDuration(
        DFSConfigKeys.DFS_DISK_BALANCER_PLAN_VALID_INTERVAL,
        DFSConfigKeys.DFS_DISK_BALANCER_PLAN_VALID_INTERVAL_DEFAULT,
        TimeUnit.MILLISECONDS);
  }

  /**
   * 关闭磁盘均衡器服务，停止正在执行的计划，释放线程资源。
   * Shutdown  disk balancer services.
   */
  public void shutdown() {
    lock.lock();
    boolean needShutdown = false;
    try {
      this.isDiskBalancerEnabled = false;
      this.currentResult = Result.NO_PLAN;
      if ((this.future != null) && (!this.future.isDone())) {
        this.currentResult = Result.PLAN_CANCELLED;
        this.blockMover.setExitFlag();
        scheduler.shutdown();
        needShutdown = true;
      }
    } finally {
      lock.unlock();
    }
    // 不需要持有锁关闭执行器，减少锁占用时间
    if (needShutdown) {
      shutdownExecutor();
    }
  }

  /**
   * 实际关闭执行器线程池，等待终止超时后强制关闭。
   * Shutdown the executor.
   */
  private void shutdownExecutor() {
    final int secondsTowait = 10;
    try {
      if (!scheduler.awaitTermination(secondsTowait, TimeUnit.SECONDS)) {
        scheduler.shutdownNow();
        if (!scheduler.awaitTermination(secondsTowait, TimeUnit.SECONDS)) {
          LOG.error("Disk Balancer : Scheduler did not terminate.");
        }
      }
    } catch (InterruptedException ex) {
      scheduler.shutdownNow();
      Thread.currentThread().interrupt();
    }
  }

  /**
   * 接收客户端提交的均衡计划，完成合法性校验后转换为工作项，提交执行。
   * Takes a client submitted plan and converts into a set of work items that
   * can be executed by the blockMover.
   *
   * @param planId      - A SHA-1 of the plan string
   * @param planVersion - version of the plan string - for future use.
   * @param planFileName    - Plan file name
   * @param planData    - Plan data in json format
   * @param force       - Skip some validations and execute the plan file.
   * @throws DiskBalancerException
   */
  public void submitPlan(String planId, long planVersion, String planFileName,
                         String planData, boolean force)
          throws DiskBalancerException {
    lock.lock();
    try {
      checkDiskBalancerEnabled();
      if ((this.future != null) && (!this.future.isDone())) {
        LOG.error("Disk Balancer - Executing another plan (Plan File: {}, Plan ID: {}), " +
            "submitPlan failed.", planFile, planID);
        throw new DiskBalancerException("Executing another plan",
            DiskBalancerException.Result.PLAN_ALREADY_IN_PROGRESS);
      }
      NodePlan nodePlan = verifyPlan(planId, planVersion, planData, force);
      createWorkPlan(nodePlan);
      this.planID = planId;
      this.planFile = planFileName;
      this.currentResult = Result.PLAN_UNDER_PROGRESS;
      executePlan();
    } finally {
      lock.unlock();
    }
  }

  /**
   * 根据存储UUID获取对应的FsVolume实例。
   * Get FsVolume by volume UUID.
   * @param fsDataset FsDataset实例
   * @param volUuid 目标卷UUID
   * @return FsVolumeSpi
   */
  private static FsVolumeSpi getFsVolume(final FsDatasetSpi<?> fsDataset,
      final String volUuid) {
    FsVolumeSpi fsVolume = null;
    try (FsVolumeReferences volumeReferences =
           fsDataset.getFsVolumeReferences()) {
      for (int i = 0; i < volumeReferences.size(); i++) {
        if (volumeReferences.get(i).getStorageID().equals(volUuid)) {
          fsVolume = volumeReferences.get(i);
          break;
        }
      }
    } catch (IOException e) {
      LOG.warn("Disk Balancer - Error when closing volume references: ", e);
    }
    return fsVolume;
  }

  /**
   * 查询当前提交计划的执行状态，返回给客户端。
   * Returns the current work status of a previously submitted Plan.
   *
   * @return DiskBalancerWorkStatus.
   * @throws DiskBalancerException
   */
  public DiskBalancerWorkStatus queryWorkStatus() throws DiskBalancerException {
    lock.lock();
    try {
      checkDiskBalancerEnabled();
      // 如果计划正在执行，检查是否已经执行完成
      if (this.currentResult == Result.PLAN_UNDER_PROGRESS &&
          this.future != null &&
          this.future.isDone()) {
        this.currentResult = Result.PLAN_DONE;
      }

      DiskBalancerWorkStatus status =
          new DiskBalancerWorkStatus(this.currentResult, this.planID,
                  this.planFile);
      for (Map.Entry<VolumePair, DiskBalancerWorkItem> entry :
          workMap.entrySet()) {
        DiskBalancerWorkEntry workEntry = new DiskBalancerWorkEntry(
            entry.getKey().getSourceVolBasePath(),
            entry.getKey().getDestVolBasePath(),
            entry.getValue());
        status.addWorkEntry(workEntry);
      }
      return status;
    } finally {
      lock.unlock();
    }
  }

  /**
   * 取消正在执行的指定计划，停止块移动，释放资源。
   * Cancels a running plan.
   *
   * @param planID - Hash of the plan to cancel.
   * @throws DiskBalancerException
   */
  public void cancelPlan(String planID) throws DiskBalancerException {
    lock.lock();
    boolean needShutdown = false;
    try {
      checkDiskBalancerEnabled();
      if (this.planID == null ||
          !this.planID.equals(planID) ||
          this.planID.isEmpty()) {
        LOG.error("Disk Balancer - No such plan. Cancel plan failed. PlanID: " +
            planID);
        throw new DiskBalancerException("No such plan.",
            DiskBalancerException.Result.NO_SUCH_PLAN);
      }
      if (!this.future.isDone()) {
        this.currentResult = Result.PLAN_CANCELLED;
        this.blockMover.setExitFlag();
        scheduler.shutdown();
        needShutdown = true;
      }
    } finally {
      lock.unlock();
    }
    // no need to hold lock while shutting down executor.
    if (needShutdown) {
      shutdownExecutor();
    }
  }

  /**
   * 获取当前DataNode所有卷的ID到基路径映射，返回JSON格式字符串给客户端。
   * Returns a volume ID to Volume base path map.
   *
   * @return Json string of the volume map.
   * @throws DiskBalancerException
   */
  public String getVolumeNames() throws DiskBalancerException {
    lock.lock();
    try {
      checkDiskBalancerEnabled();
      return JsonUtil.toJsonString(getStorageIDToVolumeBasePathMap());
    } catch (DiskBalancerException ex) {
      throw ex;
    } catch (IOException e) {
      throw new DiskBalancerException("Internal error, Unable to " +
          "create JSON string.", e,
          DiskBalancerException.Result.INTERNAL_ERROR);
    } finally {
      lock.unlock();
    }
  }

  /**
   * 获取当前磁盘均衡器配置的最大带宽值。
   * Returns the current bandwidth.
   *
   * @return string representation of bandwidth.
   * @throws DiskBalancerException
   */
  public long getBandwidth() throws DiskBalancerException {
    lock.lock();
    try {
      checkDiskBalancerEnabled();
      return this.bandwidth;
    } finally {
      lock.unlock();
    }
  }

  /**
   * 检查磁盘均衡器是否已启用，未启用则抛出异常。
   * Throws if Disk balancer is disabled.
   *
   * @throws DiskBalancerException
   */
  private void checkDiskBalancerEnabled()
      throws DiskBalancerException {
    if (!isDiskBalancerEnabled) {
      throw new DiskBalancerException("Disk Balancer is not enabled.",
          DiskBalancerException.Result.DISK_BALANCER_NOT_ENABLED);
    }
  }

  /**
   * 设置磁盘均衡器启用/禁用状态。
   * Sets Disk balancer is to enable or not to enable.
   *
   * @param diskBalancerEnabled
   *          true, enable diskBalancer, otherwise false to disable it.
   */
  public void setDiskBalancerEnabled(boolean diskBalancerEnabled) {
    isDiskBalancerEnabled = diskBalancerEnabled;
  }

  /**
   * 获取磁盘均衡器当前是否启用。
   * Returns the value indicating if diskBalancer is enabled.
   *
   * @return boolean.
   */
  @VisibleForTesting
  public boolean isDiskBalancerEnabled() {
    return isDiskBalancerEnabled;
  }

  /**
   * 设置计划有效期，超过有效期的计划会被拒绝执行。
   * Sets maximum amount of time disk balancer plan is valid.
   *
   * @param planValidityInterval - maximum amount of time in the unit of milliseconds.
   */
  public void setPlanValidityInterval(long planValidityInterval) {
    this.config.setTimeDuration(DFSConfigKeys.DFS_DISK_BALANCER_PLAN_VALID_INTERVAL,
        planValidityInterval, TimeUnit.MILLISECONDS);
    this.planValidityInterval = planValidityInterval;
  }

  /**
   * 获取当前配置的计划有效期（毫秒）。
   * Gets maximum amount of time disk balancer plan is valid.
   *
   * @return the maximum amount of time in milliseconds.
   */
  @VisibleForTesting
  public long getPlanValidityInterval() {
    return planValidityInterval;
  }

  /**
   * 从配置文件中读取计划有效期默认值。
   * Gets maximum amount of time disk balancer plan is valid in config.
   *
   * @return the maximum amount of time in milliseconds.
   */
  @VisibleForTesting
  public long getPlanValidityIntervalInConfig() {
    return config.getTimeDuration(DFSConfigKeys.DFS_DISK_BALANCER_PLAN_VALID_INTERVAL,
        DFSConfigKeys.DFS_DISK_BALANCER_PLAN_VALID_INTERVAL_DEFAULT, TimeUnit.MILLISECONDS);
  }

  /**
   * 完整校验客户端提交的计划，包括版本、哈希、时间戳、节点UUID等。
   * Verifies that user provided plan is valid.
   *
   * @param planID      - SHA-1 of the plan.
   * @param planVersion - Version of the plan, for future use.
   * @param plan        - Plan String in Json.
   * @param force       - Skip verifying when the plan was generated.
   * @return a NodePlan Object.
   * @throws DiskBalancerException
   */
  private NodePlan verifyPlan(String planID, long planVersion, String plan,
                              boolean force) throws DiskBalancerException {

    Preconditions.checkState(lock.isHeldByCurrentThread());
    verifyPlanVersion(planVersion);
    NodePlan nodePlan = verifyPlanHash(planID, plan);
    if (!force) {
      verifyTimeStamp(nodePlan);
    }
    verifyNodeUUID(nodePlan);
    return nodePlan;
  }

  /**
   * 校验计划版本是否在当前支持范围内。
   * Verifies the plan version is something that we support.
   *
   * @param planVersion - Long version.
   * @throws DiskBalancerException
   */
  private void verifyPlanVersion(long planVersion)
      throws DiskBalancerException {
    if