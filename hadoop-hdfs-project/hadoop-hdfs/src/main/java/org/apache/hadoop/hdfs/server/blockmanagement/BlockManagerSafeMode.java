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
package org.apache.hadoop.hdfs.server.blockmanagement;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.BlockListAsLongs.BlockReportReplica;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.RollingUpgradeStartupOption;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.StartupOption;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.Namesystem;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.Phase;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.StartupProgress;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.StartupProgress.Counter;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.Status;
等待
org.apache.hadoop.hdfs.server.namenode.startupprogress.Step;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.StepType;
import org.apache.hadoop.hdfs.util.RwLockMode;
import org.apache.hadoop.net.NetworkTopology;
org.apache.hadoop.util.Daemon;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_REPL_QUEUE_THRESHOLD_PCT_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_SAFEMODE_EXTENSION_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_SAFEMODE_EXTENSION_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_SAFEMODE_MIN_DATANODES_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_SAFEMODE_MIN_DATANODES_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_SAFEMODE_THRESHOLD_PCT_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_SAFEMODE_THRESHOLD_PCT_KEY;
import static org.apache.hadoop.util.Time.monotonicNow;

/**
 * HDFS块管理安全模式（SafeMode）状态管理类。
 * <p>
 * NameNode启动过程中，统计满足最小副本数要求的"安全块"数量，
 * 计算安全块占总块数的比例。当安全块比例达到阈值{@link #threshold}、
 * 且有足够多的存活DataNode注册后，还需要等待额外的{@link #extension}延长时间，
 * 才能退出安全模式，允许集群对外提供读写服务。
 * </p>
 * <p>
 * 核心职责：负责NameNode启动阶段安全模式的状态转换、阈值检查和退出逻辑管理。
 * </p>
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
class BlockManagerSafeMode {
  /**
   * 安全模式状态枚举，描述安全模式当前所处状态。
   */
  enum BMSafeModeStatus {
    PENDING_THRESHOLD, /** 等待更多安全块上报、等待更多DataNode上线，尚未满足退出阈值 */
    EXTENSION,         /** 已满足退出阈值，正在等待额外延长时间 */
    OFF                /** 安全模式已退出，集群正常运行 */
  }

  static final Logger LOG = LoggerFactory.getLogger(BlockManagerSafeMode.class);
  /** 启动进度跟踪步骤：等待块上报完成 */
  static final Step STEP_AWAITING_REPORTED_BLOCKS =
      new Step(StepType.AWAITING_REPORTED_BLOCKS);

  /** 所属块管理器实例 */
  private final BlockManager blockManager;
  /** 所属NameNode命名系统实例 */
  private final Namesystem namesystem;
  /** 是否启用高可用（HA）模式 */
  private final boolean haEnabled;
  /** 当前安全模式状态，支持多线程并发访问 */
  private volatile BMSafeModeStatus status = BMSafeModeStatus.OFF;

  /** 安全块占总块数的退出阈值百分比 */
  private final float threshold;
  /** 满足退出阈值所需的最小安全块数量 */
  private long blockThreshold;
  /** 集群总块数 */
  private long blockTotal;
  /** 当前已上报满足条件的安全块数量 */
  private long blockSafe;
  /** 退出安全模式所需的最小存活DataNode数量 */
  private final int datanodeThreshold;
  /** 判定一个块为安全块所需的最小副本数 */
  private final int safeReplication;
  /** 初始化复制队列所需的安全块占比阈值 */
  private final float replQueueThreshold;
  /** 初始化复制队列所需的最小安全块数量 */
  private long blockReplQueueThreshold;

  /** 满足阈值后额外等待的延长时间，单位毫秒 */
  @VisibleForTesting
  final long extension;
  /** 第一次满足退出阈值的时间戳 */
  private final AtomicLong reachedTime = new AtomicLong();
  /** 安全模式初始化时间戳 */
  private long startTime;
  /** 安全模式监控后台线程，用于定时检查是否满足退出条件 */
  private final Daemon smmthread;

  /** 上一次输出状态日志的时间戳 */
  private long lastStatusReport;
  /** 启动进度计数器：记录已上报的块数量 */
  private Counter awaitingReportedBlocksCounter;

  /** 累加记录未来代块（GS大于当前NameNode已知GS）的总字节数 */
  private final LongAdder bytesInFutureBlocks = new LongAdder();
  /** 累加记录未来代纠删码块组的总字节数 */
  private final LongAdder bytesInFutureECBlockGroups = new LongAdder();

  /** NameNode是否以回滚模式启动 */
  private final boolean inRollBack;

  /**
   * 构造安全模式管理器，从配置中加载各类阈值参数。
   * @param blockManager 所属块管理器
   * @param namesystem 所属命名系统
   * @param haEnabled 是否启用HA模式
   * @param conf Hadoop配置对象
   */
  BlockManagerSafeMode(BlockManager blockManager, Namesystem namesystem,
      boolean haEnabled, Configuration conf) {
    this.blockManager = blockManager;
    this.namesystem = namesystem;
    this.haEnabled = haEnabled;
    this.threshold = conf.getFloat(DFS_NAMENODE_SAFEMODE_THRESHOLD_PCT_KEY,
        DFS_NAMENODE_SAFEMODE_THRESHOLD_PCT_DEFAULT);
    if (this.threshold > 1.0) {
      LOG.warn("The threshold value shouldn't be greater than 1, " +
          "threshold: {}", threshold);
    }
    this.datanodeThreshold = conf.getInt(
        DFS_NAMENODE_SAFEMODE_MIN_DATANODES_KEY,
        DFS_NAMENODE_SAFEMODE_MIN_DATANODES_DEFAULT);
    int minReplication =
        conf.getInt(DFSConfigKeys.DFS_NAMENODE_REPLICATION_MIN_KEY,
            DFSConfigKeys.DFS_NAMENODE_REPLICATION_MIN_DEFAULT);
    // DFS_NAMENODE_SAFEMODE_REPLICATION_MIN_KEY is an expert level setting,
    // setting this lower than the min replication is not recommended
    // and/or dangerous for production setups.
    // When it's unset, safeReplication will use dfs.namenode.replication.min
    this.safeReplication =
        conf.getInt(DFSConfigKeys.DFS_NAMENODE_SAFEMODE_REPLICATION_MIN_KEY,
            minReplication);
    // default to safe mode threshold (i.e., don't populate queues before
    // leaving safe mode)
    this.replQueueThreshold =
        conf.getFloat(DFS_NAMENODE_REPL_QUEUE_THRESHOLD_PCT_KEY, threshold);
    this.extension = conf.getTimeDuration(DFS_NAMENODE_SAFEMODE_EXTENSION_KEY,
        DFS_NAMENODE_SAFEMODE_EXTENSION_DEFAULT,
        MILLISECONDS);

    this.inRollBack = isInRollBackMode(NameNode.getStartupOption(conf));
    this.smmthread = new Daemon(new SafeModeMonitor(conf));

    LOG.info("{} = {}", DFS_NAMENODE_SAFEMODE_THRESHOLD_PCT_KEY, threshold);
    LOG.info("{} = {}", DFS_NAMENODE_SAFEMODE_MIN_DATANODES_KEY,
        datanodeThreshold);
    LOG.info("{} = {}", DFS_NAMENODE_SAFEMODE_EXTENSION_KEY, extension);
  }

  /**
   * 激活安全模式，初始化总块数并检查是否可以直接退出安全模式。
   * @param total 初始总块数
   */
  void activate(long total) {
    assert namesystem.hasWriteLock(RwLockMode.BM);
    assert status == BMSafeModeStatus.OFF;

    startTime = monotonicNow();
    setBlockTotal(total);
    if (areThresholdsMet()) {
      boolean exitResult = leaveSafeMode(false);
      Preconditions.checkState(exitResult, "Failed to leave safe mode.");
    } else {
      // enter safe mode
      status = BMSafeModeStatus.PENDING_THRESHOLD;
      initializeReplQueuesIfNecessary();
      reportStatus("STATE* Safe mode ON.", true);
      lastStatusReport = monotonicNow();
    }
  }

  /**
   * 检查当前是否处于启动安全模式。
   * @return true表示当前处于安全模式，false表示已退出
   */
  boolean isInSafeMode() {
    if (status != BMSafeModeStatus.OFF) {
      doConsistencyCheck();
      return true;
    } else {
      return false;
    }
  }

  /**
   * 检查安全模式状态，执行状态机转换。安全模式已退出则无操作。
   */
  void checkSafeMode() {
    assert namesystem.hasWriteLock(RwLockMode.BM);
    if (namesystem.inTransitionToActive()) {
      return;
    }

    switch (status) {
    case PENDING_THRESHOLD:
      if (areThresholdsMet()) {
        if (blockTotal > 0 && extension > 0) {
          // PENDING_THRESHOLD -> EXTENSION 状态转换
          status = BMSafeModeStatus.EXTENSION;
          reachedTime.set(monotonicNow());
          smmthread.start();
          initializeReplQueuesIfNecessary();
          reportStatus("STATE* Safe mode extension entered.", true);
        } else {
          // TODO: let the smmthread to leave the safemode.
          // PENDING_THRESHOLD -> OFF 状态转换
          leaveSafeMode(false);
        }
      } else {
        initializeReplQueuesIfNecessary();
        reportStatus("STATE* Safe mode ON.", false);
      }
      break;
    case EXTENSION:
      reportStatus("STATE* Safe mode ON.", false);
      break;
    case OFF:
      break;
    default:
      assert false : "Non-recognized block manager safe mode status: " + status;
    }
  }

  /**
   * 在安全模式下调整安全块总数和总块数，增量更新统计值。安全模式已退出则无操作。
   * @param deltaSafe 安全块数量变化量
   * @param deltaTotal 总块数量变化量
   */
  void adjustBlockTotals(int deltaSafe, int deltaTotal) {
    assert namesystem.hasWriteLock(RwLockMode.BM);
    if (!isSafeModeTrackingBlocks()) {
      return;
    }

    long newBlockTotal;
    synchronized (this) {
      LOG.debug("Adjusting block totals from {}/{} to {}/{}",  blockSafe,
          blockTotal, blockSafe + deltaSafe, blockTotal + deltaTotal);
      assert blockSafe + deltaSafe >= 0 : "Can't reduce blockSafe " +
          blockSafe + " by " + deltaSafe + ": would be negative";
      assert blockTotal + deltaTotal >= 0 : "Can't reduce blockTotal " +
          blockTotal + " by " + deltaTotal + ": would be negative";

      blockSafe += deltaSafe;
      newBlockTotal = blockTotal + deltaTotal;
    }
    setBlockTotal(newBlockTotal);
    checkSafeMode();
  }

  /**
   * 检查是否需要增量跟踪安全块统计。
   * <p>
   * 非HA模式下从不增量跟踪；HA模式下，Standby节点在加载完镜像后，需要增量跟踪
   * 因为编辑日志会不断增删块，总块数会动态变化。
   * </p>
   * @return true表示需要增量跟踪，false表示不需要
   */
  boolean isSafeModeTrackingBlocks() {
    assert namesystem.hasWriteLock(RwLockMode.BM);
    return haEnabled && status != BMSafeModeStatus.OFF;
  }

  /**
   * 设置总块数，同时重新计算块阈值和复制队列阈值。
   * @param total 总块数
   */
  void setBlockTotal(long total) {
    assert namesystem.hasWriteLock(RwLockMode.BM);
    synchronized (this) {
      this.blockTotal = total;
      this.blockThreshold = (long) (total * threshold);
    }
    this.blockReplQueueThreshold = (long) (total * replQueueThreshold);
  }

  /**
   * 生成安全模式当前状态提示信息，用于Web UI和日志输出，告知用户当前退出进度。
   * @return 格式化后的状态提示字符串
   */
  String getSafeModeTip() {
    StringBuilder msg = new StringBuilder();
    boolean isBlockThresholdMet = false;

    synchronized (this) {
      isBlockThresholdMet = (blockSafe >= blockThreshold);
      if (!isBlockThresholdMet) {
        msg.append(String.format(
            "The reported blocks %d needs additional %d"
                + " blocks to reach the threshold %.4f of total blocks %d.%n",
            blockSafe, (blockThreshold - blockSafe), threshold, blockTotal));
      } else {
        msg.append(String.format(
            "The reported blocks %d has reached the threshold %.4f of total"
                + " blocks %d. ", blockSafe, threshold, blockTotal));
      }
    }

    if (datanodeThreshold > 0) {
      if (isBlockThresholdMet) {
        int numLive = blockManager.getDatanodeManager().getNumLiveDataNodes();
        if (numLive < datanodeThreshold) {
          msg.append(String.format(
              "The number of live datanodes %d needs an additional %d live "
                  + "datanodes to reach the minimum number %d.%n",
              numLive, (datanodeThreshold - numLive), datanodeThreshold));
        } else {
          msg.append(String.format(
              "The number of live datanodes %d has reached the minimum number"
                  + " %d. ", numLive, datanodeThreshold));
        }
      } else {
        msg.append("The number of live datanodes is not calculated ")
            .append("since reported blocks hasn't reached the threshold. ");
      }
    } else {
      msg.append("The minimum number of live datanodes is not required. ");
    }

    if (getBytesInFuture() > 0) {
      msg.append("Name node detected blocks with generation stamps in future. ")
          .append("This means that Name node metadata is inconsistent. This ")
          .append("can happen if Name node metadata files have been manually ")
          .append("replaced. Exiting safe mode will cause loss of ")
          .append(getBytesInFuture())
          .append(" byte(s). Please restart name node with right metadata ")
          .append("or use \"hdfs dfsadmin -safemode forceExit\" if you ")
          .append("are certain that the NameNode was started with the correct ")
          .append("FsImage and edit logs. If you encountered this during ")
          .append("a rollback, it is safe to exit with -safemode forceExit.");
      return msg.toString();
    }

    final String turnOffTip = "Safe mode will be turned off automatically ";
    switch(status) {
    case PENDING_THRESHOLD:
      msg.append(turnOffTip).append("once the thresholds have been reached.");
      break;
    case EXTENSION:
      msg.append("In safe mode extension. ").append(turnOffTip).append("in ")
          .append(timeToLeaveExtension() / 1000).append(" seconds