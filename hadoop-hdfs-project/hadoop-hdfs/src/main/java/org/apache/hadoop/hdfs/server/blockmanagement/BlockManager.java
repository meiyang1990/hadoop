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

import static org.apache.hadoop.hdfs.DFSConfigKeys.*;
import static org.apache.hadoop.hdfs.protocol.BlockType.CONTIGUOUS;
import static org.apache.hadoop.hdfs.protocol.BlockType.STRIPED;
import static org.apache.hadoop.util.ExitUtil.terminate;
import static org.apache.hadoop.util.Time.now;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ConcurrentLinkedQueue;

import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import javax.management.ObjectName;

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.AddBlockFlag;
import org.apache.hadoop.fs.FileEncryptionInfo;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.HAUtil;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockListAsLongs;
import org.apache.hadoop.hdfs.protocol.BlockListAsLongs.BlockReportReplica;
import org.apache.hadoop.hdfs.protocol.BlockType;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.StoragePolicySatisfierMode;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.LocatedStripedBlock;
import org.apache.hadoop.hdfs.protocol.UnregisteredNodeException;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenSecretManager;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier.AccessMode;
import org.apache.hadoop.hdfs.security.token.block.DataEncryptionKey;
import org.apache.hadoop.hdfs.security.token.block.ExportedBlockKeys;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoStriped.StorageAndBlockIndex;
import org.apache.hadoop.hdfs.server.blockmanagement.CorruptReplicasMap.Reason;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStorageInfo.AddBlockResult;
import org.apache.hadoop.hdfs.server.blockmanagement.NumberReplicas.StoredReplicaState;
import org.apache.hadoop.hdfs.server.blockmanagement.PendingDataNodeMessages.ReportedBlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.PendingReconstructionBlocks.PendingBlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.ExcessRedundancyMap.ExcessBlockInfo;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.BlockUCState;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.ReplicaState;
import org.apache.hadoop.hdfs.server.namenode.CachedBlock;
import org.apache.hadoop.hdfs.server.namenode.INode.BlocksMapUpdateInfo;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;
import org.apache.hadoop.hdfs.server.namenode.INodesInPath;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.Namesystem;
import org.apache.hadoop.hdfs.server.namenode.ha.HAContext;
import org.apache.hadoop.hdfs.server.namenode.metrics.NameNodeMetrics;
import org.apache.hadoop.hdfs.server.namenode.sps.StoragePolicySatisfyManager;
import org.apache.hadoop.hdfs.server.protocol.BlockCommand;
import org.apache.hadoop.hdfs.server.protocol.BlockReportContext;
import org.apache.hadoop.hdfs.server.protocol.BlocksWithLocations;
import org.apache.hadoop.hdfs.server.protocol.BlocksWithLocations.BlockWithLocations;
import org.apache.hadoop.hdfs.server.protocol.BlocksWithLocations.StripedBlockWithLocations;
import org.apache.hadoop.hdfs.server.protocol.DatanodeCommand;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage.State;
import org.apache.hadoop.hdfs.server.protocol.KeyUpdateCommand;
import org.apache.hadoop.hdfs.server.protocol.ReceivedDeletedBlockInfo;
import org.apache.hadoop.hdfs.server.protocol.StorageReceivedDeletedBlocks;
import org.apache.hadoop.hdfs.server.protocol.StorageReport;
import org.apache.hadoop.hdfs.server.protocol.VolumeFailureSummary;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.server.namenode.CacheManager;

import static org.apache.hadoop.hdfs.util.StripedBlockUtil.getInternalBlockLength;

import org.apache.hadoop.hdfs.util.LightWeightHashSet;
import org.apache.hadoop.hdfs.util.RwLockMode;
import org.apache.hadoop.metrics2.util.MBeans;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.util.LightWeightGSet;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.util.concurrent.SubjectInheritingThread;
import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * HDFS NameNode核心块管理类，负责维护集群中所有数据块的元信息，管理副本冗余度、处理节点上下线、
 * 调度块复制和块删除任务，维护块副本的状态一致性。
 * <p>
 * 核心安全属性维护：在正常情况下保证{@code 存活副本数 == 期望冗余度}，在节点进入维护模式时扩展安全规则：
 * <ol>
 * <li>存活副本数 &gt;= 维护模式最小副本数</li>
 * <li>存活副本数 &lt;= 期望冗余度</li>
 * <li>存活副本数 + 维护中副本数 &gt;= 期望冗余度</li>
 * </ol>
 * 该规则也兼容无维护副本的场景，可适用于所有运行场景。
 * <p>
 * 同时负责块放置策略校验，放置策略应用于所有存活+维护中副本，保证块分布符合机架感知要求。
 */
@InterfaceAudience.Private
public class BlockManager implements BlockStatsMXBean {

  public static final Logger LOG = LoggerFactory.getLogger(BlockManager.class);
  public static final Logger blockLog = NameNode.blockStateChangeLog;

  private static final String QUEUE_REASON_CORRUPT_STATE =
    "it has the wrong state or generation stamp";

  private static final String QUEUE_REASON_FUTURE_GENSTAMP =
    "generation stamp is in the future";

  private static final long BLOCK_RECOVERY_TIMEOUT_MULTIPLIER = 30;

  /** 关联的NameSystem命名空间实例 */
  private final Namesystem namesystem;

  /** 安全模式管理实例 */
  private final BlockManagerSafeMode bmSafeMode;

  /** DataNode管理器实例 */
  private final DatanodeManager datanodeManager;
  /** 心跳管理器实例 */
  private final HeartbeatManager heartbeatManager;
  /** 块令牌密钥管理器 */
  private final BlockTokenSecretManager blockTokenSecretManager;

  // 当前NameNode使用的块池ID
  private String blockPoolId;

  /** 备用节点待处理延迟块报告消息队列 */
  private final PendingDataNodeMessages pendingDNMessages =
    new PendingDataNodeMessages();

  private volatile long pendingReconstructionBlocksCount = 0L;
  private volatile long corruptReplicaBlocksCount = 0L;
  private volatile long lowRedundancyBlocksCount = 0L;
  private volatile long scheduledReplicationBlocksCount = 0L;

  private final long deleteBlockLockTimeMs;
  private final long deleteBlockUnlockIntervalTimeMs;

  /** 标识复制队列是否已完成初始化 */
  private boolean initializedReplQueues;

  private final long startupDelayBlockDeletionInMs;
  /** 块报告租约管理器 */
  private final BlockReportLeaseManager blockReportLeaseManager;
  private ObjectName mxBeanName;

  /** Used by metrics */
  public long getPendingReconstructionBlocksCount() {
    return pendingReconstructionBlocksCount;
  }
  /** Used by metrics */
  public long getLowRedundancyBlocksCount() {
    return lowRedundancyBlocksCount;
  }
  /** Used by metrics */
  public long getCorruptReplicaBlocksCount() {
    return corruptReplicaBlocksCount;
  }
  /** Used by metrics */
  public long getScheduledReplicationBlocksCount() {
    return scheduledReplicationBlocksCount;
  }
  /** Used by metrics */
  public long getPendingDeletionBlocksCount() {
    return invalidateBlocks.numBlocks();
  }
  /** Used by metrics */
  public long getStartupDelayBlockDeletionInMs() {
    return startupDelayBlockDeletionInMs;
  }
  /** Used by metrics */
  public long getExcessBlocksCount() {
    return excessRedundancyMap.size();
  }
  /** Used by metrics */
  public long getPostponedMisreplicatedBlocksCount() {
    return postponedMisreplicatedBlocks.size();
  }
  /** Used by metrics */
  public int getPendingDataNodeMessageCount() {
    return pendingDNMessages.count();
  }
  /** Used by metrics. */
  public long getNumTimedOutPendingReconstructions() {
    return pendingReconstruction.getNumTimedOuts();
  }

  /** Used by metrics. */
  public long getLowRedundancyBlocks() {
    return neededReconstruction.getLowRedundancyBlocks();
  }

  /** Used by metrics. */
  public long getCorruptBlocks() {
    return corruptReplicas.getCorruptBlocks();
  }

  /** Used by metrics. */
  public long getMissingBlocks() {
    return neededReconstruction.getCorruptBlocks();
  }

  /** Used by metrics. */
  public long getMissingReplicationOneBlocks() {
    return neededReconstruction.getCorruptReplicationOneBlocks();
  }

  /** Used by metrics. */
  public long getBadlyDistributedBlocks() {
    return neededReconstruction.getBadlyDistributedBlocks();
  }

  /** Used by metrics. */
  public long getPendingDeletionReplicatedBlocks() {
    return invalidateBlocks.getBlocks();
  }

  /** Used by metrics. */
  public long getTotalReplicatedBlocks() {
    return blocksMap.getReplicatedBlocks();
  }

  /** Used by metrics. */
  public long getLowRedundancyECBlockGroups() {
    return neededReconstruction.getLowRedundancyECBlockGroups();
  }

  /** Used by metrics. */
  public long getCorruptECBlockGroups() {
    return corruptReplicas.getCorruptECBlockGroups();
  }

  /** Used by metrics. */
  public long getMissingECBlockGroups() {
    return neededReconstruction.getCorruptECBlockGroups();
  }

  /** Used by metrics. */
  public long getPendingDeletionECBlocks() {
    return invalidateBlocks.getECBlocks();
  }

  /** Used by metrics. */
  public long getTotalECBlockGroups() {
    return blocksMap.getECBlockGroups();
  }

  /** Used by metrics. */
  public int getPendingSPSPaths() {
    if (spsManager != null) {
      return spsManager.getPendingSPSPaths();
    }
    return 0;
  }

  /** 冗余检查间隔，单位毫秒，NameNode定期扫描低冗余块 */
  private final long redundancyRecheckIntervalMs;

  /**
   * 复制队列迭代器重置阈值：计数自从上次重置队列为头开始的调用次数，
   * 超过阈值则重置迭代器从头开始扫描；阈值为0表示只有到队列末尾才重置
   */
  private int replQueueResetToHeadThreshold;
  private int replQueueCallsSinceReset = 0;

  /**
   * 块到块信息的映射表，存储所有块的元数据和位置信息
   * 仅响应客户端和DataNode上报的信息更新
   */
  final BlocksMap blocksMap;

  /** 冗余监控线程，定期扫描处理低冗余块和过期冗余 */
  private final Daemon redundancyThread = new Daemon(new RedundancyMonitor());
  /**
   * 冗余监控线程完整一轮扫描结束时间戳，供单元测试使用，验证线程已完成至少一次完整扫描
   */
  private final AtomicLong lastRedundancyCycleTS = new AtomicLong(-1);
  /** 标记删除块异步清理线程，处理已删除文件的块清理 */
  private final Daemon markedDeleteBlockScrubberThread =
      new Daemon(new MarkedDeleteBlockScrubber());

  /** 块报告异步处理线程，处理全量和增量块报告 */
  private final BlockReportProcessingThread blockReportThread;

  /**
   * 存储损坏副本映射：块 -> 持有该块损坏副本的DataNode列表
   */
  final CorruptReplicasMap corruptReplicas = new CorruptReplicasMap();

  /**
   * 待删除块队列，EC块需要单独跟踪每个内部块
   */
  private final InvalidateBlocks invalidateBlocks;
  
  /**
   * 主备切换后，过度复制块需要等到所有副本都完成块报告上报给新激活NameNode后再处理，
   * 确保NameNode已经收到故障转移前所有待处理的块删除消息
   */
  private final Set<Block> postponedMisreplicatedBlocks =
      new LinkedHashSet<Block>();
  private final int blocksPerPostpondedRescan;
  private final ArrayList<Block> rescannedMisreplicatedBlocks;

  /**
   * 过度冗余块映射：按StorageID分组存储每个DataNode上的多余副本，最终会被删除
   */
  private final ExcessRedundancyMap excessRedundancyMap =
      new ExcessRedundancyMap();

  /**
   * 存储需要进行1次或多次复制重构的低冗余块，同时保存待处理重构任务
   */
  public final LowRedundancyBlocks neededReconstruction =
      new LowRedundancyBlocks();

  @VisibleForTesting
  final PendingReconstructionBlocks pendingReconstruction;

  /** 存储块恢复尝试信息 */
  private final PendingRecoveryBlocks pendingRecoveryBlocks;

  /** 块允许的最大副本数 */
  public final short maxReplication;
  /**
   * 单个节点同时允许的最大出站复制流数量（不包含最高优先级复制任务）
   */
  private volatile int maxReplicationStreams;
  /**
   * 单个节点同时允许的最大出站复制流硬限制
   */
  private volatile int replicationStreamsHardLimit;
  /** 最小副本数，低于此不允许文件写入完成 */
  public final short minReplication;
  /** 默认副本数 */
  public final int defaultReplication;
  /** 获取损坏文件列表时返回的最大损坏文件数配置 */
  final int maxCorruptFilesReturned;

  final float blocksInvalidateWorkPct;
  private volatile int blocksReplWorkMultiplier;

  // 是否启用数据传输加密
  final boolean encryptData;
  
  // 是否开启数据传输加密;
  
  // 块报告日志最多输出块数量上限
  private final long maxNumBlocksToLog;

  // 块报告处理线程最大锁持有时间，单位毫秒
  private final long maxLockHoldTime;

  /**
   * 备用节点运行时，可能在收到活跃节点命名空间编辑日志之前就收到DataNode块报告，
   * 因此需要推迟处理这些未来块，不直接标记为损坏
   */
  private boolean shouldPostponeBlocksFromFuture = false;

  /**
   * 异步初始化复制队列，加快NameNode退出安全模式和故障转移速度，对应HDFS-5496
   */
  private Daemon reconstructionQueuesInitializer = null;
  /**
   * 复制队列异步初始化每次迭代处理块数，获取锁处理一部分后释放锁，再重新获取锁处理剩余块