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
package org.apache.hadoop.hdfs.server.namenode.sps;

import static org.apache.hadoop.util.Time.monotonicNow;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.DatanodeInfoWithStorage;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.StoragePolicySatisfierMode;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.HdfsLocatedFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.LocatedStripedBlock;
import org.apache.hadoop.hdfs.server.balancer.Matcher;
import org.apache.hadoop.hdfs.server.namenode.ErasureCodingPolicyManager;
import org.apache.hadoop.hdfs.server.protocol.BlockStorageMovementCommand.BlockMovingInfo;
import org.apache.hadoop.hdfs.util.StripedBlockUtil;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.util.Preconditions;

/**
 * 文件写入后设置存储策略只会更新命名空间中的存储策略类型，块数据的实际移动不会自动发生，需要用户手动运行Mover工具。
 * 存储策略满足器（SPS）实现了自动块移动能力，可在NameNode内部或独立运行，用户只需调用满足存储策略客户端API，
 * SPS就会自动异步处理块移动，将块物理移动到符合存储策略要求的存储介质。
 * 核心职责：收集需要移动的块，构建源块位置与目标存储类型/位置的映射，生成块移动请求发送给DataNode执行实际移动。
 * 属于HDFS NameNode模块，负责自动满足文件存储策略，实现块数据的自动分层存储。
 */
@InterfaceAudience.Private
public class StoragePolicySatisfier implements SPSService, Runnable {
  public static final Logger LOG =
      LoggerFactory.getLogger(StoragePolicySatisfier.class);
  private Daemon storagePolicySatisfierThread;
  private BlockStorageMovementNeeded storageMovementNeeded;
  private BlockStorageMovementAttemptedItems storageMovementsMonitor;
  private volatile boolean isRunning = false;
  private int spsWorkMultiplier;
  private long blockCount = 0L;
  private int blockMovementMaxRetry;
  private Context ctxt;
  private final Configuration conf;
  private DatanodeCacheManager dnCacheMgr;

  /**
   * 构造函数，仅初始化配置对象。
   * @param conf Hadoop配置对象
   */
  public StoragePolicySatisfier(Configuration conf) {
    this.conf = conf;
  }

  /**
   * 存储块移动分析结果状态容器，保存所有块分析结果和已分配目标节点信息。
   */
  private static class BlocksMovingAnalysis {

    /**
     * 块移动分析结果状态枚举。
     */
    enum Status {
      // 因条件不满足跳过分析，需要后续重试，例如块收集未完成
      ANALYSIS_SKIPPED_FOR_RETRY,
      // 部分或全部块找到了匹配的目标存储，可执行移动
      BLOCKS_TARGETS_PAIRED,
      // 没有任何块找到符合要求的目标存储
      NO_BLOCKS_TARGETS_PAIRED,
      // 所有块已经满足存储策略要求，无需移动
      BLOCKS_ALREADY_SATISFIED,
      // 因条件不满足跳过配对分析，例如文件没有块、存储策略不适用于EC文件
      BLOCKS_TARGET_PAIRING_SKIPPED,
      // 所有已报告块满足策略，但存在部分块冗余度不足
      FEW_LOW_REDUNDANCY_BLOCKS,
      // 出现意外错误，块移动失败
      BLOCKS_FAILED_TO_MOVE
    }

    private Status status = null;
    private Map<Block, Set<StorageTypeNodePair>> assignedBlocks = null;

    BlocksMovingAnalysis(Status status,
        Map<Block, Set<StorageTypeNodePair>> assignedBlocks) {
      this.status = status;
      this.assignedBlocks = assignedBlocks;
    }
  }

  /**
   * 初始化SPS，初始化各个核心组件和配置参数。
   * @param context SPS运行上下文，提供与NameNode交互的接口
   */
  public void init(final Context context) {
    this.ctxt = context;
    this.storageMovementNeeded = new BlockStorageMovementNeeded(context);
    this.storageMovementsMonitor = new BlockStorageMovementAttemptedItems(
        this, storageMovementNeeded, context);
    this.spsWorkMultiplier = getSPSWorkMultiplier(getConf());
    this.blockMovementMaxRetry = getConf().getInt(
        DFSConfigKeys.DFS_STORAGE_POLICY_SATISFIER_MAX_RETRY_ATTEMPTS_KEY,
        DFSConfigKeys.DFS_STORAGE_POLICY_SATISFIER_MAX_RETRY_ATTEMPTS_DEFAULT);
  }

  /**
   * 启动存储策略满足器服务，启动工作线程和块移动监控线程，激活等待队列。
   * @param serviceMode SPS运行模式（禁用以、内嵌NameNode、独立运行）
   */
  @Override
  public synchronized void start(StoragePolicySatisfierMode serviceMode) {
    if (serviceMode == StoragePolicySatisfierMode.NONE) {
      LOG.error("Can't start StoragePolicySatisfier for the given mode:{}",
          serviceMode);
      return;
    }
    LOG.info("Starting {} StoragePolicySatisfier.",
        StringUtils.toLowerCase(serviceMode.toString()));
    isRunning = true;
    storagePolicySatisfierThread = new Daemon(this);
    storagePolicySatisfierThread.setName("StoragePolicySatisfier");
    storagePolicySatisfierThread.start();
    this.storageMovementsMonitor.start();
    this.storageMovementNeeded.activate();
    dnCacheMgr = new DatanodeCacheManager(conf);
  }

  /**
   * 停止存储策略满足器服务，中断工作线程，停止监控线程。
   * @param forceStop 是否强制停止，强制停止会清空所有等待队列
   */
  @Override
  public synchronized void stop(boolean forceStop) {
    isRunning = false;
    if (storagePolicySatisfierThread == null) {
      return;
    }

    storageMovementNeeded.close();

    storagePolicySatisfierThread.interrupt();
    this.storageMovementsMonitor.stop();
    if (forceStop) {
      storageMovementNeeded.clearQueuesWithNotification();
    } else {
      LOG.info("Stopping StoragePolicySatisfier.");
    }
  }

  /**
   * 优雅停止存储策略满足器服务，等待工作线程处理完当前任务后退出。
   */
  @Override
  public synchronized void stopGracefully() {
    if (isRunning) {
      stop(false);
    }

    if (this.storageMovementsMonitor != null) {
      this.storageMovementsMonitor.stopGracefully();
    }

    if (storagePolicySatisfierThread != null) {
      try {
        storagePolicySatisfierThread.join(3000);
      } catch (InterruptedException ie) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Interrupted Exception while waiting to join sps thread,"
              + " ignoring it", ie);
        }
      }
    }
  }

  /**
   * 获取SPS服务是否正在运行。
   * @return true表示正在运行，false表示已停止
   */
  @Override
  public boolean isRunning() {
    return isRunning;
  }

  /**
   * SPS主工作线程循环，不断从等待队列取出待处理文件，分析块存储位置，生成移动任务提交给NameNode。
   */
  @Override
  public void run() {
    while (isRunning) {
      // 检查依赖服务是否正常运行
      if (!ctxt.isRunning()) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Upstream service is down, skipping the sps work.");
        }
        continue;
      }
      ItemInfo itemInfo = null;
      try {
        boolean retryItem = false;
        if (!ctxt.isInSafeMode()) {
          // 从待处理队列取出一个文件
          itemInfo = storageMovementNeeded.get();
          if (itemInfo != null) {
            // 超过最大重试次数，直接丢弃该文件
            if(itemInfo.getRetryCount() >= blockMovementMaxRetry){
              LOG.info("Failed to satisfy the policy after "
                  + blockMovementMaxRetry + " retries. Removing inode "
                  + itemInfo.getFile() + " from the queue");
              storageMovementNeeded.removeItemTrackInfo(itemInfo, false);
              continue;
            }
            long trackId = itemInfo.getFile();
            BlocksMovingAnalysis status = null;
            BlockStoragePolicy existingStoragePolicy;
            // 从NameNode获取文件信息
            HdfsFileStatus fileStatus = ctxt.getFileInfo(trackId);
            // 检查文件是否存在
            if (fileStatus == null || fileStatus.isDir()) {
              // 文件已删除或是目录，移除跟踪信息
              storageMovementNeeded.removeItemTrackInfo(itemInfo, true);
            } else {
              byte existingStoragePolicyID = fileStatus.getStoragePolicy();
              existingStoragePolicy = ctxt
                  .getStoragePolicy(existingStoragePolicyID);

              HdfsLocatedFileStatus file = (HdfsLocatedFileStatus) fileStatus;
              // 分析文件所有块，分配目标存储节点
              status = analyseBlocksStorageMovementsAndAssignToDN(file,
                  existingStoragePolicy);
              // 根据分析结果处理
              switch (status.status) {
              // 需要重试，添加到监控队列
              case ANALYSIS_SKIPPED_FOR_RETRY:
              // 找到目标，添加到监控队列跟踪移动结果
              case BLOCKS_TARGETS_PAIRED:
                if (LOG.isDebugEnabled()) {
                  LOG.debug("Block analysis status:{} for the file id:{}."
                      + " Adding to attempt monitor queue for the storage "
                      + "movement attempt finished report",
                      status.status, fileStatus.getFileId());
                }
                this.storageMovementsMonitor.add(itemInfo.getStartPath(),
                    itemInfo.getFile(), monotonicNow(), status.assignedBlocks,
                    itemInfo.getRetryCount());
                break;
              // 没有找到目标，需要重试
              case NO_BLOCKS_TARGETS_PAIRED:
                if (LOG.isDebugEnabled()) {
                  LOG.debug("Adding trackID:{} for the file id:{} back to"
                      + " retry queue as none of the blocks found its eligible"
                      + " targets.", trackId, fileStatus.getFileId());
                }
                retryItem = true;
                break;
              // 存在低冗余块，需要重试
              case FEW_LOW_REDUNDANCY_BLOCKS:
                if (LOG.isDebugEnabled()) {
                  LOG.debug("Adding trackID:{} for the file id:{} back to "
                      + "retry queue as some of the blocks are low redundant.",
                      trackId, fileStatus.getFileId());
                }
                retryItem = true;
                break;
              // 移动失败，需要重试
              case BLOCKS_FAILED_TO_MOVE:
                if (LOG.isDebugEnabled()) {
                  LOG.debug("Adding trackID:{} for the file id:{} back to "
                      + "retry queue as some of the blocks movement failed.",
                      trackId, fileStatus.getFileId());
                }
                retryItem = true;
                break;
              // 无需移动，清理跟踪信息
              case BLOCKS_TARGET_PAIRING_SKIPPED:
              case BLOCKS_ALREADY_SATISFIED:
              default:
                LOG.info("Block analysis status:{} for the file id:{}."
                    + " So, Cleaning up the Xattrs.", status.status,
                    fileStatus.getFileId());
                storageMovementNeeded.removeItemTrackInfo(itemInfo, true);
                break;
              }
            }
          }
        } else {
          // NameNode处于安全模式，暂停3秒后重试
          LOG.info("Namenode is in safemode. It will retry again.");
          Thread.sleep(3000);
        }
        int numLiveDn = ctxt.getNumLiveDataNodes();
        // 队列为空或已达到单次迭代处理上限，暂停3秒重置计数器
        if (storageMovementNeeded.size() == 0
            || blockCount > (numLiveDn * spsWorkMultiplier)) {
          Thread.sleep(3000);
          blockCount = 0L;
        }
        // 需要重试的文件重新加入队列
        if (retryItem) {
          this.storageMovementNeeded.add(itemInfo);
        }
      } catch (IOException e) {
        LOG.error("Exception during StoragePolicySatisfier execution - "
            + "will continue next cycle", e);
        // IO异常导致处理失败，重新加入队列重试
        this.storageMovementNeeded.add(itemInfo);
      } catch (Throwable t) {
        synchronized (this) {
          if (isRunning) {
            isRunning = false;
            if (t instanceof InterruptedException) {
              LOG.info("Stopping StoragePolicySatisfier.", t);
            } else {
              LOG.error("StoragePolicySatisfier thread received "
                  + "runtime exception.", t);
            }
            // 清空队列，停止监控线程
            this.clearQueues();
            this.storageMovementsMonitor.stopGracefully();
          }
        }
      }
    }
  }

  /**
   * 分析文件中所有块的存储位置，判断是否满足存储策略，为需要移动的块分配目标DataNode和存储类型。
   * @param fileInfo 文件信息，包含块位置信息
   * @param existingStoragePolicy 文件当前的存储策略
   * @return 分析结果，包含状态和分配好的块目标映射
   * @throws IOException 与NameNode交互IO异常
   */
  private BlocksMovingAnalysis analyseBlocksStorageMovementsAndAssignToDN(
      HdfsLocatedFileStatus fileInfo,
      BlockStoragePolicy existingStoragePolicy) throws IOException {
    BlocksMovingAnalysis.Status status =
        BlocksMovingAnalysis.Status.BLOCKS_ALREADY_SATISFIED;
    final ErasureCodingPolicy ecPolicy = fileInfo.getErasureCodingPolicy();
    final LocatedBlocks locatedBlocks = fileInfo.getLocatedBlocks();
    final boolean lastBlkComplete = locatedBlocks.isLastBlockComplete();
    // 文件还在写入中，最后一块未完成，延后处理
    if (!lastBlkComplete) {
      LOG.info("File: {} is under construction. So, postpone"
          + " this to the next retry iteration", fileInfo.getFileId());
      return new BlocksMovingAnalysis(
          BlocksMovingAnalysis.Status.ANALYSIS_SKIPPED_FOR_RETRY,
          new HashMap<>());
    }

    List<LocatedBlock> blocks = locatedBlocks.getLocatedBlocks();
    // 文件没有块，跳过分析
    if (blocks.size() == 0) {
      LOG.info("File: {} is not having any blocks."
          + " So, skipping the analysis.", fileInfo.getFileId());
      return new BlocksMovingAnalysis(
          BlocksMovingAnalysis.Status.BLOCKS_TARGET_PAIRING_SKIPPED,
          new HashMap<>());
    }
    List<BlockMovingInfo> blockMovingInfos = new ArrayList<BlockMovingInfo>();
    boolean hasLowRedundancyBlocks = false;
    int replication = fileInfo.getReplication();
    // 从缓存获取当前所有在线DataNode的存储信息
    DatanodeMap liveDns = dnCacheMgr.getLiveDatanodeStorageReport(ctxt);
    // 遍历每个块
    for (int i = 0; i < blocks.size(); i++) {
      LocatedBlock blockInfo = blocks.get(i);

      // 判断块是否低冗余，若位置数小于预期副本数/EC单元数，则标记为低冗余
      hasLowRedundancyBlocks |= isLowRedundancyBlock(blockInfo, replication,
          ecPolicy);

      List<StorageType> expectedStorageTypes;
      if (blockInfo.isStriped()) {
        // 检查存储策略是否支持EC条带块模式