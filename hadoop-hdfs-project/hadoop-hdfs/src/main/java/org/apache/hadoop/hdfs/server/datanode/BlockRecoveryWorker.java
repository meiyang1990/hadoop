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
package org.apache.hadoop.hdfs.server.datanode;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.base.Joiner;
import org.apache.hadoop.util.Preconditions;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.RecoveryInProgressException;
import org.apache.hadoop.hdfs.protocolPB.DatanodeProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.ReplicaState;
import org.apache.hadoop.hdfs.server.protocol.BlockRecoveryCommand.RecoveringBlock;
import org.apache.hadoop.hdfs.server.protocol.BlockRecoveryCommand.RecoveringStripedBlock;
import org.apache.hadoop.hdfs.server.protocol.InterDatanodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.ReplicaRecoveryInfo;
import org.apache.hadoop.hdfs.util.StripedBlockUtil;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.util.Daemon;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.hdfs.server.common.HdfsServerConstants.BLOCK_GROUP_INDEX_MASK;
import static org.apache.hadoop.hdfs.util.StripedBlockUtil.getInternalBlockLength;

/**
 * 文件：org.apache.hadoop.hdfs.server.datanode.BlockRecoveryWorker
 * 模块：HDFS DataNode
 * 核心职责：处理NameNode下发的块恢复命令，负责对未正常关闭的连续块和纠删码条带化块进行恢复操作，
 *          统一协调多个DataNode上的副本信息，最终将恢复结果提交给NameNode完成块同步。
 */
@InterfaceAudience.Private
public class BlockRecoveryWorker {
  public static final Logger LOG = DataNode.LOG;

  private final DataNode datanode;
  private final Configuration conf;
  private final DNConf dnConf;

  /**
   * 构造方法，创建块恢复工作器，关联当前DataNode实例
   * @param datanode 当前DataNode实例
   */
  BlockRecoveryWorker(DataNode datanode) {
    this.datanode = datanode;
    conf = datanode.getConf();
    dnConf = datanode.getDnConf();
  }

  /**
   * 块恢复记录类，存储单个副本所在DataNode信息、协议代理和恢复信息
   */
  static class BlockRecord {
    private final DatanodeID id;
    private final InterDatanodeProtocol datanode;
    private final ReplicaRecoveryInfo rInfo;

    private String storageID;

    BlockRecord(DatanodeID id, InterDatanodeProtocol datanode,
        ReplicaRecoveryInfo rInfo) {
      this.id = id;
      this.datanode = datanode;
      this.rInfo = rInfo;
    }

    /**
     * 请求对应DataNode更新恢复中副本的状态，更新为新的块ID和长度
     * @param bpid 块池ID
     * @param recoveryId 恢复使用的新一代号
     * @param newBlockId 恢复后的新块ID
     * @param newLength 恢复后的新块长度
     * @throws IOException 网络调用或更新失败抛出异常
     */
    private void updateReplicaUnderRecovery(String bpid, long recoveryId,
        long newBlockId, long newLength) throws IOException {
      final ExtendedBlock b = new ExtendedBlock(bpid, rInfo);
      storageID = datanode.updateReplicaUnderRecovery(b, recoveryId, newBlockId,
          newLength);
    }

    public ReplicaRecoveryInfo getReplicaRecoveryInfo(){
      return rInfo;
    }

    @Override
    public String toString() {
      return "block:" + rInfo + " node:" + id;
    }
  }

  /**
   * 连续块恢复任务类，处理普通连续块的块恢复流程
   */
  class RecoveryTaskContiguous {
    private final RecoveringBlock rBlock;
    private final ExtendedBlock block;
    private final String bpid;
    private final DatanodeInfo[] locs;
    private final long recoveryId;

    RecoveryTaskContiguous(RecoveringBlock rBlock) {
      this.rBlock = rBlock;
      block = rBlock.getBlock();
      bpid = block.getBlockPoolId();
      locs = rBlock.getLocations();
      recoveryId = rBlock.getNewGenerationStamp();
    }

    /**
     * 执行连续块恢复主流程：收集所有副本信息、校验合法性、执行块同步
     * @throws IOException 恢复失败抛出异常
     */
    protected void recover() throws IOException {
      List<BlockRecord> syncList = new ArrayList<>(locs.length);
      int errorCount = 0;
      int candidateReplicaCnt = 0;
      // 故障注入：用于测试时延迟恢复流程
      DataNodeFaultInjector.get().delay();

      // 遍历所有持有副本的DataNode，收集符合恢复条件的副本
      for(DatanodeID id : locs) {
        try {
          DatanodeID bpReg = getDatanodeID(bpid);
          // 如果是本节点直接使用本地实例，否则创建跨节点RPC代理
          InterDatanodeProtocol proxyDN = bpReg.equals(id)?
              datanode: DataNode.createInterDataNodeProtocolProxy(id, conf,
              dnConf.socketTimeout, dnConf.connectToDnViaHostname);
          // 初始化副本恢复，获取副本信息
          ReplicaRecoveryInfo info = callInitReplicaRecovery(proxyDN, rBlock);
          if (info != null &&
              info.getGenerationStamp() >= block.getGenerationStamp() &&
              info.getNumBytes() > 0) {
            // 统计符合基本条件的候选副本数量
            ++candidateReplicaCnt;
            // 仅保留原始状态为RWR或更优状态的副本参与恢复
            if (info.getOriginalReplicaState().getValue() <=
                ReplicaState.RWR.getValue()) {
              syncList.add(new BlockRecord(id, proxyDN, info));
            } else {
              LOG.debug("Block recovery: Ignored replica with invalid " +
                  "original state: {} from DataNode: {}", info, id);
            }
          } else {
            // 记录不符合条件副本的原因
            if (info == null) {
              LOG.debug("Block recovery: DataNode: {} does not have " +
                  "replica for block: {}", id, block);
            } else {
              LOG.debug("Block recovery: Ignored replica with invalid "
                  + "generation stamp or length: {} from DataNode: {}", info, id);
            }
          }
        } catch (RecoveryInProgressException ripE) {
          // 该块已经在恢复中，直接终止本次恢复
          InterDatanodeProtocol.LOG.warn(
              "Recovery for replica {} on data-node {} is already in progress. " +
                  "Recovery id = {} is aborted.", block, id, rBlock.getNewGenerationStamp(), ripE);
          return;
        } catch (IOException e) {
          // 该DataNode调用失败，统计错误数量
          ++errorCount;
          InterDatanodeProtocol.LOG.warn("Failed to recover block (block={}, datanode={})",
              block, id, e);
        }
      }

      // 所有DataNode都调用失败，抛出异常
      if (errorCount == locs.length) {
        throw new IOException("All datanodes failed: block=" + block
            + ", datanodeids=" + Arrays.asList(locs));
      }

      // 存在候选副本但没有符合状态要求的副本，抛出异常
      if (candidateReplicaCnt > 0 && syncList.isEmpty()) {
        throw new IOException("Found " + candidateReplicaCnt +
            " replica(s) for block " + block + " but none is in " +
            ReplicaState.RWR.name() + " or better state. datanodeids=" +
            Arrays.asList(locs));
      }

      // 执行块同步，完成恢复
      syncBlock(syncList);
    }

    /**
     * 块同步流程：确定最优块状态和长度，更新所有参与恢复副本，提交结果给NameNode
     * @param syncList 符合条件参与恢复的副本列表
     * @throws IOException 同步失败抛出异常
     */
    void syncBlock(List<BlockRecord> syncList) throws IOException {
      // 获取对应块池的活跃NameNode代理
      DatanodeProtocolClientSideTranslatorPB nn =
          getActiveNamenodeForBP(block.getBlockPoolId());

      // 判断是否是截断恢复（truncate操作触发的恢复）
      boolean isTruncateRecovery = rBlock.getNewBlock() != null;
      // 截断恢复使用新块ID，否则使用原块ID
      long blockId = (isTruncateRecovery) ?
          rBlock.getNewBlock().getBlockId() : block.getBlockId();

      LOG.info("BlockRecoveryWorker: block={} (length={}),"
              + " isTruncateRecovery={}, syncList={}", block,
          block.getNumBytes(), isTruncateRecovery, syncList);

      // 没有符合条件的副本，通知NameNode删除该块
      if (syncList.isEmpty()) {
        LOG.debug("syncBlock for block {}, all datanodes don't " +
            "have the block or their replicas have 0 length. The block can " +
            "be deleted.", block);
        nn.commitBlockSynchronization(block, recoveryId, 0,
            true, true, DatanodeID.EMPTY_ARRAY, null);
        return;
      }

      // 确定最优副本状态：值越小状态越好
      ReplicaState bestState = ReplicaState.RWR;
      long finalizedLength = -1;
      for (BlockRecord r : syncList) {
        assert r.rInfo.getNumBytes() > 0 : "zero length replica";
        ReplicaState rState = r.rInfo.getOriginalReplicaState();
        if (rState.getValue() < bestState.getValue()) {
          bestState = rState;
        }
        // 如果是已完成块，记录长度，检查一致性
        if(rState == ReplicaState.FINALIZED) {
          if (finalizedLength > 0 && finalizedLength != r.rInfo.getNumBytes()) {
            throw new IOException("Inconsistent size of finalized replicas. " +
                "Replica " + r.rInfo + " expected size: " + finalizedLength);
          }
          finalizedLength = r.rInfo.getNumBytes();
        }
      }

      // 根据最优状态确定参与恢复的副本和最终块长度
      List<BlockRecord> participatingList = new ArrayList<>();
      final ExtendedBlock newBlock = new ExtendedBlock(bpid, blockId,
          -1, recoveryId);
      switch(bestState) {
      case FINALIZED:
        // 最优状态是已完成，所有已完成块和长度匹配的RBW块参与恢复
        assert finalizedLength > 0 : "finalizedLength is not positive";
        for(BlockRecord r : syncList) {
          ReplicaState rState = r.rInfo.getOriginalReplicaState();
          if (rState == ReplicaState.FINALIZED ||
              rState == ReplicaState.RBW &&
                  r.rInfo.getNumBytes() == finalizedLength) {
            participatingList.add(r);
          }
          LOG.debug("syncBlock replicaInfo: block={}, from datanode {}, receivedState={}, " +
              "receivedLength={}, bestState=FINALIZED, finalizedLength={}",
              block, r.id, rState.name(), r.rInfo.getNumBytes(), finalizedLength);
        }
        newBlock.setNumBytes(finalizedLength);
        break;
      case RBW:
      case RWR:
        // 最优状态是正在写入/等待恢复，取所有同状态副本中的最小长度作为最终长度
        long minLength = Long.MAX_VALUE;
        for(BlockRecord r : syncList) {
          ReplicaState rState = r.rInfo.getOriginalReplicaState();
          if(rState == bestState) {
            minLength = Math.min(minLength, r.rInfo.getNumBytes());
            participatingList.add(r);
          }
          LOG.debug("syncBlock replicaInfo: block={}, from datanode {}, receivedState={}, " +
              "receivedLength={}, bestState={}", block, r.id, rState.name(),
              r.rInfo.getNumBytes(), bestState.name());
        }
        // recover方法保证syncList至少有一个符合状态的副本，此处minLength不应为最大值
        if (minLength == Long.MAX_VALUE) {
          throw new IOException("Incorrect block size");
        }
        newBlock.setNumBytes(minLength);
        break;
      case RUR:
      case TEMPORARY:
        // 这两种状态不应该出现在syncList中，断言失败
        assert false : "bad replica state: " + bestState;
      default:
        break; // 枚举所有值，default无实际逻辑
      }
      // 截断恢复覆盖最终长度，使用指定的截断后长度
      if (isTruncateRecovery) {
        newBlock.setNumBytes(rBlock.getNewBlock().getNumBytes());
      }

      LOG.info("BlockRecoveryWorker: block={} (length={}), bestState={},"
              + " newBlock={} (length={}), participatingList={}",
          block, block.getNumBytes(), bestState.name(), newBlock,
          newBlock.getNumBytes(), participatingList);

      List<DatanodeID> failedList = new ArrayList<>();
      final List<BlockRecord> successList = new ArrayList<>();
      // 通知所有参与恢复的DataNode更新副本状态到新长度和代
      for (BlockRecord r : participatingList) {
        try {
          r.updateReplicaUnderRecovery(bpid, recoveryId, blockId,
              newBlock.getNumBytes());
          successList.add(r);
        } catch (IOException e) {
          InterDatanodeProtocol.LOG.warn("Failed to updateBlock (newblock="
              + newBlock + ", datanode=" + r.id + ")", e);
          failedList.add(r.id);
        }
      }

      // 所有更新都失败，抛出异常
      if (successList.isEmpty()) {
        throw new IOException("Cannot recover " + block
            + ", the following datanodes failed: " + failedList);
      }

      // 收集恢复成功的DataNode和存储ID，准备提交给NameNode
      final DatanodeID[] datanodes = new DatanodeID[successList.size()];
      final String[] storages = new String[datanodes.length];
      for (int i = 0; i < datanodes.length; i++) {
        final BlockRecord r = successList.get(i);
        datanodes[i] = r.id;
        storages[i] = r.storageID;
      }

      LOG.debug("Datanode triggering commitBlockSynchronization, block={}, newGs={}, " +
          "newLength={}", block, newBlock.getGenerationStamp(), newBlock.getNumBytes());

      // 提交块同步结果给NameNode，完成恢复
      nn.commitBlockSynchronization(block,
          newBlock.getGenerationStamp(), newBlock.getNumBytes(), true, false,
          datanodes, storages);
    }
  }

  /**
   * 条带化块恢复任务类，处理纠删码块组中未正常关闭的条带块恢复，
   * 计算所有内部块的安全长度，截断到一致可解码位置，保证块组数据一致性。
   */
  public class RecoveryTaskStriped {
    private final RecoveringBlock rBlock;
    private final ExtendedBlock block;
    private final String bpid;
    private final DatanodeInfo[] locs;
    private final long recoveryId;

    private final byte[] blockIndices;
    private final ErasureCodingPolicy ecPolicy;

    RecoveryTaskStriped(RecoveringStripedBlock rBlock) {
      this.rBlock = rBlock;
      // 目前暂不支持条带化块截断恢复
      Preconditions.checkArgument(rBlock.getNewBlock() == null);

      block = rBlock.getBlock();
      bpid = block.getBlockPoolId();
      locs = rBlock.getLocations();
      recoveryId = rBlock.getNewGenerationStamp();
      blockIndices = rBlock.getBlockIndices();
      ecPolicy = rBlock.getErasureCodingPolicy();
    }

    /**
     * 执行条带化块恢复主流程：收集所有内部块信息、计算安全长度、截断内部块、提交结果给NameNode
     * @throws IOException 恢复失败抛出异常
     */