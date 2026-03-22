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

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.LongAdder;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockType;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.ipc.Server;

import org.apache.hadoop.classification.VisibleForTesting;

/**
 * 文件级注释：HDFS坏副本信息存储管理类，维护文件系统中所有标记为损坏的块副本信息
 * 
 * 只有当块的所有副本都损坏时，该块才会被视为整体损坏。在报告块的可用副本时，会隐藏损坏副本，
 * 当块重新达到预期的好副本数量时，会从该映射中移除对应的损坏记录。
 * 核心映射关系：Block -> DatanodeDescriptor -> Reason，记录每个块在哪些数据节点上有损坏副本以及损坏原因。
 */

@InterfaceAudience.Private
/**
 * 坏副本原因枚举，定义了块副本损坏的各种可能原因
 */
public enum Reason {
  NONE,                // 未指定原因
  ANY,                 // 通配符原因，用于匹配任意原因的删除操作
  GENSTAMP_MISMATCH,   // 块生成时间戳不匹配
  SIZE_MISMATCH,       // 块大小不匹配
  INVALID_STATE,       // 副本状态无效
  CORRUPTION_REPORTED  // 客户端或数据节点主动上报损坏
}

public class CorruptReplicasMap{

  // 坏副本核心存储，第一层key为块，第二层key为数据节点描述符，value为损坏原因
  private final Map<Block, Map<DatanodeDescriptor, Reason>> corruptReplicasMap =
    new HashMap<Block, Map<DatanodeDescriptor, Reason>>();

  // 统计普通连续块中损坏块的总数量，使用LongAdder保证高并发计数性能
  private final LongAdder totalCorruptBlocks = new LongAdder();
  // 统计EC纠删码块组中损坏块组的总数量，使用LongAdder保证高并发计数性能
  private final LongAdder totalCorruptECBlockGroups = new LongAdder();

  /**
   * 将指定数据节点上的块标记为损坏，并添加到坏副本映射中
   *
   * @param blk 要添加到坏副本映射的块
   * @param dn 存储该损坏副本的数据节点描述符
   * @param reason 文本形式的损坏原因，用于日志记录
   * @param reasonCode 枚举形式的损坏原因
   * @param isStriped 是否为纠删码条带化块
   */
  void addToCorruptReplicasMap(Block blk, DatanodeDescriptor dn,
      String reason, Reason reasonCode, boolean isStriped) {
    // 获取当前块对应的所有损坏节点映射
    Map <DatanodeDescriptor, Reason> nodes = corruptReplicasMap.get(blk);
    if (nodes == null) {
      // 当前块无损坏记录，新建映射并添加到全局存储
      nodes = new HashMap<DatanodeDescriptor, Reason>();
      corruptReplicasMap.put(blk, nodes);
      // 增加对应类型的损坏块统计
      incrementBlockStat(isStriped);
    }
    
    String reasonText;
    // 拼接日志用的原因文本
    if (reason != null) {
      reasonText = " because " + reason;
    } else {
      reasonText = "";
    }
    
    // 输出不同的调试日志：区分首次添加和重复添加
    if (!nodes.keySet().contains(dn)) {
      NameNode.blockStateChangeLog.debug(
          "BLOCK NameSystem.addToCorruptReplicasMap: {} added as corrupt on "
              + "{} by {} {}", blk, dn, Server.getRemoteIp(),
          reasonText);
    } else {
      NameNode.blockStateChangeLog.debug(
          "BLOCK NameSystem.addToCorruptReplicasMap: duplicate requested for" +
              " {} to add as corrupt on {} by {} {}", blk, dn,
          Server.getRemoteIp(), reasonText);
    }
    // 添加节点或更新损坏原因
    nodes.put(dn, reasonCode);
  }

  /**
   * 从坏副本映射中移除整个块的所有损坏记录
   * @param blk 要移除的块
   */
  void removeFromCorruptReplicasMap(BlockInfo blk) {
    if (corruptReplicasMap != null) {
      Map<DatanodeDescriptor, Reason> value = corruptReplicasMap.remove(blk);
      if (value != null) {
        // 移除成功后减少对应类型的损坏块统计
        decrementBlockStat(blk.isStriped());
      }
    }
  }

  /**
   * 从坏副本映射中移除指定数据节点上的块损坏记录
   * @param blk 要移除的块
   * @param datanode 该块所在的数据节点
   * @return true 如果移除成功；false 如果该副本不在坏副本映射中
   */ 
  boolean removeFromCorruptReplicasMap(
      BlockInfo blk, DatanodeDescriptor datanode) {
    return removeFromCorruptReplicasMap(blk, datanode, Reason.ANY);
  }

  /**
   * 按指定原因从坏副本映射中移除指定数据节点上的块损坏记录
   * @param blk 要移除的块
   * @param datanode 该块所在的数据节点
   * @param reason 指定的损坏原因，Reason.ANY表示匹配任意原因
   * @return true 如果移除成功；false 如果原因不匹配或副本不存在
   */
  boolean removeFromCorruptReplicasMap(
      BlockInfo blk, DatanodeDescriptor datanode, Reason reason) {
    // 获取当前块对应的所有损坏节点映射
    Map <DatanodeDescriptor, Reason> datanodes = corruptReplicasMap.get(blk);
    if (datanodes == null) {
      return false;
    }

    // 如果指定了具体原因且和存储的原因不匹配，直接返回移除失败
    Reason storedReason = datanodes.get(datanode);
    if (reason != Reason.ANY && storedReason != null &&
        reason != storedReason) {
      return false;
    }

    // 移除该数据节点的损坏记录
    if (datanodes.remove(datanode) != null) {
      // 如果移除后当前块已经没有损坏副本，移除整个块的映射并更新统计
      if (datanodes.isEmpty()) {
        corruptReplicasMap.remove(blk);
        decrementBlockStat(blk.isStriped());
      }
      return true;
    }
    return false;
  }

  /**
   * 按块类型增加损坏块统计计数
   * @param isStriped 是否为纠删码条带化块
   */
  private void incrementBlockStat(boolean isStriped) {
    if (isStriped) {
      totalCorruptECBlockGroups.increment();
    } else {
      totalCorruptBlocks.increment();
    }
  }

  /**
   * 按块类型减少损坏块统计计数
   * @param isStriped 是否为纠删码条带化块
   */
  private void decrementBlockStat(boolean isStriped) {
    if (isStriped) {
      totalCorruptECBlockGroups.decrement();
    } else {
      totalCorruptBlocks.decrement();
    }
  }

  /**
   * 获取拥有指定块损坏副本的所有数据节点集合
   * 
   * @param blk 要查询的块
   * @return 拥有损坏副本的数据节点集合，如果块不存在坏记录则返回null
   */
  Collection<DatanodeDescriptor> getNodes(Block blk) {
    Map <DatanodeDescriptor, Reason> nodes = corruptReplicasMap.get(blk);
    if (nodes == null)
      return null;
    return nodes.keySet();
  }

  /**
   * 检查指定数据节点上的块副本是否损坏
   *
   * @param blk 要检查的块
   * @param node 存储该副本的数据节点描述符
   * @return true 如果副本在坏副本映射中标记为损坏；false否则
   */
  boolean isReplicaCorrupt(Block blk, DatanodeDescriptor node) {
    Collection<DatanodeDescriptor> nodes = getNodes(blk);
    return ((nodes != null) && (nodes.contains(node)));
  }

  /**
   * 获取指定块的损坏副本数量
   * @param blk 要查询的块
   * @return 损坏副本数量，无损坏则返回0
   */
  int numCorruptReplicas(Block blk) {
    Collection<DatanodeDescriptor> nodes = getNodes(blk);
    return (nodes == null) ? 0 : nodes.size();
  }
  
  /**
   * 获取拥有损坏副本的块总数量
   * @return 拥有至少一个损坏副本的块总数
   */
  int size() {
    return corruptReplicasMap.size();
  }

  /**
   * 分页获取指定类型的损坏块ID，仅用于测试
   * 从startingBlockId之后开始返回最多numExpectedBlocks个损坏块ID
   *
   * @param bim 块ID管理器，用于判断块类型
   * @param blockType 期望返回的块类型
   * @param numExpectedBlocks 期望返回的块数量，范围0 <= numExpectedBlocks <= 100
   * @param startingBlockId 起始块ID，null表示从头开始
   * @return 符合条件的损坏块ID数组，参数非法或起始块不存在则返回null
   */
  @VisibleForTesting
  long[] getCorruptBlockIdsForTesting(BlockIdManager bim, BlockType blockType,
      int numExpectedBlocks, Long startingBlockId) {
    if (numExpectedBlocks < 0 || numExpectedBlocks > 100) {
      return null;
    }
    // 设置游标起始位置
    long cursorBlockId =
        startingBlockId != null ? startingBlockId : Long.MIN_VALUE;
    return corruptReplicasMap.keySet()
        .stream()
        // 按块类型和块ID起始位置过滤
        .filter(r -> {
          if (blockType == BlockType.STRIPED) {
            return bim.isStripedBlock(r) && r.getBlockId() >= cursorBlockId;
          } else {
            return !bim.isStripedBlock(r) && r.getBlockId() >= cursorBlockId;
          }
        })
        // 按块ID排序
        .sorted()
        // 限制返回数量
        .limit(numExpectedBlocks)
        // 转换为块ID数组
        .mapToLong(Block::getBlockId)
        .toArray();
  }

  /**
   * 获取所有存在损坏副本的块集合
   * @return 所有存在至少一个损坏副本的块对象集合
   */
  Set<Block> getCorruptBlocksSet() {
    Set<Block> corruptBlocks = new HashSet<Block>();
    corruptBlocks.addAll(corruptReplicasMap.keySet());
    return corruptBlocks;
  }

  /**
   * 获取指定块在指定数据节点上的损坏原因文本
   * @param block 存在损坏副本的块
   * @param node 存储该损坏副本的数据节点
   * @return 损坏原因文本，不存在则返回null
   */
  String getCorruptReason(Block block, DatanodeDescriptor node) {
    Reason reason = null;
    if(corruptReplicasMap.containsKey(block)) {
      if (corruptReplicasMap.get(block).containsKey(node)) {
        reason = corruptReplicasMap.get(block).get(node);
      }
    }
    if (reason != null) {
      return reason.toString();
    } else {
      return null;
    }
  }

  /**
   * 获取普通连续损坏块的总数量
   * @return 普通连续损坏块总数
   */
  long getCorruptBlocks() {
    return totalCorruptBlocks.longValue();
  }

  /**
   * 获取损坏EC块组的总数量
   * @return 损坏EC块组总数
   */
  long getCorruptECBlockGroups() {
    return totalCorruptECBlockGroups.longValue();
  }
}