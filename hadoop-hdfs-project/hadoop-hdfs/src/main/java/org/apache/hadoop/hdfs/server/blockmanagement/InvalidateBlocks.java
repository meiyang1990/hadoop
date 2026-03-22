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

import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.LongAdder;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.util.LightWeightHashSet;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.hdfs.DFSUtil;

import org.apache.hadoop.classification.VisibleForTesting;

/**
 * 维护每个数据节点上待删除（无效化）的数据块集合，管理NameNode启动阶段延迟删除块的逻辑。
 * 在HDFS中，当数据块被删除时，NameNode会先将块标记为待删除，分批发送给数据节点进行删除，
 * 本类负责缓存和管理这些待删除块，支持区分普通块和EC纠删码块。
 */
@InterfaceAudience.Private
class InvalidateBlocks {
  // 按数据节点分组存储待删除的普通连续块
  private final Map<DatanodeInfo, LightWeightHashSet<Block>>
      nodeToBlocks = new HashMap<>();
  // 按数据节点分组存储待删除的EC纠删码块
  private final Map<DatanodeInfo, LightWeightHashSet<Block>>
      nodeToECBlocks = new HashMap<>();
  // 待删除普通块总数
  private final LongAdder numBlocks = new LongAdder();
  // 待删除EC纠删码块总数
  private final LongAdder numECBlocks = new LongAdder();
  // 单次发送给数据节点的最大无效块数量限制
  private final int blockInvalidateLimit;
  // 块ID管理器，用于区分块类型
  private final BlockIdManager blockIdManager;

  /**
   * NameNode启动后块删除延迟等待时长
   */
  private final long pendingPeriodInMs;
  /** NameNode启动时间 */
  private final long startupTime = Time.monotonicNow();

  /**
   * 构造无效块管理器，初始化配置并打印块删除预计开始时间
   * @param blockInvalidateLimit 单次批量删除的最大块数量限制
   * @param pendingPeriodInMs 启动后块删除延迟等待时长
   * @param blockIdManager 块ID管理器，用于区分块类型
   */
  InvalidateBlocks(final int blockInvalidateLimit, long pendingPeriodInMs,
                   final BlockIdManager blockIdManager) {
    this.blockInvalidateLimit = blockInvalidateLimit;
    this.pendingPeriodInMs = pendingPeriodInMs;
    this.blockIdManager = blockIdManager;
    printBlockDeletionTime();
  }

  /**
   * 打印块删除延迟配置信息和预计开始时间日志
   */
  private void printBlockDeletionTime() {
    BlockManager.LOG.info("{} is set to {}",
        DFSConfigKeys.DFS_NAMENODE_STARTUP_DELAY_BLOCK_DELETION_SEC_KEY,
        DFSUtil.durationToString(pendingPeriodInMs));
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy MMM dd HH:mm:ss");
    Calendar calendar = new GregorianCalendar();
    calendar.add(Calendar.SECOND, (int) (this.pendingPeriodInMs / 1000));
    BlockManager.LOG.info("The block deletion will start around {}",
        sdf.format(calendar.getTime()));
  }

  /**
   * 获取所有待无效化块的总数（包含普通块和EC块）
   * @return 待删除块总数
   */
  long numBlocks() {
    return getECBlocks() + getBlocks();
  }

  /**
   * 获取待无效化普通连续块的总数
   * @return 待删除普通块总数
   */
  long getBlocks() {
    return numBlocks.longValue();
  }

  /**
   * 获取待无效化EC条带块的总数
   * @return 待删除EC块总数
   */
  long getECBlocks() {
    return numECBlocks.longValue();
  }

  /**
   * 获取指定数据节点对应的普通块集合
   * @param dn 目标数据节点信息
   * @return 普通待删除块集合，不存在则返回null
   */
  private LightWeightHashSet<Block> getBlocksSet(final DatanodeInfo dn) {
    return nodeToBlocks.get(dn);
  }

  /**
   * 获取指定数据节点对应的EC块集合
   * @param dn 目标数据节点信息
   * @return EC待删除块集合，不存在则返回null
   */
  private LightWeightHashSet<Block> getECBlocksSet(final DatanodeInfo dn) {
    return nodeToECBlocks.get(dn);
  }

  /**
   * 根据块类型获取对应数据节点对应的块集合
   * @param dn 目标数据节点信息
   * @param block 待处理块
   * @return 对应类型的待删除块集合
   */
  private LightWeightHashSet<Block> getBlocksSet(final DatanodeInfo dn,
      final Block block) {
    if (blockIdManager.isStripedBlock(block)) {
      return getECBlocksSet(dn);
    } else {
      return getBlocksSet(dn);
    }
  }

  /**
   * 将块集合存入对应类型的map中
   * @param dn 目标数据节点
   * @param block 块实例，用于判断类型
   * @param set 待存储的块集合
   */
  private void putBlocksSet(final DatanodeInfo dn, final Block block,
      final LightWeightHashSet set) {
    if (blockIdManager.isStripedBlock(block)) {
      assert getECBlocksSet(dn) == null;
      nodeToECBlocks.put(dn, set);
    } else {
      assert getBlocksSet(dn) == null;
      nodeToBlocks.put(dn, set);
    }
  }

  /**
   * 获取指定数据节点上所有待删除块总数
   * @param dn 目标数据节点
   * @return 该节点待删除块总数（普通块+EC块）
   */
  private long getBlockSetsSize(final DatanodeInfo dn) {
    LightWeightHashSet<Block> replicaBlocks = getBlocksSet(dn);
    LightWeightHashSet<Block> stripedBlocks = getECBlocksSet(dn);
    return ((replicaBlocks == null ? 0 : replicaBlocks.size()) +
        (stripedBlocks == null ? 0 : stripedBlocks.size()));
  }


  /**
   * 检查指定数据节点上是否存在指定块在待删除列表中，需要匹配生成戳。
   * 如果同一块有不同生成戳的版本待删除，则返回false，只有生成戳匹配才返回true。
   * @param dn 目标数据节点
   * @param block 待检查块
   * @return 如果存在且生成戳匹配返回true，否则返回false
   */
  synchronized boolean contains(final DatanodeInfo dn, final Block block) {
    final LightWeightHashSet<Block> s = getBlocksSet(dn, block);
    if (s == null) {
      return false; // no invalidate blocks for this storage ID
    }
    Block blockInSet = s.getElement(block);
    return blockInSet != null &&
        block.getGenerationStamp() == blockInSet.getGenerationStamp();
  }

  /**
   * 添加一个块到指定数据节点的待删除列表中
   * @param block 待删除块
   * @param datanode 块所在的数据节点
   * @param log 是否需要记录块状态变更日志
   */
  synchronized void add(final Block block, final DatanodeInfo datanode,
      final boolean log) {
    LightWeightHashSet<Block> set = getBlocksSet(datanode, block);
    if (set == null) {
      set = new LightWeightHashSet<>();
      putBlocksSet(datanode, block, set);
    }
    if (set.add(block)) {
      if (blockIdManager.isStripedBlock(block)) {
        numECBlocks.increment();
      } else {
        numBlocks.increment();
      }
      if (log) {
        NameNode.blockStateChangeLog.debug("BLOCK* {}: add {} to {}",
            getClass().getSimpleName(), block, datanode);
      }
    }
  }

  /**
   * 移除指定数据节点的所有待删除块
   * @param dn 目标数据节点
   */
  synchronized void remove(final DatanodeInfo dn) {
    LightWeightHashSet<Block> replicaBlockSets = nodeToBlocks.remove(dn);
    if (replicaBlockSets != null) {
      numBlocks.add(replicaBlockSets.size() * -1);
    }
    LightWeightHashSet<Block> ecBlocksSet = nodeToECBlocks.remove(dn);
    if (ecBlocksSet != null) {
      numECBlocks.add(ecBlocksSet.size() * -1);
    }
  }

  /**
   * 从指定数据节点的待删除列表中移除特定块
   * @param dn 目标数据节点
   * @param block 待移除块
   */
  synchronized void remove(final DatanodeInfo dn, final Block block) {
    final LightWeightHashSet<Block> v = getBlocksSet(dn, block);
    if (v != null && v.remove(block)) {
      if (blockIdManager.isStripedBlock(block)) {
        numECBlocks.decrement();
      } else {
        numBlocks.decrement();
      }
      if (v.isEmpty() && getBlockSetsSize(dn) == 0) {
        remove(dn);
      }
    }
  }

  /**
   * 将指定map中存储的待删除块信息打印输出到PrintWriter
   * @param nodeToBlocksMap 存储待删除块的map（普通块或EC块）
   * @param out 输出流
   */
  private void dumpBlockSet(final Map<DatanodeInfo,
      LightWeightHashSet<Block>> nodeToBlocksMap, final PrintWriter out) {
    for(Entry<DatanodeInfo, LightWeightHashSet<Block>> entry :
        nodeToBlocksMap.entrySet()) {
      final LightWeightHashSet<Block> blocks = entry.getValue();
      if (blocks != null && blocks.size() > 0) {
        out.println(entry.getKey());
        out.println(StringUtils.join(',', blocks));
      }
    }
  }

  /**
   * 将所有待删除块信息转储到输出流，用于元数据保存和调试
   * @param out 输出流
   */
  synchronized void dump(final PrintWriter out) {
    final int size = nodeToBlocks.values().size() +
        nodeToECBlocks.values().size();
    out.println("Metasave: Blocks " + numBlocks()
        + " waiting deletion from " + size + " datanodes.");
    if (size == 0) {
      return;
    }
    dumpBlockSet(nodeToBlocks, out);
    dumpBlockSet(nodeToECBlocks, out);
  }

  /**
   * 获取所有包含待删除块的数据节点列表
   * @return 包含待删除块的数据节点列表去重后的结果
   */
  synchronized List<DatanodeInfo> getDatanodes() {
    HashSet<DatanodeInfo> set = new HashSet<>();
    set.addAll(nodeToBlocks.keySet());
    set.addAll(nodeToECBlocks.keySet());
    return new ArrayList<>(set);
  }

  /**
   * 获取块删除还需要延迟等待的剩余时间
   * @return 剩余等待时间，单位毫秒，如果延迟已结束则返回负数
   */
  @VisibleForTesting
  long getInvalidationDelay() {
    return pendingPeriodInMs - (Time.monotonicNow() - startupTime);
  }

  /**
   * 按照数量限制从待删除块集合中拉取块，取出后从原集合移除，更新计数统计
   * @param blockSet 待处理块集合
   * @param toInvalidate 输出参数，取出的待删除块会添加到该列表
   * @param statsAdder 对应类型的总数统计计数器
   * @param limit 本次最多取出的块数量限制
   * @return 拉取后剩余还可取出的数量配额
   */
  private int getBlocksToInvalidateByLimit(LightWeightHashSet<Block> blockSet,
      List<Block> toInvalidate, LongAdder statsAdder, int limit) {
    assert blockSet != null;
    int remainingLimit = limit;
    List<Block> polledBlocks = blockSet.pollN(limit);
    remainingLimit -= polledBlocks.size();
    toInvalidate.addAll(polledBlocks);
    statsAdder.add(polledBlocks.size() * -1);
    return remainingLimit;
  }

  /**
   * 为指定数据节点生成本次心跳周期需要处理的待无效化块列表，按照单次最大数量限制返回
   * @param dn 目标数据节点描述符
   * @return 本次需要删除的块列表，如果删除延迟未结束则返回null
   */
  synchronized List<Block> invalidateWork(final DatanodeDescriptor dn) {
    final long delay = getInvalidationDelay();
    // 如果还在启动延迟期，不处理删除请求，返回null
    if (delay > 0) {
      BlockManager.LOG
          .debug("Block deletion is delayed during NameNode startup. "
              + "The deletion will start after {} ms.", delay);
      return null;
    }

    int remainingLimit = blockInvalidateLimit;
    final List<Block> toInvalidate = new ArrayList<>();

    // 先拉取普通块，再拉取EC块，不超过单次数量限制
    if (nodeToBlocks.get(dn) != null) {
      remainingLimit = getBlocksToInvalidateByLimit(nodeToBlocks.get(dn),
          toInvalidate, numBlocks, remainingLimit);
    }
    if ((remainingLimit > 0) && (nodeToECBlocks.get(dn) != null)) {
      getBlocksToInvalidateByLimit(nodeToECBlocks.get(dn),
          toInvalidate, numECBlocks, remainingLimit);
    }
    // 如果该节点已经没有待删除块，从map中移除节点条目
    if (toInvalidate.size() > 0) {
      if (getBlockSetsSize(dn) == 0) {
        remove(dn);
      }
      // 将待删除块添加到数据节点描述符的待删除列表，后续会通过心跳发送给数据节点
      dn.addBlocksToBeInvalidated(toInvalidate);
    }
    return toInvalidate;
  }
  
  /**
   * 清空所有待无效化块，重置所有计数统计
   */
  synchronized void clear() {
    nodeToBlocks.clear();
    nodeToECBlocks.clear();
    numBlocks.reset();
    numECBlocks.reset();
  }
}