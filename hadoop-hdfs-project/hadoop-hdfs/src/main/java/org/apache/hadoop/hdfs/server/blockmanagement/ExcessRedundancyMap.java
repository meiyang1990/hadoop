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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.util.LightWeightHashSet;
import org.slf4j.Logger;

import org.apache.hadoop.classification.VisibleForTesting;

import static org.apache.hadoop.util.Time.monotonicNow;

/**
 * 文件：过剩冗余块记录表，维护每个DataNode上存储的过剩冗余块信息
 * HDFS中块副本数超过配置的副本系数时，多出的副本即为过剩冗余块，需要被删除回收
 * 该类线程安全，支持多线程并发访问
 */
class ExcessRedundancyMap {
  public static final Logger blockLog = NameNode.blockStateChangeLog;

  private final Map<String, LightWeightHashSet<Block>> map = new HashMap<>();
  private final AtomicLong size = new AtomicLong(0L);

  /**
   * 获取当前map中存储的过剩冗余块总数
   * @return 过剩冗余块总数
   */
  long size() {
    return size.get();
  }

  /**
   * 仅用于测试：获取指定DataNode上的过剩冗余块数量
   * @param dnUuid DataNode的UUID标识
   * @return 指定DataNode上的过剩冗余块数量
   */
  @VisibleForTesting
  synchronized int getSize4Testing(String dnUuid) {
    final LightWeightHashSet<Block> set = map.get(dnUuid);
    return set == null? 0: set.size();
  }

  /**
   * 清空整个过剩冗余块记录表
   */
  synchronized void clear() {
    map.clear();
    size.set(0L);
  }

  /**
   * 判断指定DataNode上的指定块是否存在于过剩冗余表中
   * @param dn DataNode描述符
   * @param blk 块信息
   * @return true 如果该块在该DataNode上是过剩冗余块，否则返回false
   */
  synchronized boolean contains(DatanodeDescriptor dn, BlockInfo blk) {
    final LightWeightHashSet<Block> set = map.get(dn.getDatanodeUuid());
    return set != null && set.contains(blk);
  }

  /**
   * 向记录表中添加一个过剩冗余块，标记该块在指定DataNode上为过剩冗余
   * @param dn 存储该过剩块的DataNode
   * @param blk 过剩块的信息
   * @return true 成功添加，false 该块已经存在，未重复添加
   */
  synchronized boolean add(DatanodeDescriptor dn, BlockInfo blk) {
    LightWeightHashSet<Block> set = map.get(dn.getDatanodeUuid());
    if (set == null) {
      set = new LightWeightHashSet<>();
      map.put(dn.getDatanodeUuid(), set);
    }
    final boolean added = set.add(new ExcessBlockInfo(blk));
    if (added) {
      size.incrementAndGet();
      blockLog.debug("BLOCK* ExcessRedundancyMap.add({}, {})", dn, blk);
    }
    return added;
  }

  /**
   * 从记录表中删除一个过剩冗余块，取消该块在指定DataNode上的过剩冗余标记
   * @param dn 存储该过剩块的DataNode
   * @param blk 需要移除标记的块
   * @return true 成功移除，false 该块不在记录表中，未执行移除
   */
  synchronized boolean remove(DatanodeDescriptor dn, BlockInfo blk) {
    final LightWeightHashSet<Block> set = map.get(dn.getDatanodeUuid());
    if (set == null) {
      return false;
    }
    final boolean removed = set.remove(blk);
    if (removed) {
      size.decrementAndGet();
      blockLog.debug("BLOCK* ExcessRedundancyMap.remove({}, {})", dn, blk);

      if (set.isEmpty()) {
        map.remove(dn.getDatanodeUuid());
      }
    }
    return removed;
  }

  /**
   * 获取整个过剩冗余块映射表，键为DataNode UUID，值为该节点上的过剩块集合
   * @return 完整的过剩冗余块映射表
   */
  synchronized Map<String, LightWeightHashSet<Block>> getExcessRedundancyMap() {
    return map;
  }

  /**
   * 过剩冗余块信息封装，继承Block类，额外记录添加到过剩表的时间戳
   * 用于跟踪过剩块添加时间，支持延迟删除等处理逻辑
   */
  static class ExcessBlockInfo extends Block {
    private long timeStamp;
    private final BlockInfo blockInfo;

    /**
     * 构造过剩冗余块信息对象，初始化时间戳为当前时间
     * @param blockInfo 原始块信息
     */
    ExcessBlockInfo(BlockInfo blockInfo) {
      super(blockInfo.getBlockId(), blockInfo.getNumBytes(), blockInfo.getGenerationStamp());
      this.timeStamp = monotonicNow();
      this.blockInfo = blockInfo;
    }

    /**
     * 获取原始块信息对象
     * @return 原始BlockInfo对象
     */
    public BlockInfo getBlockInfo() {
      return blockInfo;
    }

    /**
     * 获取该块添加到过剩表的时间戳
     * @return 时间戳，基于单调时钟
     */
    long getTimeStamp() {
      return timeStamp;
    }

    /**
     * 更新时间戳为当前时间，用于重置过剩块等待删除的计时
     */
    void setTimeStamp() {
      timeStamp = monotonicNow();
    }

    @Override
    public int hashCode() {
      return super.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
      return super.equals(obj);
    }
  }
}