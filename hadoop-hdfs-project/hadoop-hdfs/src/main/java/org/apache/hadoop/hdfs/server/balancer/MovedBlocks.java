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
package org.apache.hadoop.hdfs.server.balancer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.util.Time;

/**
 * HDFS平衡器已移动数据块跟踪窗口实现，使用双滑动窗口机制，仅保留固定时间窗口内（默认1.5小时）的已移动块记录，
 * 自动清理超期的旧记录，避免内存无限增长。分为旧窗口存储较早移动的块、当前窗口存储近期移动的块，
 * 清理时检查旧窗口是否超期，超期则清空旧窗口并将当前窗口转为旧窗口，新建空的当前窗口。
 * 
 * @param <L> 位置类型，存储数据块所在节点位置信息
 */
public class MovedBlocks<L> {
  /**
   * 存储数据块及其所有副本位置信息的容器类
   * @param <L> 位置类型
   */
  public static class Locations<L> {
    private final Block block; // 数据块对象
    /** 该数据块所有副本的位置列表 */
    protected final List<L> locations = new ArrayList<L>(3);
    
    /**
     * 构造方法，绑定对应数据块
     * @param block 需要跟踪的数据块
     */
    public Locations(Block block) {
      this.block = block;
    }
    
    /** 清空所有位置信息 */
    public synchronized void clearLocations() {
      locations.clear();
    }
    
    /**
     * 添加一个数据块副本位置，去重处理
     * @param loc 需要添加的位置
     */
    public synchronized void addLocation(L loc) {
      if (!locations.contains(loc)) {
        locations.add(loc);
      }
    }
    
    /**
     * 检查该数据块是否存在指定位置的副本
     * @param loc 需要检查的位置
     * @return 若存在返回true，否则返回false
     */
    public synchronized boolean isLocatedOn(L loc) {
      return locations.contains(loc);
    }
    
    /**
     * 获取该数据块所有副本位置列表
     * @return 所有位置的不可修改列表
     */
    public synchronized List<L> getLocations() {
      return locations;
    }
    
    /**
     * 获取当前跟踪的数据块对象
     * @return 数据块对象
     */
    public Block getBlock() {
      return block;
    }
    
    /**
     * 获取数据块大小（字节数）
     * @return 数据块字节数
     */
    public long getNumBytes() {
      return block.getNumBytes();
    }

    @Override
    public String toString() {
      return block + " size=" + getNumBytes();
    }
  }

  private static final int CUR_WIN = 0;
  private static final int OLD_WIN = 1;
  private static final int NUM_WINS = 2;

  /** 滑动窗口时间间隔，超过该间隔的已移动块记录会被清理 */
  private final long winTimeInterval;
  /** 上一次清理操作的时间戳，使用单调递增时间 */
  private long lastCleanupTime = Time.monotonicNow();
  /** 双窗口存储已移动块，索引0为当前窗口，索引1为旧窗口 */
  private final List<Map<Block, Locations<L>>> movedBlocks
      = new ArrayList<Map<Block, Locations<L>>>(NUM_WINS);
  
  /**
   * 构造已移动块跟踪容器，初始化双窗口
   * @param winTimeInterval 滑动窗口时间间隔（毫秒），超过该间隔的记录会被清理
   */
  public MovedBlocks(long winTimeInterval) {
    this.winTimeInterval = winTimeInterval;
    movedBlocks.add(newMap());
    movedBlocks.add(newMap());
  }

  /**
   * 创建空的块位置映射表
   * @return 新建的空HashMap
   */
  private Map<Block, Locations<L>> newMap() {
    return new HashMap<Block, Locations<L>>();
  }

  /**
   * 将一个已移动块添加到跟踪容器中标记为已移动
   * @param block 已移动块的位置信息对象
   */
  public synchronized void put(Locations<L> block) {
    movedBlocks.get(CUR_WIN).put(block.getBlock(), block);
  }

  /**
   * 检查指定块是否已被移动（存在于任意一个窗口中）
   * @param block 需要检查的数据块
   * @return 若块已被移动返回true，否则返回false
   */
  public synchronized boolean contains(Block block) {
    return movedBlocks.get(CUR_WIN).containsKey(block) ||
      movedBlocks.get(OLD_WIN).containsKey(block);
  }

  /**
   * 清理超期的旧已移动块记录，滑动窗口推进
   */
  public synchronized void cleanup() {
    // 获取当前单调时间
    long curTime = Time.monotonicNow();
    // 检查距离上次清理是否已经超过窗口间隔
    if (lastCleanupTime + winTimeInterval <= curTime) {
      // 将当前窗口转为旧窗口，清空原旧窗口
      movedBlocks.set(OLD_WIN, movedBlocks.get(CUR_WIN));
      // 创建新的空当前窗口
      movedBlocks.set(CUR_WIN, newMap());
      // 更新最后清理时间
      lastCleanupTime = curTime;
    }
  }
}