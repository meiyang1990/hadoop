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
package org.apache.hadoop.hdfs.server.datanode.fsdataset.impl;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.common.AutoCloseDataSetLock;
import org.apache.hadoop.hdfs.server.common.DataNodeLockManager;
import org.apache.hadoop.hdfs.server.common.DataNodeLockManager.LockLevel;
import org.apache.hadoop.hdfs.server.common.NoLockManager;
import org.apache.hadoop.hdfs.server.datanode.ReplicaInfo;
import org.apache.hadoop.util.LightWeightResizableGSet;

/**
 * 文件级注释：维护DataNode节点上所有数据块副本的元数据映射表，
 * 按块池分组存储所有副本信息，提供线程安全的增删改查操作，
 * 是DataNode管理本地存储副本的核心数据结构。
 * <br>
 * 核心职责：按块池ID -> 块ID -> 副本元数据的层级结构存储副本信息，
 * 支持并发访问控制，服务于DataNode的块副本管理流程。
 */
class ReplicaMap {
  // 用于同步当前实例读写操作的锁管理器
  private DataNodeLockManager<AutoCloseDataSetLock> lockManager;

  // 顶层映射：key为块池ID，value为该块池下所有块的轻量级GSet，存储块ID到副本元数据的映射
  private final Map<String, LightWeightResizableGSet<Block, ReplicaInfo>> map =
      new ConcurrentHashMap<>();

  /**
   * 构造函数，使用指定的锁管理器创建副本映射表，用于生产环境的正式使用场景。
   * @param manager 数据节点锁管理器，用于控制并发访问
   */
  ReplicaMap(DataNodeLockManager<AutoCloseDataSetLock> manager) {
    if (manager == null) {
      throw new HadoopIllegalArgumentException(
          "Object to synchronize on cannot be null");
    }
    this.lockManager = manager;
  }

  /**
   * 无参构造函数，创建不需要锁保护的副本映射表，仅用于单元测试和临时场景。
   */
  ReplicaMap() {
    this.lockManager = new NoLockManager();
  }
  
  /**
   * 获取当前映射表中所有已注册块池的ID列表。
   * @return 所有块池ID构成的数组
   */
  String[] getBlockPoolList() {
    Set<String> bpset = map.keySet();
    return bpset.toArray(new String[bpset.size()]);
  }
  
  /**
   * 参数校验：检查块池ID是否为空，为空则抛出非法参数异常。
   * @param bpid 待检查的块池ID
   */
  private void checkBlockPool(String bpid) {
    if (bpid == null) {
      throw new IllegalArgumentException("Block Pool Id is null");
    }
  }
  
  /**
   * 参数校验：检查块对象是否为空，为空则抛出非法参数异常。
   * @param b 待检查的块对象
   */
  private void checkBlock(Block b) {
    if (b == null) {
      throw new IllegalArgumentException("Block is null");
    }
  }
  
  /**
   * 根据块池ID和块对象获取匹配块ID且生成时间戳一致的副本元数据。
   * @param bpid 块池ID
   * @param block 目标块对象，包含块ID和生成时间戳
   * @return 匹配成功返回副本元数据，匹配失败返回null
   * @throws IllegalArgumentException 输入块或块池为空时抛出
   */
  ReplicaInfo get(String bpid, Block block) {
    checkBlockPool(bpid);
    checkBlock(block);
    ReplicaInfo replicaInfo = get(bpid, block.getBlockId());
    if (replicaInfo != null && 
        block.getGenerationStamp() == replicaInfo.getGenerationStamp()) {
      return replicaInfo;
    }
    return null;
  }
  
  
  /**
   * 根据块池ID和块ID获取副本元数据，不校验生成时间戳。
   * @param bpid 块池ID
   * @param blockId 目标块ID
   * @return 对应的副本元数据，不存在则返回null
   */
  ReplicaInfo get(String bpid, long blockId) {
    checkBlockPool(bpid);
    try (AutoCloseDataSetLock l = lockManager.readLock(LockLevel.BLOCK_POOl, bpid)) {
      LightWeightResizableGSet<Block, ReplicaInfo> m = map.get(bpid);
      return m != null ? m.get(new Block(blockId)) : null;
    }
  }

  /**
   * 向指定块池添加一个副本元数据。
   * @param bpid 块池ID
   * @param replicaInfo 待添加的副本元数据
   * @return 若该块已存在副本，返回旧的副本元数据；否则返回null
   * @throws IllegalArgumentException 输入参数为空时抛出
   */
  ReplicaInfo add(String bpid, ReplicaInfo replicaInfo) {
    checkBlockPool(bpid);
    checkBlock(replicaInfo);
    try (AutoCloseDataSetLock l = lockManager.readLock(LockLevel.BLOCK_POOl, bpid)) {
      LightWeightResizableGSet<Block, ReplicaInfo> m = map.get(bpid);
      if (m == null) {
        // 块池不存在则创建新的GSet并放入顶层映射
        map.putIfAbsent(bpid, new LightWeightResizableGSet<Block, ReplicaInfo>());
        m = map.get(bpid);
      }
      return  m.put(replicaInfo);
    }
  }

  /**
   * 向指定块池添加副本元数据，若已存在同块副本则返回已有副本，不存在则添加并返回新副本。
   * @param bpid 块池ID
   * @param replicaInfo 待添加的副本元数据
   * @return 已存在则返回旧副本，不存在则添加后返回新副本
   */
  ReplicaInfo addAndGet(String bpid, ReplicaInfo replicaInfo) {
    checkBlockPool(bpid);
    checkBlock(replicaInfo);
    try (AutoCloseDataSetLock l = lockManager.readLock(LockLevel.BLOCK_POOl, bpid)) {
      LightWeightResizableGSet<Block, ReplicaInfo> m = map.get(bpid);
      if (m == null) {
        // 块池不存在则创建新的GSet并放入顶层映射
        map.putIfAbsent(bpid, new LightWeightResizableGSet<Block, ReplicaInfo>());
        m = map.get(bpid);
      }
      ReplicaInfo oldReplicaInfo = m.get(replicaInfo);
      if (oldReplicaInfo != null) {
        return oldReplicaInfo;
      } else {
        m.put(replicaInfo);
      }
      return replicaInfo;
    }
  }

  /**
   * 将另一个副本映射表的所有块池数据批量添加到当前映射表，直接覆盖同名块池。
   * @param other 待合并的源副本映射表
   */
  void addAll(ReplicaMap other) {
    map.putAll(other.map);
  }


  /**
   * 将另一个副本映射表的所有副本信息合并到当前映射表，同名块池进行增量合并。
   * @param other 待合并的源副本映射表
   */
  void mergeAll(ReplicaMap other) {
    Set<String> bplist = other.map.keySet();
    // 遍历所有待合并的块池
    for (String bp : bplist) {
      checkBlockPool(bp);
      try (AutoCloseDataSetLock l = lockManager.writeLock(LockLevel.BLOCK_POOl, bp)) {
        LightWeightResizableGSet<Block, ReplicaInfo> replicaInfos = other.map.get(bp);
        LightWeightResizableGSet<Block, ReplicaInfo> curSet = map.get(bp);
        HashSet<ReplicaInfo> replicaSet = new HashSet<>();
        // 先将所有待添加副本缓存到集合，避免遍历原GSet时修改导致死循环
        for (ReplicaInfo replicaInfo : replicaInfos) {
          replicaSet.add(replicaInfo);
        }
        if (curSet == null && !replicaSet.isEmpty()) {
          // 当前块池不存在则创建新GSet
          curSet = new LightWeightResizableGSet<>();
          map.put(bp, curSet);
        }
        // 将缓存的副本逐个添加到当前块池
        for (ReplicaInfo replicaInfo : replicaSet) {
          checkBlock(replicaInfo);
          curSet.put(replicaInfo);
        }
      }
    }
  }
  
  /**
   * 根据块池ID和块对象删除匹配块ID且生成时间戳一致的副本。
   * @param bpid 块池ID
   * @param block 目标块对象，包含块ID和生成时间戳
   * @return 被删除的副本元数据，不存在则返回null
   * @throws IllegalArgumentException 输入块为空时抛出
   */
  ReplicaInfo remove(String bpid, Block block) {
    checkBlockPool(bpid);
    checkBlock(block);
    try (AutoCloseDataSetLock l = lockManager.readLock(LockLevel.BLOCK_POOl, bpid)) {
      LightWeightResizableGSet<Block, ReplicaInfo> m = map.get(bpid);
      if (m != null) {
        ReplicaInfo replicaInfo = m.get(block);
        if (replicaInfo != null &&
            block.getGenerationStamp() == replicaInfo.getGenerationStamp()) {
          return m.remove(block);
        }
      }
    }
    
    return null;
  }
  
  /**
   * 根据块池ID和块ID删除对应的副本，不校验生成时间戳。
   * @param bpid 块池ID
   * @param blockId 目标块ID
   * @return 被删除的副本元数据，不存在则返回null
   */
  ReplicaInfo remove(String bpid, long blockId) {
    checkBlockPool(bpid);
    try (AutoCloseDataSetLock l = lockManager.readLock(LockLevel.BLOCK_POOl, bpid)) {
      LightWeightResizableGSet<Block, ReplicaInfo> m = map.get(bpid);
      if (m != null) {
        return m.remove(new Block(blockId));
      }
    }
    return null;
  }
 
  /**
   * 获取指定块池下的副本总数量。
   * @param bpid 块池ID
   * @return 该块池下的副本数量，块池不存在则返回0
   */
  int size(String bpid) {
    try (AutoCloseDataSetLock l = lockManager.readLock(LockLevel.BLOCK_POOl, bpid)) {
      LightWeightResizableGSet<Block, ReplicaInfo> m = map.get(bpid);
      return m != null ? m.size() : 0;
    }
  }
  
  /**
   * 获取指定块池下所有副本的集合，该方法不同步，非线程安全。
   * 线程安全场景请使用{@link #replicas(String, Consumer<Iterator<ReplicaInfo>>)}。
   * @param bpid 块池ID
   * @return 该块池下所有副本的集合，块池不存在则返回null
   */
  Collection<ReplicaInfo> replicas(String bpid) {
    LightWeightResizableGSet<Block, ReplicaInfo> m = null;
    m = map.get(bpid);
    return m != null ? m.values() : null;
  }

  /**
   * 在锁保护下对指定块池的所有副本执行消费者处理逻辑，该方法线程安全。
   * @param bpid 块池ID
   * @param consumer 对副本迭代器执行处理的消费者函数
   */
  void replicas(String bpid, Consumer<Iterator<ReplicaInfo>> consumer) {
    LightWeightResizableGSet<Block, ReplicaInfo> m = null;
    try (AutoCloseDataSetLock l = lockManager.readLock(LockLevel.BLOCK_POOl, bpid)) {
      m = map.get(bpid);
      if (m !=null) {
        m.getIterator(consumer);
      }
    }
  }

  /**
   * 初始化指定块池的存储结构，若块池已存在则不做操作。
   * @param bpid 待初始化的块池ID
   */
  void initBlockPool(String bpid) {
    checkBlockPool(bpid);
    try (AutoCloseDataSetLock l = lockManager.writeLock(LockLevel.BLOCK_POOl, bpid)) {
      LightWeightResizableGSet<Block, ReplicaInfo> m = map.get(bpid);
      if (m == null) {
        // 块池不存在则创建新的GSet
        m = new LightWeightResizableGSet<Block, ReplicaInfo>();
        map.put(bpid, m);
      }
    }
  }
  
  /**
   * 清理指定块池的所有数据，从映射表中移除该块池。
   * @param bpid 待清理的块池ID
   */
  void cleanUpBlockPool(String bpid) {
    checkBlockPool(bpid);
    try (AutoCloseDataSetLock l = lockManager.writeLock(LockLevel.BLOCK_POOl, bpid)) {
      map.remove(bpid);
    }
  }
}