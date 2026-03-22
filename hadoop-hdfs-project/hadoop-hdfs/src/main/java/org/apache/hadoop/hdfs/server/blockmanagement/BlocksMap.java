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

import java.util.Iterator;
import java.util.concurrent.atomic.LongAdder;

import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.namenode.INodeId;
import org.apache.hadoop.util.GSet;
import org.apache.hadoop.util.LightWeightGSet;

/**
 * @file org/apache/hadoop/hdfs/server/blockmanagement/BlocksMap.java
 * @brief HDFS块信息映射管理器，维护块ID到块元数据的映射关系
 * 
 * 该类是HDFS NameNode端核心数据结构，负责维护所有块的元数据映射，
 * 包括块所属的文件/块集合、存储该块的DataNode信息，
 * 同时统计复制块和EC块组的总数。
 */
/**
 * This class maintains the map from a block to its metadata.
 * block's metadata currently includes blockCollection it belongs to and
 * the datanodes that store the block.
 */
/**
 * @class BlocksMap
 * @brief 块信息映射管理器，维护块到元数据的映射关系
 * 
 * 核心职责：
 * 1. 存储并管理所有HDFS块的元数据（BlockInfo）
 * 2. 提供块的增删查改操作接口
 * 3. 维护复制块和EC块组的计数统计
 * 4. 提供遍历块存储位置的迭代器
 */
class BlocksMap {
  /**
   * @class StorageIterator
   * @brief 迭代块的所有存储信息，跳过数组中的空项
   * 
   * 用于遍历一个块所在的所有Datanode存储信息，
   * 支持纠删码块中存在空槽位的场景，自动跳过null元素。
   */
  public static class StorageIterator implements Iterator<DatanodeStorageInfo> {
    private final BlockInfo blockInfo;
    private int nextIdx = 0;

    StorageIterator(BlockInfo blkInfo) {
      this.blockInfo = blkInfo;
    }

    @Override
    public boolean hasNext() {
      if (blockInfo == null) {
        return false;
      }
      while (nextIdx < blockInfo.getCapacity() &&
          blockInfo.getDatanode(nextIdx) == null) {
        // note that for striped blocks there may be null in the triplets
        nextIdx++;
      }
      return nextIdx < blockInfo.getCapacity();
    }

    @Override
    public DatanodeStorageInfo next() {
      return blockInfo.getStorageInfo(nextIdx++);
    }

    @Override
    public void remove()  {
      throw new UnsupportedOperationException("Sorry. can't remove.");
    }
  }

  /** Constant {@link LightWeightGSet} capacity. */
  private final int capacity;
  
  private GSet<Block, BlockInfo> blocks;

  // 复制块总数统计，使用LongAdder保证高并发下性能
  private final LongAdder totalReplicatedBlocks = new LongAdder();
  // EC块组总数统计，使用LongAdder保证高并发下性能
  private final LongAdder totalECBlockGroups = new LongAdder();

  /**
   * @brief 构造函数，初始化BlocksMap，指定初始容量
   * @param capacity GSet初始容量，根据总内存2%计算得到
   */
  BlocksMap(int capacity) {
    // Use 2% of total memory to size the GSet capacity
    this.capacity = capacity;
    this.blocks = new LightWeightGSet<Block, BlockInfo>(capacity) {
      @Override
      public Iterator<BlockInfo> iterator() {
        SetIterator iterator = new SetIterator();
        /*
         * Not tracking any modifications to set. As this set will be used
         * always under FSNameSystem lock, modifications will not cause any
         * ConcurrentModificationExceptions. But there is a chance of missing
         * newly added elements during iteration.
         */
        // 不跟踪修改，因为总是在FSNameSystem锁下使用，不会产生并发修改异常
        iterator.setTrackModification(false);
        return iterator;
      }
    };
  }


  /**
   * @brief 关闭BlocksMap，释放资源
   */
  void close() {
    clear();
    blocks = null;
  }
  
  /**
   * @brief 清空所有块信息，重置计数统计
   */
  void clear() {
    if (blocks != null) {
      blocks.clear();
      totalReplicatedBlocks.reset();
      totalECBlockGroups.reset();
    }
  }

  /**
   * Add block b belonging to the specified block collection to the map.
   * @brief 将块添加到映射，并关联到指定的块集合（文件）
   * @param b 待添加的块信息
   * @param bc 块所属的块集合（INodeFile）
   * @return 返回添加后的块信息
   */
  BlockInfo addBlockCollection(BlockInfo b, BlockCollection bc) {
    BlockInfo info = blocks.get(b);
    if (info != b) {
      info = b;
      blocks.put(info);
      incrementBlockStat(info);
    }
    info.setBlockCollectionId(bc.getId());
    return info;
  }

  /**
   * Remove the block from the block map;
   * remove it from all data-node lists it belongs to;
   * and remove all data-node locations associated with the block.
   * @brief 完全移除一个块，清理所有关联引用
   * @param block 待移除的块信息
   */
  void removeBlock(BlockInfo block) {
    BlockInfo blockInfo = blocks.remove(block);
    if (blockInfo == null) {
      return;
    }
    // 更新块计数
    decrementBlockStat(block);

    // 块必须已经从文件中删除，才能被完全移除
    assert blockInfo.getBlockCollectionId() == INodeId.INVALID_INODE_ID;
    // 根据块类型遍历所有存储位置
    final int size = blockInfo.isStriped() ?
        blockInfo.getCapacity() : blockInfo.numNodes();
    // 倒序遍历，避免空槽位问题
    for(int idx = size - 1; idx >= 0; idx--) {
      DatanodeDescriptor dn = blockInfo.getDatanode(idx);
      if (dn != null) {
        removeBlock(dn, blockInfo); // 从DataNode的块列表中移除本块，清空关联位置
      }
    }
  }

  /**
   * @brief 根据块标识查询存储的块信息
   * @param b 块标识
   * @return 如果存在返回块元数据，否则返回null
   */
  BlockInfo getStoredBlock(Block b) {
    return blocks.get(b);
  }

  /**
   * Searches for the block in the BlocksMap and 
   * returns {@link Iterable} of the storages the block belongs to.
   * @brief 获取块所在的所有存储信息的可迭代对象
   * @param b 待查询的块标识
   * @return 存储信息可迭代对象
   */
  Iterable<DatanodeStorageInfo> getStorages(Block b) {
    return getStorages(blocks.get(b));
  }

  /**
   * For a block that has already been retrieved from the BlocksMap
   * returns {@link Iterable} of the storages the block belongs to.
   * @brief 基于已获取的块信息，返回存储信息的可迭代对象
   * @param storedBlock 已从BlocksMap获取的块信息
   * @return 存储信息可迭代对象
   */
  Iterable<DatanodeStorageInfo> getStorages(final BlockInfo storedBlock) {
    return new Iterable<DatanodeStorageInfo>() {
      @Override
      public Iterator<DatanodeStorageInfo> iterator() {
        return new StorageIterator(storedBlock);
      }
    };
  }

  /** counts number of containing nodes. Better than using iterator.
   * @brief 获取块所在的DataNode数量，比迭代效率更高
   * @param b 待查询的块标识
   * @return 存储该块的DataNode数量
   */
  int numNodes(Block b) {
    BlockInfo info = blocks.get(b);
    return info == null ? 0 : info.numNodes();
  }

  /**
   * Remove data-node reference from the block.
   * Remove the block from the block map
   * only if it does not belong to any file and data-nodes.
   * @brief 从块中移除指定DataNode的引用，如果块不再属于任何文件且无存储，则删除该块
   * @param b 待处理的块标识
   * @param node 要移除的DataNode
   * @return 是否成功移除引用
   */
  boolean removeNode(Block b, DatanodeDescriptor node) {
    BlockInfo info = blocks.get(b);
    if (info == null)
      return false;

    // 从DataNode列表移除块，从块信息移除DataNode引用
    boolean removed = removeBlock(node, info);

    if (info.hasNoStorage()    // 已经没有任何存储节点
        && info.isDeleted()) { // 块已经不属于任何文件
      blocks.remove(b);  // 从映射中删除该块
      decrementBlockStat(info);
    }
    return removed;
  }

  /**
   * Remove block from the list of blocks belonging to the data-node. Remove
   * data-node from the block.
   * @brief 静态工具方法，从DataNode中移除块引用
   * @param dn DataNode描述符
   * @param b 待移除的块信息
   * @return 是否成功移除
   */
  static boolean removeBlock(DatanodeDescriptor dn, BlockInfo b) {
    final DatanodeStorageInfo s = b.findStorageInfo(dn);
    // 如果块存在于此DataNode上，则移除
    return s != null && s.removeBlock(b);
  }

  /**
   * @brief 获取当前映射中块的总数量
   * @return 块总数
   */
  int size() {
    if (blocks != null) {
      return blocks.size();
    } else {
      return 0;
    }
  }

  /**
   * @brief 获取所有块信息的可迭代对象，用于遍历
   * @return 所有块信息的可迭代对象
   */
  Iterable<BlockInfo> getBlocks() {
    return blocks;
  }
  
  /** Get the capacity of the HashMap that stores blocks */
  /**
   * @brief 获取底层存储GSet的容量
   * @return GSet容量
   */
  int getCapacity() {
    return capacity;
  }

  /**
   * @brief 根据块类型增加对应计数统计
   * @param block 新增的块
   */
  private void incrementBlockStat(BlockInfo block) {
    if (block.isStriped()) {
      totalECBlockGroups.increment();
    } else {
      totalReplicatedBlocks.increment();
    }
  }

  /**
   * @brief 根据块类型减少对应计数统计
   * @param block 移除的块
   */
  private void decrementBlockStat(BlockInfo block) {
    if (block.isStriped()) {
      totalECBlockGroups.decrement();
      assert totalECBlockGroups.longValue() >= 0 :
          "Total number of ec block groups should be non-negative";
    } else {
      totalReplicatedBlocks.decrement();
      assert totalReplicatedBlocks.longValue() >= 0 :
          "Total number of replicated blocks should be non-negative";
    }
  }

  /**
   * @brief 获取当前复制块总数
   * @return 复制块总数
   */
  long getReplicatedBlocks() {
    return totalReplicatedBlocks.longValue();
  }

  /**
   * @brief 获取当前EC块组总数
   * @return EC块组总数
   */
  long getECBlockGroups() {
    return totalECBlockGroups.longValue();
  }
}