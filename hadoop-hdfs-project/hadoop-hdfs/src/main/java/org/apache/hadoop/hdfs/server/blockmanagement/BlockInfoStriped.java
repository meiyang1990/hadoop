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

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.util.Preconditions;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockType;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.BlockUCState;
import org.apache.hadoop.hdfs.util.StripedBlockUtil;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * 文件: BlockInfoStriped.java
 * 描述: HDFS纠删码块组信息类，继承自BlockInfo，用于存储和管理纠删码条带化块组的元数据
 *
 * 纠删码块组由m个数据块和k个校验块组成，本类负责管理整个块组的位置信息和索引关系
 * 支持超冗余复制场景，可动态扩展存储位置数组
 */
@InterfaceAudience.Private
public class BlockInfoStriped extends BlockInfo {
  /** 该块组使用的纠删码编码策略，包含数据块数、校验块数、单元大小等参数 */
  private final ErasureCodingPolicy ecPolicy;
  /**
   * 存储每个triplet对应的块索引，长度与triplets数组一致
   * TODO: 仅超冗余块需要该索引，可进一步优化内存占用
   */
  private byte[] indices;

  /**
   * 构造一个纠删码块组的BlockInfo对象
   * @param blk 纠删码块组的基础块信息
   * @param ecPolicy 使用的纠删码编码策略
   */
  public BlockInfoStriped(Block blk, ErasureCodingPolicy ecPolicy) {
    super(blk, (short) (ecPolicy.getNumDataUnits() + ecPolicy.getNumParityUnits()));
    indices = new byte[ecPolicy.getNumDataUnits() + ecPolicy.getNumParityUnits()];
    initIndices();
    this.ecPolicy = ecPolicy;
  }

  /**
   * 获取该纠删码块组的总块数（数据块+校验块）
   * @return 总块数
   */
  public short getTotalBlockNum() {
    return (short) (ecPolicy.getNumDataUnits() + ecPolicy.getNumParityUnits());
  }

  /**
   * 获取该纠删码块组的数据块数量
   * @return 数据块数量
   */
  public short getDataBlockNum() {
    return (short) ecPolicy.getNumDataUnits();
  }

  /**
   * 获取该纠删码块组的校验块数量
   * @return 校验块数量
   */
  public short getParityBlockNum() {
    return (short) ecPolicy.getNumParityUnits();
  }

  /**
   * 获取该纠删码策略的条带单元大小
   * @return 单元大小，单位字节
   */
  public int getCellSize() {
    return ecPolicy.getCellSize();
  }

  /**
   * 获取实际需要存储的数据块数量
   * 如果块已完成提交且总数据小于一个完整条带，返回实际占用的数据块数；否则返回策略定义的数据块数
   * @return 实际数据块数量
   */
  public short getRealDataBlockNum() {
    if (isComplete() || getBlockUCState() == BlockUCState.COMMITTED) {
      return (short) Math.min(getDataBlockNum(),
          (getNumBytes() - 1) / ecPolicy.getCellSize() + 1);
    } else {
      return getDataBlockNum();
    }
  }

  /**
   * 获取实际需要存储的总块数（实际数据块+校验块）
   * @return 实际总块数
   */
  public short getRealTotalBlockNum() {
    return (short) (getRealDataBlockNum() + getParityBlockNum());
  }

  /**
   * 获取该块组使用的纠删码策略
   * @return 纠删码编码策略对象
   */
  public ErasureCodingPolicy getErasureCodingPolicy() {
    return ecPolicy;
  }

  /**
   * 初始化所有块索引为-1，表示未分配
   */
  private void initIndices() {
    for (int i = 0; i < indices.length; i++) {
      indices[i] = -1;
    }
  }

  /**
   * 查找一个空闲的存储槽位用于新增超冗余副本
   * @return 空闲槽位索引，如果容量不足则扩容后返回新索引
   */
  private int findSlot() {
    int i = getTotalBlockNum();
    int capacity = getCapacity();
    for (; i < capacity; i++) {
      if (getStorageInfo(i) == null) {
        return i;
      }
    }
    // 需要扩容triplet数组
    ensureCapacity(i + 1, true);
    return i;
  }

  @Override
  /**
   * 向纠删码块组添加数据节点存储信息，处理超冗余副本场景
   * @param storage 目标数据节点存储信息
   * @param reportedBlock 数据节点上报的块信息
   * @return 添加成功始终返回true
   */
  boolean addStorage(DatanodeStorageInfo storage, Block reportedBlock) {
    // 校验上报块确实是纠删码块
    Preconditions.checkArgument(BlockIdManager.isStripedBlockID(
        reportedBlock.getBlockId()), "reportedBlock is not striped");
    // 校验上报块确实属于当前块组
    Preconditions.checkArgument(BlockIdManager.convertToStripedID(
        reportedBlock.getBlockId()) == this.getBlockId(),
        "reported blk_%s does not belong to the group of stored blk_%s",
        reportedBlock.getBlockId(), this.getBlockId());
    // 获取当前块在块组中的索引
    int blockIndex = BlockIdManager.getBlockIndex(reportedBlock);
    int index = blockIndex;
    DatanodeStorageInfo old = getStorageInfo(index);
    if (old != null && !old.equals(storage)) { // 当前位置已有存储且不是同一个节点，属于超冗余场景
      // 检查该存储是否已经添加过
      int i = findStorageInfo(storage);
      if (i == -1) {
        // 未添加过，查找空闲槽位
        index = findSlot();
      } else {
        // 已添加过，直接返回
        return true;
      }
    }
    // 将存储信息添加到对应槽位，并记录块索引
    addStorage(storage, index, blockIndex);
    return true;
  }

  /**
   * 将数据节点存储信息添加到指定槽位，记录对应的块索引
   * @param storage 数据节点存储信息
   * @param index 槽位索引
   * @param blockIndex 块在块组中的索引
   */
  private void addStorage(DatanodeStorageInfo storage, int index,
      int blockIndex) {
    setStorageInfo(index, storage);
    setNext(index, null);
    setPrevious(index, null);
    indices[index] = (byte) blockIndex;
  }

  /**
   * 从数组末尾向前查找指定存储信息的索引
   * @param storage 目标数据节点存储信息
   * @return 找到返回索引，否则返回-1
   */
  private int findStorageInfoFromEnd(DatanodeStorageInfo storage) {
    final int len = getCapacity();
    for(int idx = len - 1; idx >= 0; idx--) {
      DatanodeStorageInfo cur = getStorageInfo(idx);
      if (storage.equals(cur)) {
        return idx;
      }
    }
    return -1;
  }

  @VisibleForTesting
  /**
   * 获取指定存储对应的块在块组中的索引，仅用于测试
   * @param storage 数据节点存储信息
   * @return 块索引，未找到返回-1
   */
  public byte getStorageBlockIndex(DatanodeStorageInfo storage) {
    int i = this.findStorageInfo(storage);
    return i == -1 ? -1 : indices[i];
  }

  /**
   * 获取指定存储上存储的块对象，生成对应子块ID
   * @param storage 数据节点存储信息
   * @return 对应块对象，未找到返回null
   */
  Block getBlockOnStorage(DatanodeStorageInfo storage) {
    int index = getStorageBlockIndex(storage);
    if (index < 0) {
      return null;
    } else {
      Block block = new Block(this);
      // 子块ID = 组ID + 块索引
      block.setBlockId(this.getBlockId() + index);
      return block;
    }
  }

  @Override
  /**
   * 从纠删码块组中移除指定数据节点存储信息
   * @param storage 要移除的数据节点存储
   * @return 移除成功返回true，未找到返回false
   */
  boolean removeStorage(DatanodeStorageInfo storage) {
    int dnIndex = findStorageInfoFromEnd(storage);
    if (dnIndex < 0) { // 未找到该节点
      return false;
    }
    assert getPrevious(dnIndex) == null && getNext(dnIndex) == null :
        "Block is still in the list and must be removed first.";
    // 清空对应槽位信息
    setStorageInfo(dnIndex, null);
    setNext(dnIndex, null);
    setPrevious(dnIndex, null);
    indices[dnIndex] = -1;
    return true;
  }

  /**
   * 确保triplets数组容量满足要求，不够则扩容并复制原有数据
   * @param totalSize 需要的总容量
   * @param keepOld 是否保留原有数据
   */
  private void ensureCapacity(int totalSize, boolean keepOld) {
    if (getCapacity() < totalSize) {
      Object[] old = triplets;
      byte[] oldIndices = indices;
      triplets = new Object[totalSize * 3];
      indices = new byte[totalSize];
      initIndices();

      if (keepOld) {
        System.arraycopy(old, 0, triplets, 0, old.length);
        System.arraycopy(oldIndices, 0, indices, 0, oldIndices.length);
      }
    }
  }

  /**
   * 计算该纠删码块组占用的总存储空间大小
   * @return 总占用空间，单位字节
   */
  public long spaceConsumed() {
    // 纠删码块总占用为所有数据块和校验块实际占用的总和
    // getNumBytes已经返回了实际数据块的总大小
    return StripedBlockUtil.spaceConsumedByStripedBlock(getNumBytes(),
        ecPolicy.getNumDataUnits(), ecPolicy.getNumParityUnits(),
        ecPolicy.getCellSize());
    }

  @Override
  /**
   * 判断是否为条带化纠删码块
   * @return 始终返回true
   */
  public final boolean isStriped() {
    return true;
  }

  @Override
  /**
   * 获取块类型
   * @return 返回STRIPED类型
   */
  public BlockType getBlockType() {
    return BlockType.STRIPED;
  }

  @Override
  /**
   * 统计存储该块组的总数据节点数量
   * @return 数据节点总数
   */
  public int numNodes() {
    assert this.triplets != null : "BlockInfo is not initialized";
    assert triplets.length % 3 == 0 : "Malformed BlockInfo";
    int num = 0;
    // 遍历所有槽位统计非空存储节点
    for (int idx = getCapacity()-1; idx >= 0; idx--) {
      if (getStorageInfo(idx) != null) {
        num++;
      }
    }
    return num;
  }

  @Override
  /**
   * 检查该块组是否没有任何存储节点
   * @return 没有任何存储返回true，否则返回false
   */
  final boolean hasNoStorage() {
    final int len = getCapacity();
    for(int idx = 0; idx < len; idx++) {
      if (getStorageInfo(idx) != null) {
        return false;
      }
    }
    return true;
  }

  /**
   * 纠删码块不支持在提供存储上使用，提供存储上的块都假定为连续块
   * @return 始终返回false
   */
  @Override
  boolean isProvided() {
    return false;
  }

  /**
   * 存储数据节点存储信息和对应块索引的容器类
   * 用于遍历块组时返回存储与索引的配对信息
   */
  public static class StorageAndBlockIndex {
    private final DatanodeStorageInfo storage;
    private final byte blockIndex;

    StorageAndBlockIndex(DatanodeStorageInfo storage, byte blockIndex) {
      this.storage = storage;
      this.blockIndex = blockIndex;
    }

    /**
     * 获取数据节点存储信息
     * @return 数据节点存储对象
     */
    public DatanodeStorageInfo getStorage() {
      return storage;
    }

    /**
     * 获取块在块组中的索引
     * @return 块索引
     */
    public byte getBlockIndex() {
      return blockIndex;
    }
  }

  /**
   * 获取可迭代对象，用于遍历块组中所有非空存储及其对应块索引
   * @return 可迭代对象，遍历返回StorageAndBlockIndex实例
   */
  public Iterable<StorageAndBlockIndex> getStorageAndIndexInfos() {
    return new Iterable<StorageAndBlockIndex>() {
      @Override
      public Iterator<StorageAndBlockIndex> iterator() {
        return new Iterator<StorageAndBlockIndex>() {
          private int index = 0;

          @Override
          public boolean hasNext() {
            // 跳过空槽位
            while (index < getCapacity() && getStorageInfo(index) == null) {
              index++;
            }
            return index < getCapacity();
          }

          @Override
          public StorageAndBlockIndex next() {
            if (!hasNext()) {
              throw new NoSuchElementException();
            }
            int i = index++;
            return new StorageAndBlockIndex(
                (DatanodeStorageInfo) triplets[i * 3], indices[i]);
          }

          @Override
          public void remove() {
            throw new UnsupportedOperationException("Remove is not supported");
          }
        };
      }
    };
  }
}