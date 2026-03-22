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
package org.apache.hadoop.hdfs.server.protocol;

import org.apache.hadoop.util.Preconditions;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.protocol.Block;

/**
 * HDFS服务器端协议中，存储多个数据块及其位置信息的容器类
 * 用于在NameNode和DataNode之间传输块位置信息，包含多个块的位置集合
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class BlocksWithLocations {

  /**
   * 维护单个数据块及其所有存储位置信息的内部类
   * 存储块本身、所在DataNode、存储ID和存储类型等位置相关信息
   */
  @InterfaceAudience.Private
  @InterfaceStability.Evolving
  public static class BlockWithLocations {
    final Block block;
    final String[] datanodeUuids;
    final String[] storageIDs;
    final StorageType[] storageTypes;
    
    /**
     * 构造单个块位置信息对象
     * @param block 数据块对象
     * @param datanodeUuids 存储该块的所有DataNode的UUID数组
     * @param storageIDs 存储该块的所有存储ID数组
     * @param storageTypes 存储该块的所有存储类型数组
     */
    public BlockWithLocations(Block block, String[] datanodeUuids,
        String[] storageIDs, StorageType[] storageTypes) {
      this.block = block;
      this.datanodeUuids = datanodeUuids;
      this.storageIDs = storageIDs;
      this.storageTypes = storageTypes;
    }
    
    /**
     * 获取该位置信息对应的数据块
     * @return 数据块对象
     */
    public Block getBlock() {
      return block;
    }
    
    /**
     * 获取存储该块的所有DataNode的UUID数组
     * @return DataNode UUID数组
     */
    public String[] getDatanodeUuids() {
      return datanodeUuids;
    }

    /**
     * 获取存储该块的所有存储ID数组
     * @return 存储ID数组
     */
    public String[] getStorageIDs() {
      return storageIDs;
    }

    /**
     * 获取存储该块的所有存储类型数组
     * @return 存储类型数组
     */
    public StorageType[] getStorageTypes() {
      return storageTypes;
    }

    @Override
    public String toString() {
      final StringBuilder b = new StringBuilder();
      b.append(block);
      if (datanodeUuids.length == 0) {
        return b.append("[]").toString();
      }
      
      appendString(0, b.append("["));
      for(int i = 1; i < datanodeUuids.length; i++) {
        appendString(i, b.append(","));
      }
      return b.append("]").toString();
    }
    
    private StringBuilder appendString(int i, StringBuilder b) {
      return b.append("[").append(storageTypes[i]).append("]")
              .append(storageIDs[i])
              .append("@").append(datanodeUuids[i]);
    }
  }

  /**
   * 纠删码条纹块的位置信息类，继承普通块位置信息，扩展纠删码相关属性
   * 用于存储纠删码组中内部块索引、数据块数量和单元大小等纠删码特有信息
   */
  public static class StripedBlockWithLocations extends BlockWithLocations {
    final byte[] indices;
    final short dataBlockNum;
    final int cellSize;

    /**
     * 构造纠删码条纹块位置信息对象
     * @param blk 普通块位置信息基础对象
     * @param indices 纠删码组内各个块的索引数组
     * @param dataBlockNum 纠删码组中数据块的数量
     * @param cellSize 纠删码单元大小
     */
    public StripedBlockWithLocations(BlockWithLocations blk, byte[] indices,
         short dataBlockNum, int cellSize) {
      super(blk.getBlock(), blk.getDatanodeUuids(), blk.getStorageIDs(),
          blk.getStorageTypes());
      // 校验DataNode数量和块索引数量一致
      Preconditions.checkArgument(
          blk.getDatanodeUuids().length == indices.length);
      this.indices = indices;
      this.dataBlockNum = dataBlockNum;
      this.cellSize = cellSize;
    }

    /**
     * 获取纠删码组内各个块的索引数组
     * @return 块索引数组
     */
    public byte[] getIndices() {
      return indices;
    }

    /**
     * 获取纠删码组中数据块的数量
     * @return 数据块数量
     */
    public short getDataBlockNum() {
      return dataBlockNum;
    }

    /**
     * 获取纠删码单元大小
     * @return 纠删码单元大小（字节）
     */
    public int getCellSize() {
      return cellSize;
    }
  }

  private final BlockWithLocations[] blocks;

  /**
   * 构造多个块位置信息容器
   * @param blocks 块位置信息数组
   */
  public BlocksWithLocations(BlockWithLocations[] blocks) {
    this.blocks = blocks;
  }

  /**
   * 获取所有块位置信息数组
   * @return 块位置信息数组
   */
  public BlockWithLocations[] getBlocks() {
    return blocks;
  }
}