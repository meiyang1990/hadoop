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

import org.apache.hadoop.hdfs.protocol.Block;

/**
 * HDFS增量块报告中存储已接收/已删除块信息的数据结构，用于DataNode向NameNode上报块状态变更。
 */
public class ReceivedDeletedBlockInfo {
  Block block;
  BlockStatus status;
  String delHints;

  /**
   * 块状态枚举，标识块当前的生命周期状态
   */
  public enum BlockStatus {
    /** 正在接收中还未完成的块 */
    RECEIVING_BLOCK(1),
    /** 接收完成的块 */
    RECEIVED_BLOCK(2),
    /** 已删除的块 */
    DELETED_BLOCK(3);
    
    private final int code;
    /**
     * 构造函数，通过枚举编码创建状态实例
     * @param code 状态对应的整数编码
     */
    BlockStatus(int code) {
      this.code = code;
    }
    
    /**
     * 获取状态对应的整数编码
     * @return 状态编码
     */
    public int getCode() {
      return code;
    }
    
    /**
     * 根据整数编码解析得到对应的状态枚举
     * @param code 状态整数编码
     * @return 对应的状态枚举，编码不存在返回null
     */
    public static BlockStatus fromCode(int code) {
      for (BlockStatus bs : BlockStatus.values()) {
        if (bs.code == code) {
          return bs;
        }
      }
      return null;
    }
  }

  /**
   * 空构造函数
   */
  public ReceivedDeletedBlockInfo() {
  }

  /**
   * 构造包含指定块信息、状态、删除提示的实例
   * @param blk 目标块
   * @param status 块状态
   * @param delHints 删除提示信息
   */
  public ReceivedDeletedBlockInfo(
      Block blk, BlockStatus status, String delHints) {
    this.block = blk;
    this.status = status;
    this.delHints = delHints;
  }

  /**
   * 获取当前实例存储的块对象
   * @return 块对象
   */
  public Block getBlock() {
    return this.block;
  }

  /**
   * 设置当前实例存储的块对象
   * @param blk 块对象
   */
  public void setBlock(Block blk) {
    this.block = blk;
  }

  /**
   * 获取删除提示信息
   * @return 删除提示字符串
   */
  public String getDelHints() {
    return this.delHints;
  }

  /**
   * 设置删除提示信息
   * @param hints 删除提示字符串
   */
  public void setDelHints(String hints) {
    this.delHints = hints;
  }

  /**
   * 获取块状态
   * @return 块状态枚举
   */
  public BlockStatus getStatus() {
    return status;
  }

  @Override
  public boolean equals(Object o) {
    // 类型检查，不匹配直接返回false
    if (!(o instanceof ReceivedDeletedBlockInfo)) {
      return false;
    }
    ReceivedDeletedBlockInfo other = (ReceivedDeletedBlockInfo) o;
    // 比较块、状态、删除提示三个字段全部相等才返回true
    return this.block.equals(other.getBlock())
        && this.status == other.status
        && this.delHints != null
        && this.delHints.equals(other.delHints);
  }

  @Override
  public int hashCode() {
    // 该类未设计hashCode方法，断言提示开发者不要使用
    assert false : "hashCode not designed";
    return 0; 
  }

  /**
   * 判断当前存储的块是否等于指定块
   * @param b 待比较的块
   * @return 相等返回true，否则返回false
   */
  public boolean blockEquals(Block b) {
    return this.block.equals(b);
  }

  /**
   * 判断当前块是否处于已删除状态
   * @return 是已删除状态返回true，否则返回false
   */
  public boolean isDeletedBlock() {
    return status == BlockStatus.DELETED_BLOCK;
  }

  @Override
  public String toString() {
    return block.toString() + ", status: " + status +
      ", delHint: " + delHints;
  }
}