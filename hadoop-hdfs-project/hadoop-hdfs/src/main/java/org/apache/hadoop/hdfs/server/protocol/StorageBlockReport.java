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

import org.apache.hadoop.hdfs.protocol.BlockListAsLongs;

/**
 * 数据节点单个存储目录的块报告，封装DataNode向NameNode上报的指定存储目录上所有块的信息
 */
public class StorageBlockReport {
  private final DatanodeStorage storage;
  private final BlockListAsLongs blocks;
  
  /**
   * 构造存储块报告对象
   * @param storage 目标数据节点存储目录信息
   * @param blocks 该存储目录上的所有数据块列表，压缩编码为long数组格式
   */
  public StorageBlockReport(DatanodeStorage storage, BlockListAsLongs blocks) {
    this.storage = storage;
    this.blocks = blocks;
  }

  /**
   * 获取本次报告对应的存储目录信息
   * @return 数据节点存储信息对象
   */
  public DatanodeStorage getStorage() {
    return storage;
  }

  /**
   * 获取该存储目录上报的所有数据块列表
   * @return 压缩编码后的块列表对象
   */
  public BlockListAsLongs getBlocks() {
    return blocks;
  }
}