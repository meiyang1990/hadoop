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

import java.util.Arrays;

/**
 * 文件所属模块：HDFS服务端核心协议
 * 类的核心职责：封装DataNode单个存储目录上报的已接收和已删除块的报告信息，用于DataNode向NameNode发送块增量状态更新
 */
public class StorageReceivedDeletedBlocks {
  final DatanodeStorage storage;
  private final ReceivedDeletedBlockInfo[] blocks;

  /**
   * 获取存储ID的过时方法，推荐使用getStorage()获取完整存储信息
   * @deprecated Use {@link #getStorage()} instead
   */
  @Deprecated
  public String getStorageID() {
    return storage.getStorageID();
  }

  /**
   * 获取当前报告对应的DataNode存储信息
   * @return DataNode存储对象
   */
  public DatanodeStorage getStorage() {
    return storage;
  }

  /**
   * 获取本次上报的所有块信息（包含接收和删除的块）
   * @return 块信息数组
   */
  public ReceivedDeletedBlockInfo[] getBlocks() {
    return blocks;
  }

  /**
   * 过时构造方法，仅通过存储ID构造报告，推荐使用包含完整DatanodeStorage参数的构造方法
   * @deprecated Use {@link #StorageReceivedDeletedBlocks(
   * DatanodeStorage, ReceivedDeletedBlockInfo[])} instead
   * @param storageID 存储ID
   * @param blocks 本次上报的块信息数组
   */
  @Deprecated
  public StorageReceivedDeletedBlocks(final String storageID,
      final ReceivedDeletedBlockInfo[] blocks) {
    this.storage = new DatanodeStorage(storageID);
    this.blocks = blocks;
  }

  /**
   * 构造DataNode单个存储的增量块报告
   * @param storage DataNode存储信息对象
   * @param blocks 本次上报的块信息数组
   */
  public StorageReceivedDeletedBlocks(final DatanodeStorage storage,
      final ReceivedDeletedBlockInfo[] blocks) {
    this.storage = storage;
    this.blocks = blocks;
  }

  @Override
  public String toString() {
    return storage + Arrays.toString(blocks);
  }
}