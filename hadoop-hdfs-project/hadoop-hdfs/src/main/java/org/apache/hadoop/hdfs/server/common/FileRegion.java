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
package org.apache.hadoop.hdfs.server.common;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.ProvidedStorageLocation;

/**
 * 文件路径文件区域类，表示外部提供存储中基于文件偏移量定义的数据块，
 * 用于HDFS外部存储（如云存储、外部存储系统）数据块的引用，通过文件路径、偏移量、长度描述数据区域。
 * 实现BlockAlias接口，可作为HDFS块的别名存在。
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public class FileRegion implements BlockAlias {

  /** 存储块信息和对应外部存储位置的对 */
  private final Pair<Block, ProvidedStorageLocation> pair;

  /**
   * 构造FileRegion，使用指定块ID、文件路径、偏移量、长度和生成时间戳
   * @param blockId 块ID
   * @param path 数据所在文件路径
   * @param offset 数据在文件中的起始偏移量
   * @param length 数据长度
   * @param genStamp 块生成时间戳
   */
  public FileRegion(long blockId, Path path, long offset,
      long length, long genStamp) {
    this(blockId, path, offset, length, genStamp, new byte[0]);
  }

  /**
   * 构造FileRegion，支持指定nonce用于唯一性标识
   * @param blockId 块ID
   * @param path 数据所在文件路径
   * @param offset 数据在文件中的起始偏移量
   * @param length 数据长度
   * @param genStamp 块生成时间戳
   * @param nonce 唯一性随机数
   */
  public FileRegion(long blockId, Path path, long offset,
                    long length, long genStamp, byte[] nonce) {
    this(new Block(blockId, length, genStamp),
            new ProvidedStorageLocation(path, offset, length, nonce));
  }

  /**
   * 构造FileRegion，使用默认的祖父级生成时间戳
   * @param blockId 块ID
   * @param path 数据所在文件路径
   * @param offset 数据在文件中的起始偏移量
   * @param length 数据长度
   */
  public FileRegion(long blockId, Path path, long offset, long length) {
    this(blockId, path, offset, length,
        HdfsConstants.GRANDFATHER_GENERATION_STAMP);
  }

  /**
   * 构造FileRegion，直接使用已有的Block和ProvidedStorageLocation对象
   * @param block HDFS块对象
   * @param providedStorageLocation 外部存储位置对象
   */
  public FileRegion(Block block,
      ProvidedStorageLocation providedStorageLocation) {
    this.pair  = Pair.of(block, providedStorageLocation);
  }

  /**
   * 获取当前FileRegion对应的HDFS块对象
   * @return HDFS块对象
   */
  public Block getBlock() {
    return pair.getKey();
  }

  /**
   * 获取当前FileRegion对应的外部存储位置信息
   * @return 外部存储位置对象，包含路径、偏移量、长度信息
   */
  public ProvidedStorageLocation getProvidedStorageLocation() {
    return pair.getValue();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    FileRegion that = (FileRegion) o;

    return pair.equals(that.pair);
  }

  @Override
  public int hashCode() {
    return pair.hashCode();
  }
}