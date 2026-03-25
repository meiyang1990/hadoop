// 这个文件已经全部加上中文注释
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.aliasmap;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.ProvidedStorageLocation;
import org.apache.hadoop.hdfs.server.common.FileRegion;
import org.apache.hadoop.io.retry.Idempotent;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.List;
import java.util.Optional;

/**
 * 文件级注释：HDFS 内置块别名映射内存实现的RPC协议接口，定义了客户端对内存别名映射表的读写操作规范
 * 用于支持提供存储（Provided Storage）场景下，外部存储块与存储位置的别名映射管理
 */
/**
 * Protocol used by clients to read/write data about aliases of
 * provided blocks for an in-memory implementation of the
 * {@link org.apache.hadoop.hdfs.server.common.blockaliasmap.BlockAliasMap}.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public interface InMemoryAliasMapProtocol {

  /**
 * 迭代查询结果封装类，存储分页查询别名映射的一批结果和下一次查询的起始标记
 * 支持分批遍历整个别名映射表，避免一次性加载全部数据导致内存占用过高
 */
  /**
   * The result of a read from the in-memory aliasmap. It contains the
   * a list of FileRegions that are returned, along with the next block
   * from which the read operation must continue.
   */
  class IterationResult {

    private final List<FileRegion> batch;
    private final Optional<Block> nextMarker;

    public IterationResult(List<FileRegion> batch, Optional<Block> nextMarker) {
      this.batch = batch;
      this.nextMarker = nextMarker;
    }

    public List<FileRegion> getFileRegions() {
      return batch;
    }

    public Optional<Block> getNextBlock() {
      return nextMarker;
    }
  }

  /**
   * 分页遍历别名映射表，从指定标记位置获取下一批文件区域信息
   * @param marker 本次查询的起始块标记，为空表示从表头开始查询
   * @return 迭代结果，包含一批文件区域和下一次查询的起始标记
   * @throws IO异常
   */
  /**
   * List the next batch of {@link FileRegion}s in the alias map starting from
   * the given {@code marker}. To retrieve all {@link FileRegion}s stored in the
   * alias map, multiple calls to this function might be required.
   * @param marker the next block to get fileregions from.
   * @return the {@link IterationResult} with a set of
   * FileRegions and the next marker.
   * @throws IOException
   */
  @Idempotent
  InMemoryAliasMap.IterationResult list(Optional<Block> marker)
      throws IOException;

  /**
   * 根据指定数据块查询对应的提供存储位置
   * @param block 要查询的数据块
   * @return 数据块对应的提供存储位置，如果不存在则返回空
   * @throws IO异常
   */
  /**
   * Gets the {@link ProvidedStorageLocation} associated with the
   * specified block.
   * @param block the block to lookup
   * @return the associated {@link ProvidedStorageLocation}.
   * @throws IOException
   */
  @Nonnull
  @Idempotent
  Optional<ProvidedStorageLocation> read(@Nonnull Block block)
      throws IOException;

  /**
   * 将数据块及其对应的提供存储位置写入内存别名映射表
   * @param block 数据块对象
   * @param providedStorageLocation 数据块对应的存储位置
   * @throws IO异常
   */
  /**
   * Stores the block and it's associated {@link ProvidedStorageLocation}
   * in the alias map.
   * @param block
   * @param providedStorageLocation
   * @throws IOException
   */
  @Idempotent
  void write(@Nonnull Block block,
      @Nonnull ProvidedStorageLocation providedStorageLocation)
      throws IOException;

  /**
   * 获取当前别名映射表所属的块池ID
   * @return 对应NameNode的块池ID
   * @throws IO异常
   */
  /**
   * Get the associated block pool id.
   * @return the block pool id associated with the Namenode running
   * the in-memory alias map.
   */
  @Idempotent
  String getBlockPoolId() throws IOException;
}