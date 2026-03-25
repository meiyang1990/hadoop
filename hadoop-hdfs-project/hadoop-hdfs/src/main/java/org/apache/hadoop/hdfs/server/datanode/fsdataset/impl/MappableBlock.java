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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.ExtendedBlockId;

import java.io.Closeable;

/**
 * 文件级注释：HDFS DataNode端已内存映射块的抽象接口，定义了内存缓存块需要提供的基础能力
 *
 * Represents an HDFS block that is mapped by the DataNode.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public interface MappableBlock extends Closeable {

  /**
   * 获取已缓存到内存的块的字节长度
   * @return 已缓存块的字节长度
   */
  long getLength();

  /**
   * 获取块在内存中的缓存地址，不支持时返回-1
   * @return 内存缓存地址，不适用则返回-1
   */
  long getAddress();

  /**
   * 获取当前缓存块的扩展块ID，用于唯一标识缓存块
   * @return 当前缓存块的ExtendedBlockId标识
   */
  ExtendedBlockId getKey();
}