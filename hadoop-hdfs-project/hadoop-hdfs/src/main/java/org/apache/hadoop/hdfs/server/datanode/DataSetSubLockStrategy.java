// 这个文件已经全部加上中文注释
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdfs.server.datanode;

import java.util.List;

/**
 * 文件职责：定义数据节点数据集分段锁策略接口，用于将数据块映射到不同的子锁，
 * 实现细粒度锁控制，降低锁竞争，提升DataNode多线程并发操作性能。
 * <p>
 * This interface is used to generate sub lock name for a blockid.
 */
public interface DataSetSubLockStrategy {

  /**
   * 根据数据块ID计算对应子锁的名称，实现数据块到子锁的映射。
   * @param blockid 目标数据块ID
   * @return 该数据块对应的子锁名称
   */
  String blockIdToSubLock(long blockid);

  /**
   * 获取当前策略定义的所有子锁名称，用于提前初始化所有锁实例。
   * @return 当前策略包含的所有子锁名称列表
   */
  List<String> getAllSubLockNames();
}