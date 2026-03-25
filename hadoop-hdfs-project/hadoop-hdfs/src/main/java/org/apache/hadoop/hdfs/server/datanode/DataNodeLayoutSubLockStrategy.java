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
 * 数据节点块目录层级结构的子锁策略实现
 * 按照数据节点原有的块目录层级划分粒度，对数据块的并发操作进行细粒度锁控制
 * 实现了DataSetSubLockStrategy接口，基于块ID生成子锁名称，适配数据节点原有目录结构
 */
public class DataNodeLayoutSubLockStrategy implements DataSetSubLockStrategy {

  /**
   * 根据块ID生成对应的子锁名称
   * 沿用数据节点原有块目录分桶规则作为锁划分依据，保证目录结构和锁粒度一致
   * @param blockid 数据块ID
   * @return 对应子锁的名称
   */
  @Override
  public String blockIdToSubLock(long blockid) {
    return DatanodeUtil.idToBlockDirSuffix(blockid);
  }

  /**
   * 获取所有可能的子锁名称列表
   * 用于初始化所有子锁对象，覆盖数据节点所有可能的块目录分桶
   * @return 所有子锁名称的完整列表
   */
  @Override
  public List<String> getAllSubLockNames() {
    return DatanodeUtil.getAllSubDirNameForDataSetLock();
  }
}