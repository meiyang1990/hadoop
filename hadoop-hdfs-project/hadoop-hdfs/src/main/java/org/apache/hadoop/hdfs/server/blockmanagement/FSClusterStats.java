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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.StorageType;

import java.util.Map;

/**
 * 文件级注释：HDFS集群负载统计接口，定义了获取集群整体负载、节点状态和存储统计的统一方法
 * 该接口用于获取集群负载相关统计信息，供块放置选择、负载均衡等模块查询集群状态
 */
@InterfaceAudience.Private
/**
 * 集群负载统计接口，定义获取HDFS集群各类负载和状态统计信息的统一规范
 * 核心职责：为块放置决策、负载均衡提供集群运行状态统计数据
 */
public interface FSClusterStats {

  /**
   * 获取集群当前总体负载
   * @return 集群当前正在进行的块传输和块写入操作总数量
   */
  public int getTotalLoad();

  /**
   * 查询集群是否开启了避免将数据写入过时DataNode的策略
   * @return true表示当前集群正在避免使用过时DataNode作为写入目标，false表示未开启该策略
   */
  public boolean isAvoidingStaleDataNodesForWrite();

  /**
   * 获取当前处于服务可用状态的DataNode数量
   * @return 存活且未处于退役/已退役状态的DataNode数量
   */
  public int getNumDatanodesInService();

  /**
   * 获取可用于块放置的在服务节点的平均负载
   * @return 在服务节点上当前正在进行的块传输和块写入操作的平均值
   */
  public double getInServiceXceiverAverage();

  /**
   * 获取可用于块放置的在服务节点卷层面的平均负载
   * @return 在服务节点所有卷上当前正在进行的块传输和块写入操作的平均值
   */
  double getInServiceXceiverAverageForVolume();

  /**
   * 获取按存储类型分类的存储统计信息
   * @return 存储类型到对应存储统计信息的映射表
   */
  Map<StorageType, StorageTypeStats> getStorageTypeStats();
}