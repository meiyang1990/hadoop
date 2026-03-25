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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.placement;

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;

import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

/**
 * YARN多节点查找策略接口，为不同的容器放置分配器提供可扩展的节点选择机制，
 * 允许根据实际需求自定义节点排序与选择算法。
 * 
 * @param <N> 泛型类型，必须是SchedulerNode或其子类
 */
public interface MultiNodeLookupPolicy<N extends SchedulerNode> {
  /**
   * 根据需求和可用状态获取偏好节点的迭代器
   *
   * @param nodes
   *          待选节点集合
   * @param partition
   *          节点标签分区
   *
   * @return 排序后的偏好节点迭代器
   */
  Iterator<N> getPreferredNodeIterator(Collection<N> nodes, String partition);

  /**
   * 添加节点集合，并根据所选算法重新排序刷新分区对应的工作节点集合
   *
   * @param nodes
   *          新增的工作节点集合
   * @param partition
   *          节点标签分区
   */
  void addAndRefreshNodesSet(Collection<N> nodes, String partition);

  /**
   * 获取指定分区排序后的节点集合
   *
   * @param partition
   *          节点标签分区
   *
   * @return 排序后的节点集合
   */
  Set<N> getNodesPerPartition(String partition);

}