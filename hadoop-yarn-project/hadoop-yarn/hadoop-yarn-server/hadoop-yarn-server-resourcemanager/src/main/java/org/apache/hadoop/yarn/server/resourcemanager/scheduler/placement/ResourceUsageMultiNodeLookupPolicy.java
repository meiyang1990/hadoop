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

import java.util.Comparator;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;

/**
 * 文件说明：YARN容器放置策略的多节点查找实现，基于节点已分配资源使用率对节点排序
 * <p>
 * 该类核心功能：
 * <p>
 * 根据节点当前资源使用情况对节点集合进行排序，为容器分配优先选择资源使用率较低的节点，实现负载均衡
 * </p>
 */
public class ResourceUsageMultiNodeLookupPolicy<N extends SchedulerNode>
    implements MultiNodeLookupPolicy<N> {

  // 按分区分组存储排序后的节点集合
  protected Map<String, Set<N>> nodesPerPartition = new ConcurrentHashMap<>();
  // 节点排序比较器
  protected Comparator<N> comparator;

  /**
   * 构造函数，初始化基于资源使用率的节点排序比较器
   * 排序规则：已分配资源越少的节点排在越前面，相同资源量时按节点ID排序
   */
  public ResourceUsageMultiNodeLookupPolicy() {
    this.comparator = new Comparator<N>() {
      @Override
      public int compare(N o1, N o2) {
        // 先比较已分配资源大小
        int allocatedDiff = o1.getAllocatedResource()
            .compareTo(o2.getAllocatedResource());
        if (allocatedDiff == 0) {
          // 资源相同时按节点ID排序保证顺序稳定
          return o1.getNodeID().compareTo(o2.getNodeID());
        }
        return allocatedDiff;
      }
    };
  }

  /**
   * 获取已排序的优选节点迭代器，按资源使用率从小到大返回节点
   * @param nodes 候选节点集合（未使用，直接返回预排序好的集合）
   * @param partition 分区名称
   * @return 排序后的节点迭代器
   */
  @Override
  public Iterator<N> getPreferredNodeIterator(Collection<N> nodes,
      String partition) {
    return getNodesPerPartition(partition).iterator();
  }

  /**
   * 重新构建指定分区的节点集合，按资源使用率重新排序后更新缓存
   * @param nodes 最新的候选节点集合
   * @param partition 分区名称
   */
  @Override
  public void addAndRefreshNodesSet(Collection<N> nodes,
      String partition) {
    // 创建基于比较器的并发跳表，自动排序节点
    Set<N> nodeList = new ConcurrentSkipListSet<N>(comparator);
    nodeList.addAll(nodes);
    // 更新分区缓存，返回不可修改集合保证线程安全
    nodesPerPartition.put(partition, Collections.unmodifiableSet(nodeList));
  }

  /**
   * 获取指定分区已排序好的节点集合
   * @param partition 分区名称
   * @return 指定分区的节点集合，分区不存在则返回空集合
   */
  @Override
  public Set<N> getNodesPerPartition(String partition) {
    return nodesPerPartition.getOrDefault(partition, Collections.emptySet());
  }
}