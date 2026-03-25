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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.util.Preconditions;
import org.apache.hadoop.yarn.server.resourcemanager.ClusterMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * YARN ResourceManager 集群节点跟踪器，负责：
 * - 跟踪集群中所有调度节点的状态信息
 * - 提供节点过滤、排序、分组查询的便捷方法
 */
@InterfaceAudience.Private
public class ClusterNodeTracker<N extends SchedulerNode> {
  private static final Logger LOG =
      LoggerFactory.getLogger(ClusterNodeTracker.class);

  // 读写锁，保障并发访问节点数据的线程安全
  private ReadWriteLock readWriteLock = new ReentrantReadWriteLock(true);
  private Lock readLock = readWriteLock.readLock();
  private Lock writeLock = readWriteLock.writeLock();

  // NodeId -> 调度节点 映射表
  private HashMap<NodeId, N> nodes = new HashMap<>();
  // 节点主机名 -> 调度节点 映射表
  private Map<String, N> nodeNameToNodeMap = new HashMap<>();
  // 机架名 -> 该机架下节点列表 分组映射
  private Map<String, List<N>> nodesPerRack = new HashMap<>();
  // 节点标签分区 -> 该分区下节点列表 分组映射
  private Map<String, List<N>> nodesPerLabel = new HashMap<>();

  // 集群总容量（实时计算值）
  private Resource clusterCapacity = Resources.createResource(0, 0);
  // 集群总容量（对外暴露的缓存快照，保证并发读取可见性）
  private volatile Resource staleClusterCapacity =
      Resources.clone(Resources.none());

  // 各资源类型的最大单节点分配量
  private final long[] maxAllocation;
  // 配置的最大允许分配量
  private Resource configuredMaxAllocation;
  // 是否强制使用配置的最大分配量
  private boolean forceConfiguredMaxAllocation = true;
  // 强制使用配置最大分配量的等待超时时间
  private long configuredMaxAllocationWaitTime;
  // 标记是否已有节点上报了资源信息
  private boolean reportedMaxAllocation = false;

  /**
   * 构造一个空的集群节点跟踪器
   */
  public ClusterNodeTracker() {
    maxAllocation = new long[ResourceUtils.getNumberOfCountableResourceTypes()];
    Arrays.fill(maxAllocation, -1);
  }

  /**
   * 添加新节点到集群跟踪器
   * @param node 要添加的调度节点
   */
  public void addNode(N node) {
    writeLock.lock();
    try {
      // 更新节点映射
      nodes.put(node.getNodeID(), node);
      nodeNameToNodeMap.put(node.getNodeName(), node);

      // 更新节点标签分区分组
      List<N> nodesPerLabels = nodesPerLabel.get(node.getPartition());

      if (nodesPerLabels == null) {
        nodesPerLabels = new ArrayList<N>();
      }
      nodesPerLabels.add(node);
      nodesPerLabel.put(node.getPartition(), nodesPerLabels);

      // 更新机架分组
      String rackName = node.getRackName();
      List<N> nodesList = nodesPerRack.get(rackName);
      if (nodesList == null) {
        nodesList = new ArrayList<>();
        nodesPerRack.put(rackName, nodesList);
      }
      nodesList.add(node);

      // 更新集群总容量
      Resources.addTo(clusterCapacity, node.getTotalResource());
      staleClusterCapacity = Resources.clone(clusterCapacity);
      ClusterMetrics.getMetrics().incrCapability(node.getTotalResource());

      // 更新最大分配量统计
      updateMaxResources(node, true);
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * 检查指定节点是否存在于集群中
   * @param nodeId 节点ID
   * @return 是否存在
   */
  public boolean exists(NodeId nodeId) {
    readLock.lock();
    try {
      return nodes.containsKey(nodeId);
    } finally {
      readLock.unlock();
    }
  }

  /**
   * 根据节点ID获取调度节点对象
   * @param nodeId 节点ID
   * @return 调度节点对象，不存在则返回null
   */
  public N getNode(NodeId nodeId) {
    readLock.lock();
    try {
      return nodes.get(nodeId);
    } finally {
      readLock.unlock();
    }
  }

  /**
   * 获取指定节点的调度报告
   * @param nodeId 节点ID
   * @return 节点调度报告，节点不存在则返回null
   */
  public SchedulerNodeReport getNodeReport(NodeId nodeId) {
    readLock.lock();
    try {
      N n = nodes.get(nodeId);
      return n == null ? null : new SchedulerNodeReport(n);
    } finally {
      readLock.unlock();
    }
  }

  /**
   * 获取集群总节点数
   * @return 集群节点总数
   */
  public int nodeCount() {
    readLock.lock();
    try {
      return nodes.size();
    } finally {
      readLock.unlock();
    }
  }

  /**
   * 获取指定机架的节点数
   * @param rackName 机架名称
   * @return 指定机架的节点数
   */
  public int nodeCount(String rackName) {
    readLock.lock();
    String rName = rackName == null ? "NULL" : rackName;
    try {
      List<N> nodesList = nodesPerRack.get(rName);
      return nodesList == null ? 0 : nodesList.size();
    } finally {
      readLock.unlock();
    }
  }

  /**
   * 获取集群总容量（返回缓存快照）
   * @return 集群总容量
   */
  public Resource getClusterCapacity() {
    return staleClusterCapacity;
  }

  /**
   * 从集群中移除指定节点
   * @param nodeId 要移除的节点ID
   * @return 被移除的节点对象，节点不存在则返回null
   */
  public N removeNode(NodeId nodeId) {
    writeLock.lock();
    try {
      N node = nodes.remove(nodeId);
      if (node == null) {
        LOG.warn("Attempting to remove a non-existent node " + nodeId);
        return null;
      }
      // 移除主机名映射
      nodeNameToNodeMap.remove(node.getNodeName());

      // 从机架分组中移除
      String rackName = node.getRackName();
      List<N> nodesList = nodesPerRack.get(rackName);
      if (nodesList == null) {
        LOG.error("Attempting to remove node from an empty rack " + rackName);
      } else {
        nodesList.remove(node);
        if (nodesList.isEmpty()) {
          nodesPerRack.remove(rackName);
        }
      }

      // 从标签分区分组中移除
      List<N> nodesPerPartition = nodesPerLabel.get(node.getPartition());
      nodesPerPartition.remove(node);

      if (nodesPerPartition.isEmpty()) {
        nodesPerLabel.remove(node.getPartition());
      } else {
        nodesPerLabel.put(node.getPartition(), nodesPerPartition);
      }

      // 更新集群总容量
      Resources.subtractFrom(clusterCapacity, node.getTotalResource());
      staleClusterCapacity = Resources.clone(clusterCapacity);
      ClusterMetrics.getMetrics().decrCapability(node.getTotalResource());

      // 更新最大分配量统计
      updateMaxResources(node, false);

      return node;
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * 设置配置的最大允许分配量
   * @param resource 配置的最大分配量
   */
  public void setConfiguredMaxAllocation(Resource resource) {
    writeLock.lock();
    try {
      configuredMaxAllocation = Resources.clone(resource);
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * 设置强制使用配置最大分配量的等待超时时间
   * @param configuredMaxAllocationWaitTime 等待超时时间（毫秒）
   */
  public void setConfiguredMaxAllocationWaitTime(
      long configuredMaxAllocationWaitTime) {
    writeLock.lock();
    try {
      this.configuredMaxAllocationWaitTime =
          configuredMaxAllocationWaitTime;
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * 获取最终允许的最大分配量
   * @return 允许的最大分配量，取配置值和节点实际最大值中的较小值
   */
  public Resource getMaxAllowedAllocation() {
    readLock.lock();
    try {
      // 如果超过等待超时时间，取消强制使用配置值
      if (forceConfiguredMaxAllocation &&
          System.currentTimeMillis() - ResourceManager.getClusterTimeStamp()
              > configuredMaxAllocationWaitTime) {
        forceConfiguredMaxAllocation = false;
      }

      // 仍在强制期或还没有节点上报，直接返回配置值
      if (forceConfiguredMaxAllocation || !reportedMaxAllocation) {
        return configuredMaxAllocation;
      }

      // 对每个资源类型，取配置值和实际节点最大值中的较小值
      Resource ret = Resources.clone(configuredMaxAllocation);

      for (int i = 0; i < maxAllocation.length; i++) {
        ResourceInformation info = ret.getResourceInformation(i);

        if (info.getValue() > maxAllocation[i]) {
          info.setValue(maxAllocation[i]);
        }
      }

      return ret;
    } finally {
      readLock.unlock();
    }
  }

  @VisibleForTesting
  /**
   * 设置是否强制使用配置的最大分配量（仅用于测试）
   * @param flag 是否强制
   */
  public void setForceConfiguredMaxAllocation(boolean flag) {
    writeLock.lock();
    try {
      forceConfiguredMaxAllocation = flag;
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * 更新最大资源统计，在添加/移除节点时调用
   * @param node 变更的节点
   * @param add true表示添加节点，false表示移除节点
   */
  private void updateMaxResources(SchedulerNode node, boolean add) {
    Resource totalResource = node.getTotalResource();
    ResourceInformation[] totalResources;

    if (totalResource != null) {
      totalResources = totalResource.getResources();
    } else {
      LOG.warn(node.getNodeName() + " reported in with null resources, which "
          + "indicates a problem in the source code. Please file an issue at "
          + "https://issues.apache.org/jira/secure/CreateIssue!default.jspa");

      return;
    }

    writeLock.lock();

    try {
      if (add) { // 添加节点场景
        // 标记已有节点上报资源
        reportedMaxAllocation = true;

        // 更新每个资源类型的最大值
        for (int i = 0; i < maxAllocation.length; i++) {
          long value = totalResources[i].getValue();

          if (value > maxAllocation[i]) {
            maxAllocation[i] = value;
          }
        }
      } else {  // 移除节点场景
        boolean recalculate = false;

        // 如果被移除节点正好持有当前最大值，需要重新计算
        for (int i = 0; i < maxAllocation.length; i++) {
          if (totalResources[i].getValue() == maxAllocation[i]) {
            maxAllocation[i] = -1;
            recalculate = true;
          }
        }

        // 需要重新计算所有节点的最大值
        if (recalculate) {
          reportedMaxAllocation = false;
          nodes.values().forEach(n -> updateMaxResources(n, true));
        }
      }
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * 获取集群所有节点列表
   * @return 所有节点列表
   */
  public List<N> getAllNodes() {
    return getNodes(null);
  }

  /**
   * 根据过滤条件获取节点列表
   * @param nodeFilter 节点过滤器，null不过滤
   * @return 过滤后的节点列表
   */
  public List<N> getNodes(NodeFilter nodeFilter) {
    List<N> nodeList = new ArrayList<>();
    readLock.lock();
    try {
      if (nodeFilter == null) {
        nodeList.addAll(nodes.values());
      } else {
        for (N node : nodes.values()) {
          if (nodeFilter.accept(node)) {
            nodeList.add(node);
          }
        }
      }
    } finally {
      readLock.unlock();
    }
    return nodeList;
  }

  /**
   * 获取集群所有节点ID列表
   * @return 所有节点ID列表
   */
  public List<NodeId> getAllNodeIds() {
    return getNodeIds(null);
  }

  /**
   * 根据过滤条件获取节点ID列表
   * @param nodeFilter 节点过滤器，null不过滤
   * @return 过滤后的节点ID列表
   */
  public List<NodeId> getNodeIds(NodeFilter nodeFilter) {
    List<NodeId> nodeList = new ArrayList<>();
    readLock.lock();
    try {
      if (nodeFilter == null) {
        for (N node : nodes.values()) {
          nodeList.add(node.getNodeID());
        }
      } else {
        for (N node : nodes.values()) {
          if (nodeFilter.accept(node)) {
            nodeList.add(node.getNodeID());
          }
        }
      }
    } finally {
      readLock.unlock();
    }
    return nodeList;
  }

  /**
   * 获取按指定比较器排序的节点集合
   * 使用TreeSet保证在节点动态变化时仍能正常排序
   * @param comparator 节点比较器
   * @return 排序后的节点TreeSet
   */
  public TreeSet<N> sortedNodeSet(Comparator<N> comparator) {
    TreeSet<N> sortedSet = new TreeSet<>(comparator);
    readLock.lock();
    try {
      sortedSet.addAll(nodes.values());
    } finally {
      readLock.unlock();
    }
    return sortedSet;
  }

  /**
   * 根据ResourceRequest中的资源名称获取匹配的节点列表
   * 资源名称可以是ANY、主机名或机架名
   * @param resourceName 资源名称
   * @return 匹配的节点列表
   */
  public List<N> getNodesByResourceName(final String resourceName) {
    Preconditions.checkArgument(
        resourceName != null && !resourceName.isEmpty());
    List<N> retNodes = new ArrayList<>();
    if (ResourceRequest.ANY.equals(resourceName)) {
      retNodes.addAll(getAllNodes());
    } else if (nodeNameToNodeMap.containsKey(resourceName)) {
      retNodes.add(nodeNameToNodeMap.get(resourceName));
    } else if (nodesPerRack.containsKey(resourceName)) {
      retNodes.addAll(nodesPerRack.get(resourceName));
    } else {
      LOG.info(
          "Could not find a node matching given resourceName " + resourceName);
    }
    return retNodes;
  }

  /**
   * 根据ResourceRequest中的资源名称获取匹配的节点ID列表
   * 资源名称可以是ANY、主机名或机架名
   * @param resourceName 资源名称
   * @return 匹配的节点ID列表
   */
  public List<NodeId> getNodeIdsByResourceName(final String resourceName) {
    Preconditions.checkArgument(
        resourceName != null && !resourceName.isEmpty());
    List<NodeId> retNodes = new ArrayList<>();
    if (ResourceRequest.ANY.equals(resourceName)) {
      retNodes.addAll(getAllNodeIds());
    } else if (nodeNameToNode