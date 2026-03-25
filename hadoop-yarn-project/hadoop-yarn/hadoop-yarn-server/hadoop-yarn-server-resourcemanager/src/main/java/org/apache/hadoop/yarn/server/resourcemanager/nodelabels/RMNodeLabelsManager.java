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

package org.apache.hadoop.yarn.server.resourcemanager.nodelabels;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeLabel;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.nodelabels.CommonNodeLabelsManager;
import org.apache.hadoop.yarn.nodelabels.RMNodeLabel;
import org.apache.hadoop.yarn.security.YarnAuthorizationProvider;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeLabelsUpdateSchedulerEvent;
import org.apache.hadoop.yarn.util.resource.Resources;

import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableSet;

/**
 * ResourceManager节点标签管理器，继承通用节点标签管理能力，
 * 负责维护节点标签与队列的关联关系，计算各标签对应的可用资源，
 * 支持节点标签的动态更新并通知调度器资源变化。
 */
public class RMNodeLabelsManager extends CommonNodeLabelsManager {
  /**
   * 队列的节点标签信息封装类，保存队列可访问的标签和对应可用资源
   */
  protected static class Queue {
    protected Set<String> accessibleNodeLabels;
    protected Resource resource;

    protected Queue() {
      accessibleNodeLabels =
          Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());
      resource = Resource.newInstance(0, 0);
    }
  }

  // 队列名称 -> 队列标签信息的映射
  ConcurrentMap<String, Queue> queueCollections =
      new ConcurrentHashMap<String, Queue>();
  private YarnAuthorizationProvider authorizer;
  private RMContext rmContext = null;
  
  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    super.serviceInit(conf);
    // 初始化权限检查器
    authorizer = YarnAuthorizationProvider.getInstance(conf);
  }

  @Override
  public void addLabelsToNode(Map<NodeId, Set<String>> addedLabelsToNode)
      throws IOException {
    writeLock.lock();
    try {
      // 保存修改前指定节点的信息，用于后续资源计算
      Map<String, Host> before = cloneNodeMap(addedLabelsToNode.keySet());

      super.addLabelsToNode(addedLabelsToNode);

      // 保存修改后指定节点的信息
      Map<String, Host> after = cloneNodeMap(addedLabelsToNode.keySet());

      // 更新各标签和队列的资源映射
      updateResourceMappings(before, after);
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * 检查待删除标签是否被队列占用，若占用则不允许删除
   * @param labelsToRemove 待删除标签集合
   * @throws IOException 若标签被队列占用则抛出异常
   */
  protected void checkRemoveFromClusterNodeLabelsOfQueue(
      Collection<String> labelsToRemove) throws IOException {
    // Check if label to remove doesn't existed or null/empty, will throw
    // exception if any of labels to remove doesn't meet requirement
    for (String label : labelsToRemove) {
      label = normalizeLabel(label);

      // 遍历所有队列检查是否包含当前标签
      for (Entry<String, Queue> entry : queueCollections.entrySet()) {
        String queueName = entry.getKey();
        Set<String> queueLabels = entry.getValue().accessibleNodeLabels;
        if (queueLabels.contains(label)) {
          throw new IOException("Cannot remove label=" + label
              + ", because queue=" + queueName + " is using this label. "
              + "Please remove label on queue before remove the label");
        }
      }
    }
  }

  @Override
  public void removeFromClusterNodeLabels(Collection<String> labelsToRemove)
      throws IOException {
    writeLock.lock();
    try {
      // 恢复过程中不做检查，避免回放编辑日志误判
      if (!isInitNodeLabelStoreInProgress()) {
        // We cannot remove node labels from collection when some queue(s) are
        // using any of them.
        // We will not do remove when recovery is in prpgress. During
        // service starting, we will replay edit logs and recover state. It is
        // possible that a history operation removed some labels which were not
        // used by some queues in the past but are used by current queues.
        checkRemoveFromClusterNodeLabelsOfQueue(labelsToRemove);
      }
      // 保存修改前所有节点信息
      Map<String, Host> before = cloneNodeMap();

      super.removeFromClusterNodeLabels(labelsToRemove);

      updateResourceMappings(before, nodeCollections);
    } finally {
      writeLock.unlock();
    }
  }

  @Override
  public void addToCluserNodeLabels(Collection<NodeLabel> labels)
      throws IOException {
    writeLock.lock();
    try {
      super.addToCluserNodeLabels(labels);
    } finally {
      writeLock.unlock();
    }
  }

  @Override
  public void
      removeLabelsFromNode(Map<NodeId, Set<String>> removeLabelsFromNode)
          throws IOException {
    writeLock.lock();
    try {
      // 保存修改前指定节点的信息
      Map<String, Host> before =
          cloneNodeMap(removeLabelsFromNode.keySet());

      super.removeLabelsFromNode(removeLabelsFromNode);

      // 保存修改后指定节点的信息
      Map<String, Host> after = cloneNodeMap(removeLabelsFromNode.keySet());

      // 更新各标签和队列的资源映射
      updateResourceMappings(before, after);
    } finally {
      writeLock.unlock();
    }
  }

  @Override
  public void replaceLabelsOnNode(Map<NodeId, Set<String>> replaceLabelsToNode)
      throws IOException {
    writeLock.lock();
    try {
      // 计算真正发生标签变更的节点映射
      Map<NodeId, Set<String>> effectiveModifiedLabelMappings =
          getModifiedNodeLabelsMappings(replaceLabelsToNode);

      if(effectiveModifiedLabelMappings.isEmpty()) {
        LOG.info("No Modified Node label Mapping to replace");
        return;
      }

      // 保存修改前节点信息
      Map<String, Host> before =
          cloneNodeMap(effectiveModifiedLabelMappings.keySet());

      super.replaceLabelsOnNode(effectiveModifiedLabelMappings);

      // 保存修改后节点信息
      Map<String, Host> after =
          cloneNodeMap(effectiveModifiedLabelMappings.keySet());

      // 更新资源映射
      updateResourceMappings(before, after);
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * 筛选出真正发生标签变更的节点，避免无意义更新
   * @param replaceLabelsToNode 请求替换的节点标签映射
   * @return 真正发生变更的节点标签映射
   */
  private Map<NodeId, Set<String>> getModifiedNodeLabelsMappings(
      Map<NodeId, Set<String>> replaceLabelsToNode) {
    Map<NodeId, Set<String>> effectiveModifiedLabels = new HashMap<>();
    // 遍历所有请求替换的节点
    for (Entry<NodeId, Set<String>> nodeLabelMappingEntry : replaceLabelsToNode
        .entrySet()) {
      NodeId nodeId = nodeLabelMappingEntry.getKey();
      Set<String> modifiedNodeLabels = nodeLabelMappingEntry.getValue();
      Set<String> labelsBeforeModification = null;
      Host host = nodeCollections.get(nodeId.getHost());
      // 节点不存在，直接认为需要修改
      if (host == null) {
        effectiveModifiedLabels.put(nodeId, modifiedNodeLabels);
        continue;
      } else if (nodeId.getPort() == WILDCARD_PORT) {
        // 通配符端口，取主机级别标签
        labelsBeforeModification = host.labels;
      } else if (host.nms.get(nodeId) != null) {
        // 取具体节点的标签
        labelsBeforeModification = host.nms.get(nodeId).labels;
      }
      // 比较标签，只有内容不同才加入变更列表
      if (labelsBeforeModification == null
          || labelsBeforeModification.size() != modifiedNodeLabels.size()
          || !labelsBeforeModification.containsAll(modifiedNodeLabels)) {
        effectiveModifiedLabels.put(nodeId, modifiedNodeLabels);
      }
    }
    return effectiveModifiedLabels;
  }

  /*
   * Following methods are used for setting if a node is up and running, and it
   * will update running nodes resource
   */
  /**
   * 激活节点，将节点加入标签资源统计，更新资源映射
   * @param nodeId 节点ID
   * @param resource 节点可用资源
   */
  public void activateNode(NodeId nodeId, Resource resource) {
    writeLock.lock();
    try {
      // 保存修改前节点信息
      Map<String, Host> before = cloneNodeMap(ImmutableSet.of(nodeId));
      
      // 若主机不存在则创建主机条目
      createHostIfNonExisted(nodeId.getHost());
      try {
        // 若节点不存在则创建节点条目
        createNodeIfNonExisted(nodeId);
      } catch (IOException e) {
        LOG.error("This shouldn't happen, cannot get host in nodeCollection"
            + " associated to the node being activated");
        return;
      }

      // 更新节点资源和运行状态
      Node nm = getNMInNodeSet(nodeId);
      nm.resource = resource;
      nm.running = true;

      // 将节点添加到对应标签的节点列表中
      Set<String> labelsForNode = getLabelsByNode(nodeId);
      if (labelsForNode != null) {
        for (String label : labelsForNode) {
          RMNodeLabel labelInfo = labelCollections.get(label);
          if(labelInfo != null) {
            labelInfo.addNodeId(nodeId);
          }
        }
      }
      
      // 保存修改后节点信息
      Map<String, Host> after = cloneNodeMap(ImmutableSet.of(nodeId));
      
      // 更新资源映射
      updateResourceMappings(before, after);
    } finally {
      writeLock.unlock();
    }
  }
  
  /*
   * Following methods are used for setting if a node unregistered to RM
   */
  /**
   * 停用节点，从标签资源统计中移除节点资源
   * @param nodeId 节点ID
   */
  public void deactivateNode(NodeId nodeId) {
    writeLock.lock();
    try {
      // 保存修改前节点信息
      Map<String, Host> before = cloneNodeMap(ImmutableSet.of(nodeId));
      Node nm = getNMInNodeSet(nodeId);
      if (null != nm) {
        if (isNodeLabelExplicit(nm.nodeId)) {
          // When node deactivated, remove the nm from node collection if no
          // labels explicitly set for this particular nm

          // 先保存节点原有标签，后续需要更新标签->节点关系
          Set<String> savedNodeLabels = getLabelsOnNode(nodeId);
          
          // 从节点集合中移除该节点
          nodeCollections.get(nodeId.getHost()).nms.remove(nodeId);
          
          // 从各标签的节点列表中移除该节点
          removeNodeFromLabels(nodeId, savedNodeLabels);
        } else {
          // 显式标签未设置，仅标记为未运行，资源清零
          nm.running = false;
          nm.resource = Resource.newInstance(0, 0);
        }
      }
      
      // 保存修改后节点信息
      Map<String, Host> after = cloneNodeMap(ImmutableSet.of(nodeId));
      
      // 更新资源映射
      updateResourceMappings(before, after);
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * 更新节点资源，先停用再激活以更新资源统计
   * @param node 节点ID
   * @param newResource 新资源值
   */
  public void updateNodeResource(NodeId node, Resource newResource) {
    deactivateNode(node);
    activateNode(node, newResource);
  }

  /**
   * 重新初始化所有队列的标签配置，重新计算各队列可用资源
   * @param queueToLabels 队列->可访问标签映射
   */
  public void reinitializeQueueLabels(Map<String, Set<String>> queueToLabels) {
    writeLock.lock();
    try {
      // 清空原有队列配置
      this.queueCollections.clear();

      // 遍历所有队列重新构建配置
      for (Entry<String, Set<String>> entry : queueToLabels.entrySet()) {
        String queue = entry.getKey();
        Queue q = new Queue();
        this.queueCollections.put(queue, q);

        Set<String> labels = entry.getValue();
        // 包含ANY标签，整个集群资源都可访问，无需累加，后续直接返回集群总资源
        if (labels.contains(ANY)) {
          continue;
        }

        // 添加可访问标签
        q.accessibleNodeLabels.addAll(labels);
        // 遍历所有运行中节点，累加对队列可用的资源
        for (Host host : nodeCollections.values()) {
          for (Entry<NodeId, Node> nentry : host.nms.entrySet()) {
            NodeId nodeId = nentry.getKey();
            Node nm = nentry.getValue();
            if (nm.running && isNodeUsableByQueue(getLabelsByNode(nodeId), q)) {
              Resources.addTo(q.resource, nm.resource);
            }
          }
        }
      }
    } finally {
      writeLock.unlock();
    }
  }
  
  /**
   * 获取队列对应标签的可用资源
   * @param queueName 队列名称
   * @param queueLabels 队列可访问标签
   * @param clusterResource 集群总资源
   * @return 队列可用资源
   */
  public Resource getQueueResource(String queueName, Set<String> queueLabels,
      Resource clusterResource) {
    readLock.lock();
    try {
      // 包含ANY标签，直接返回整个集群资源
      if (queueLabels.contains(ANY)) {
        return clusterResource;
      }
      Queue q = queueCollections.get(queueName);
      if (null == q) {
        return Resources.none();
      }
      return q.resource;
    } finally {
      readLock.unlock();
    }
  }
  
  /*
   * Get active node count based on label.
   */
  /**
   * 获取指定标签对应的活跃节点数量
   * @param label 标签名称
   * @return 活跃节点数
   */
  public int getActiveNMCountPerLabel(String label) {
    if (label == null) {
      return 0;
    }
    readLock.lock();
    try {
      RMNodeLabel labelInfo = labelCollections.get(label);
      return (labelInfo == null) ? 0 : labelInfo.getNumActiveNMs();
    } finally {
      readLock.unlock();
    }
  }

  /**
   * 获取指定节点上的所有标签
   * @param nodeId 节点ID
   * @return 不可修改的标签集合
   */
  public Set<String> getLabelsOnNode(NodeId nodeId) {
    readLock.lock();
    try {
      Set<String> nodeLabels = getLabelsByNode(nodeId);
      return Collections.unmodifiableSet(nodeLabels);
    } finally {
      readLock.unlock();
    }
  }
  
  /**
   * 检查集群是否包含指定标签
   * @param label 标签名称
   * @return 是否包含
   */
  public boolean containsNodeLabel(String label) {
    readLock.lock();
    try {
      return label != null
          && (label.isEmpty() || labelCollections.containsKey(label));
    } finally {
      readLock.unlock();
    }
  }

  /**
   * 复制指定节点集合对应的节点信息，用于对比修改前后状态
   * @param nodesToCopy 需要复制的节点集合
   * @return 复制后的节点信息映射
   */
  private Map<String, Host> cloneNodeMap(Set<NodeId> nodesToCopy) {
    Map<String, Host> map = new HashMap<String, Host>();
    for (NodeId nodeId : nodesToCopy) {
      // 主机未复制过，先复制主机信息
      if (!map.containsKey(nodeId.getHost())) {
        Host originalN = nodeCollections.get(nodeId.getHost());
        if (null == originalN) {