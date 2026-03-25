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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeLabel;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;

import org.apache.hadoop.classification.VisibleForTesting;

/**
 * 周期性为ResourceManager更新节点标签映射。它从{@link RMNodeLabelsMappingProvider}收集节点标签，
 * 并通过{@link RMNodeLabelsManager}更新节点到标签的映射关系。
 * 该服务仅在配置"yarn.node-labels.configuration-type"设置为"delegated-centralized"时启用。
 */
public class RMDelegatedNodeLabelsUpdater extends CompositeService {

  private static final Logger LOG = LoggerFactory
      .getLogger(RMDelegatedNodeLabelsUpdater.class);

  /** 禁用全量节点标签更新的标识 */
  public static final long DISABLE_DELEGATED_NODE_LABELS_UPDATE = -1;

  // 用于调度节点标签获取任务的定时器
  private Timer nodeLabelsScheduler;
  // 新注册节点标签更新间隔，默认30秒
  @VisibleForTesting
  public long nodeLabelsUpdateInterval;

  // 待更新标签的新注册节点集合
  private Set<NodeId> newlyRegisteredNodes = new HashSet<NodeId>();
  // 保护新注册节点集合的锁对象
  private Object lock = new Object();
  // 上次全量更新节点标签的时间戳
  private long lastAllNodesLabelUpdateMills = 0L;
  // 全量节点标签更新间隔
  private long allNodesLabelUpdateInterval;

  // 节点标签映射提供器实例
  private RMNodeLabelsMappingProvider rmNodeLabelsMappingProvider;

  // RM上下文对象
  private RMContext rmContext;

  /**
   * 构造函数，初始化节点标签更新器。
   * @param rmContext ResourceManager上下文
   */
  public RMDelegatedNodeLabelsUpdater(RMContext rmContext) {
    super("RMDelegatedNodeLabelsUpdater");
    this.rmContext = rmContext;
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    // 从配置读取全量节点标签更新间隔
    allNodesLabelUpdateInterval = conf.getLong(
        YarnConfiguration.RM_NODE_LABELS_PROVIDER_FETCH_INTERVAL_MS,
        YarnConfiguration.DEFAULT_RM_NODE_LABELS_PROVIDER_FETCH_INTERVAL_MS);
    // 从配置读取新注册节点标签更新间隔
    nodeLabelsUpdateInterval =
        conf.getLong(YarnConfiguration.RM_NODE_LABELS_PROVIDER_UPDATE_NEWLY_REGISTERED_INTERVAL_MS,
            YarnConfiguration.DEFAULT_RM_NODE_LABELS_PROVIDER_UPDATE_NEWLY_REGISTERED_INTERVAL_MS);
    // 创建节点标签映射提供器实例
    rmNodeLabelsMappingProvider = createRMNodeLabelsMappingProvider(conf);
    // 将提供器添加为子服务进行管理
    addService(rmNodeLabelsMappingProvider);
    super.serviceInit(conf);
  }

  @Override
  protected void serviceStart() throws Exception {
    // 创建后台定时器线程
    nodeLabelsScheduler = new Timer(
        "RMDelegatedNodeLabelsUpdater-Timer", true);
    // 创建定时更新任务
    TimerTask delegatedNodeLabelsUpdaterTimerTask =
        new RMDelegatedNodeLabelsUpdaterTimerTask();
    // 按固定速率调度定时更新任务
    nodeLabelsScheduler.scheduleAtFixedRate(
        delegatedNodeLabelsUpdaterTimerTask,
        nodeLabelsUpdateInterval,
        nodeLabelsUpdateInterval);

    super.serviceStart();
  }

  /**
   * 终止定时器，停止服务。
   *
   * @throws Exception 停止过程中发生异常
   */
  @Override
  protected void serviceStop() throws Exception {
    if (nodeLabelsScheduler != null) {
      nodeLabelsScheduler.cancel();
    }
    super.serviceStop();
  }

  /**
   * 定时更新节点标签的任务类
   */
  private class RMDelegatedNodeLabelsUpdaterTimerTask extends TimerTask {
    @Override
    public void run() {
      Set<NodeId> nodesToUpdateLabels = null;
      boolean isUpdatingAllNodes = false;

      // 如果全量更新未禁用，检查是否需要执行全量更新
      if (allNodesLabelUpdateInterval != DISABLE_DELEGATED_NODE_LABELS_UPDATE) {
        long elapsedTimeSinceLastUpdate =
            System.currentTimeMillis() - lastAllNodesLabelUpdateMills;
        // 距离上次全量更新超过间隔，触发全量更新
        if (elapsedTimeSinceLastUpdate > allNodesLabelUpdateInterval) {
          nodesToUpdateLabels =
              Collections.unmodifiableSet(rmContext.getRMNodes().keySet());
          isUpdatingAllNodes = true;
        }
      }

      // 不需要全量更新，且存在待更新的新注册节点，处理增量更新
      if (nodesToUpdateLabels == null && !newlyRegisteredNodes.isEmpty()) {
        synchronized (lock) {
          if (!newlyRegisteredNodes.isEmpty()) {
            nodesToUpdateLabels = new HashSet<NodeId>(newlyRegisteredNodes);
          }
        }
      }

      try {
        // 存在待更新节点，执行更新逻辑
        if (nodesToUpdateLabels != null && !nodesToUpdateLabels.isEmpty()) {
          updateNodeLabelsInternal(nodesToUpdateLabels);
          // 如果是全量更新，更新上次全量更新时间
          if (isUpdatingAllNodes) {
            lastAllNodesLabelUpdateMills = System.currentTimeMillis();
          }
          // 从待更新集合中移除已经处理过的节点
          synchronized (lock) {
            newlyRegisteredNodes.removeAll(nodesToUpdateLabels);
          }
        }
      } catch (IOException e) {
        LOG.error("Failed to update node Labels", e);
      }
    }
  }

  /**
   * 内部方法，执行实际的节点标签更新
   * @param nodes 待更新标签的节点集合
   * @throws IOException 获取或更新标签失败时抛出
   */
  private void updateNodeLabelsInternal(Set<NodeId> nodes)
      throws IOException {
    // 从标签提供器获取指定节点的最新标签
    Map<NodeId, Set<NodeLabel>> labelsUpdated =
        rmNodeLabelsMappingProvider.getNodeLabels(nodes);
    // 如果存在更新，转换格式后更新到节点标签管理器
    if (labelsUpdated != null && labelsUpdated.size() != 0) {
      Map<NodeId, Set<String>> nodeToLabels =
          new HashMap<NodeId, Set<String>>(labelsUpdated.size());
      for (Map.Entry<NodeId, Set<NodeLabel>> entry
          : labelsUpdated.entrySet()) {
        nodeToLabels.put(entry.getKey(),
            NodeLabelsUtils.convertToStringSet(entry.getValue()));
      }
      // 替换节点上的标签映射
      rmContext.getNodeLabelManager().replaceLabelsOnNode(nodeToLabels);
    }
  }

  /**
   * 根据配置创建节点标签映射提供器实例。
   * @param conf 配置对象
   * @return 创建好的提供器实例
   * @throws IOException 创建失败时抛出
   */
  private RMNodeLabelsMappingProvider createRMNodeLabelsMappingProvider(
      Configuration conf) throws IOException {
    RMNodeLabelsMappingProvider nodeLabelsMappingProvider = null;
    try {
      // 从配置读取提供器实现类
      Class<? extends RMNodeLabelsMappingProvider> labelsProviderClass =
          conf.getClass(YarnConfiguration.RM_NODE_LABELS_PROVIDER_CONFIG,
              null, RMNodeLabelsMappingProvider.class);
      if (labelsProviderClass != null) {
        // 反射实例化提供器
        nodeLabelsMappingProvider = labelsProviderClass.newInstance();
      }
    } catch (InstantiationException | IllegalAccessException
        | RuntimeException e) {
      LOG.error("Failed to create RMNodeLabelsMappingProvider based on"
          + " Configuration", e);
      throw new IOException("Failed to create RMNodeLabelsMappingProvider : "
          + e.getMessage(), e);
    }

    // 未配置提供器类，抛出异常
    if (nodeLabelsMappingProvider == null) {
      String msg = "RMNodeLabelsMappingProvider should be configured when "
          + "delegated-centralized node label configuration is enabled";
      LOG.error(msg);
      throw new IOException(msg);
    } else {
      LOG.debug("RM Node labels mapping provider class is : {}",
          nodeLabelsMappingProvider.getClass());
    }

    return nodeLabelsMappingProvider;
  }

  /**
   * 添加指定节点到待更新标签队列，当新节点注册时调用。
   * @param node 需要更新标签的节点ID
   */
  public void updateNodeLabels(NodeId node) {
    synchronized (lock) {
      newlyRegisteredNodes.add(node);
    }
  }
}