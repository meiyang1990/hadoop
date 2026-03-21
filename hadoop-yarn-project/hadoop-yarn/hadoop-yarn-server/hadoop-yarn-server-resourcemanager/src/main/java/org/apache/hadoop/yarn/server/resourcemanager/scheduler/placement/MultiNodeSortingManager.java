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

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.collections4.IteratorUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerUtils;

/**
 * 多节点排序管理器，负责管理所有节点排序线程和排序策略，为应用分配容器提供排序后的节点列表
 * @param <N> 继承自SchedulerNode
 */
public class MultiNodeSortingManager<N extends SchedulerNode>
    extends AbstractService {

  private static final Logger LOG = LoggerFactory
      .getLogger(MultiNodeSortingManager.class);

  // RM上下文对象，提供集群信息访问
  private RMContext rmContext;
  // 保存正在运行的排序器实例，key为策略类名
  private Map<String, MultiNodeSorter<N>> runningMultiNodeSorters;
  // 所有已注册的节点排序策略配置
  private Set<MultiNodePolicySpec> policySpecs = new HashSet<MultiNodePolicySpec>();
  // YARN配置对象
  private Configuration conf;
  // 是否启用多节点 placement 功能
  private boolean multiNodePlacementEnabled;
  // 跳过失联节点的心跳间隔阈值
  private long skipNodeInterval;

  /**
   * 构造函数，初始化节点排序管理器
   */
  public MultiNodeSortingManager() {
    super("MultiNodeSortingManager");
    this.runningMultiNodeSorters = new ConcurrentHashMap<>();
  }

  @Override
  public void serviceInit(Configuration configuration) throws Exception {
    LOG.info("Initializing NodeSortingService=" + getName());
    super.serviceInit(configuration);
    this.conf = configuration;
    // 从配置中读取跳过失联节点的间隔阈值
    this.skipNodeInterval = YarnConfiguration.getSkipNodeInterval(conf);
  }

  @Override
  public void serviceStart() throws Exception {
    LOG.info("Starting NodeSortingService=" + getName());
    // 创建并启动所有已注册的排序策略
    createAllPolicies();
    super.serviceStart();
  }

  @Override
  public void serviceStop() throws Exception {
    // 停止所有正在运行的排序器
    for (MultiNodeSorter<N> sorter : runningMultiNodeSorters.values()) {
      sorter.stop();
    }
    super.serviceStop();
  }

  /**
   * 创建并启动所有已注册的节点排序策略实例
   */
  private void createAllPolicies() {
    // 多节点placement未启用则直接返回
    if (!multiNodePlacementEnabled) {
      return;
    }
    // 遍历所有策略配置，逐个创建排序器
    for (MultiNodePolicySpec policy : policySpecs) {
      MultiNodeSorter<N> mon = new MultiNodeSorter<N>(rmContext, policy);
      mon.init(conf);
      mon.start();
      runningMultiNodeSorters.put(policy.getPolicyClassName(), mon);
    }
  }

  /**
   * 根据策略名称获取已启动的多节点排序器实例
   * @param name 策略类名称
   * @return 排序器实例，不存在则返回null
   */
  public MultiNodeSorter<N> getMultiNodePolicy(String name) {
    return runningMultiNodeSorters.get(name);
  }

  /**
   * 设置RM上下文对象
   * @param context RM上下文
   */
  public void setRMContext(RMContext context) {
    this.rmContext = context;
  }

  /**
   * 注册多节点排序策略配置
   * @param isMultiNodePlacementEnabled 是否启用多节点placement
   * @param multiNodePlacementPolicies 多节点排序策略配置集合
   */
  public void registerMultiNodePolicyNames(
      boolean isMultiNodePlacementEnabled,
      Set<MultiNodePolicySpec> multiNodePlacementPolicies) {
    this.policySpecs.addAll(multiNodePlacementPolicies);
    this.multiNodePlacementEnabled = isMultiNodePlacementEnabled;
    LOG.info("MultiNode scheduling is '" + multiNodePlacementEnabled +
        "', and configured policies are " + StringUtils
        .join(policySpecs.iterator(), ","));
  }

  /**
   * 获取经过排序和过滤的节点迭代器，供容器分配时选择节点
   * @param nodes 候选节点集合
   * @param partition 节点分区
   * @param policyName 使用的排序策略名称
   * @return 排序过滤后的节点迭代器
   */
  public Iterator<N> getMultiNodeSortIterator(Collection<N> nodes,
      String partition, String policyName) {
    // nodeLookupPolicy can be null if app is configured with invalid policy.
    // in such cases, use the the first node.
    // 策略名称为空，说明配置无效，返回仅包含第一个节点的迭代器
    if(policyName == null) {
      LOG.warn("Multi Node scheduling is enabled, however invalid class is"
          + " configured. Valid sorting policy has to be configured in"
          + " yarn.scheduler.capacity.<queue>.multi-node-sorting.policy");
      return IteratorUtils.singletonIterator(
          nodes.iterator().next());
    }

    MultiNodeSorter multiNodeSorter = getMultiNodePolicy(policyName);
    // 找不到对应排序器，说明全局多节点placement未启用，返回仅包含第一个节点的迭代器
    if (multiNodeSorter == null) {
      LOG.warn(
          "MultiNode policy '" + policyName + "' is configured, however " +
              "yarn.scheduler.capacity.multi-node-placement-enabled is false");
      return IteratorUtils.singletonIterator(
          nodes.iterator().next());
    }

    // 获取策略对应的节点查找策略
    MultiNodeLookupPolicy<N> policy = multiNodeSorter
        .getMultiNodeLookupPolicy();
    // If sorter thread is not running, refresh node set.
    // 排序线程未运行时，手动刷新节点集合
    if (!multiNodeSorter.isSorterThreadRunning()) {
      policy.addAndRefreshNodesSet(nodes, partition);
    }

    // 获取策略排序后的首选节点迭代器
    Iterator<N> nodesIterator = policy.getPreferredNodeIterator(nodes,
        partition);

    // Skip node which missed YarnConfiguration.SCHEDULER_SKIP_NODE_MULTIPLIER
    // heartbeats since the node might be dead and we should not continue
    // allocate containers on that.
    // 包装迭代器，过滤掉长时间未上报心跳的失联节点
    Iterator<N> filteringIterator = new Iterator() {
      private N cached;
      private boolean hasCached;
      @Override
      public boolean hasNext() {
        if (hasCached) {
          return true;
        }
        // 遍历查找下一个健康节点
        while (nodesIterator.hasNext()) {
          cached = nodesIterator.next();
          // 检查节点最近是否上报过心跳
          if (SchedulerUtils.isNodeHeartbeated(cached, skipNodeInterval)) {
            hasCached = true;
            return true;
          }
        }
        return false;
      }

      @Override
      public N next() {
        if (hasCached) {
          hasCached = false;
          return cached;
        }
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        return next();
      }
    };
    return filteringIterator;
  }
}