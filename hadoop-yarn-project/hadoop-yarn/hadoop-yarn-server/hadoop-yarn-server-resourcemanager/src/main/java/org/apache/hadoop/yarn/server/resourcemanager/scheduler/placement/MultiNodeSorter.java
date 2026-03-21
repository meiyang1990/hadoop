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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.concurrent.SubjectInheritingThread;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.AbstractYarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;

import org.apache.hadoop.classification.VisibleForTesting;

/**
 * YARN资源调度节点排序公共服务，根据指定策略对集群节点进行排序，
 * 为应用分配容器时提供有序节点列表，提升调度性能和数据局部性。
 * @param <N> 继承自SchedulerNode的节点类型
 */
public class MultiNodeSorter<N extends SchedulerNode> extends AbstractService {

  // 实际执行节点排序的策略实例
  private MultiNodeLookupPolicy<N> multiNodePolicy;
  private static final Logger LOG =
      LoggerFactory.getLogger(MultiNodeSorter.class);

  // 定期执行节点重排序的定时线程池
  private ScheduledExecutorService ses;
  // 定时任务处理器，用于取消任务
  private ScheduledFuture<?> handler;
  // 服务停止标记
  private volatile boolean stopped;
  // RM上下文，获取集群节点等信息
  private RMContext rmContext;
  // 排序策略配置，包含策略类名和重排序间隔
  private MultiNodePolicySpec policySpec;

  /**
   * 构造多节点排序器实例。
   * @param rmContext ResourceManager上下文
   * @param policy 排序策略配置
   */
  public MultiNodeSorter(RMContext rmContext,
      MultiNodePolicySpec policy) {
    super("MultiNodeLookupPolicy");
    this.rmContext = rmContext;
    this.policySpec = policy;
  }

  @VisibleForTesting
  public synchronized MultiNodeLookupPolicy<N> getMultiNodeLookupPolicy() {
    return multiNodePolicy;
  }

  /**
   * 服务初始化方法，加载并初始化节点排序策略。
   * @param conf Hadoop配置
   * @throws Exception 初始化失败时抛出异常
   */
  public void serviceInit(Configuration conf) throws Exception {
    LOG.info("Initializing MultiNodeSorter=" + policySpec.getPolicyClassName()
        + ", with sorting interval=" + policySpec.getSortingInterval());
    initPolicy(policySpec.getPolicyClassName());
    super.serviceInit(conf);
  }

  @SuppressWarnings("unchecked")
  void initPolicy(String policyName) throws YarnException {
    Class<?> policyClass;
    try {
      // 加载策略类
      policyClass = Class.forName(policyName);
    } catch (ClassNotFoundException e) {
      throw new YarnException(
          "Invalid policy name:" + policyName + e.getMessage());
    }
    // 通过反射实例化策略对象
    this.multiNodePolicy = (MultiNodeLookupPolicy<N>) ReflectionUtils
        .newInstance(policyClass, null);
  }

  @Override
  public void serviceStart() throws Exception {
    LOG.info("Starting SchedulingMonitor=" + getName());
    assert !stopped : "starting when already stopped";
    // 创建单线程定时线程池，使用继承访问主体的线程工厂
    ses = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
      public Thread newThread(Runnable r) {
        Thread t = new SubjectInheritingThread(r);
        t.setName(getName());
        return t;
      }
    });

    // 仅当排序间隔为正整数时启动定时重排序线程
    if(policySpec.getSortingInterval() != 0) {
      handler = ses.scheduleAtFixedRate(new SortingThread(),
          0, policySpec.getSortingInterval(), TimeUnit.MILLISECONDS);
    }
    super.serviceStart();
  }

  @Override
  public void serviceStop() throws Exception {
    stopped = true;
    if (handler != null) {
      LOG.info("Stop " + getName());
      // 取消定时任务
      handler.cancel(true);
      // 关闭线程池
      ses.shutdown();
    }
    super.serviceStop();
  }

  @SuppressWarnings("unchecked")
  @VisibleForTesting
  /**
   * 对所有分区（节点标签）的集群节点重新执行排序。
   */
  public void reSortClusterNodes() {
    // 获取所有集群节点标签
    Set<String> nodeLabels = new HashSet<>();
    nodeLabels
        .addAll(rmContext.getNodeLabelManager().getClusterNodeLabelNames());
    // 添加无标签分区
    nodeLabels.add(RMNodeLabelsManager.NO_LABEL);
    // 遍历每个标签分区
    for (String label : nodeLabels) {
      Map<NodeId, SchedulerNode> nodesByPartition = new HashMap<>();
      // 获取该分区下的所有节点
      List<SchedulerNode> nodes = ((AbstractYarnScheduler) rmContext
          .getScheduler()).getNodeTracker().getNodesPerPartition(label);
      if (nodes != null) {
        // 转换为节点ID映射
        nodes.forEach(n -> nodesByPartition.put(n.getNodeID(), n));
        // 通知排序策略更新该分区的节点集合，重新排序
        multiNodePolicy.addAndRefreshNodesSet(
            (Collection<N>) nodesByPartition.values(), label);
      }
    }
  }

  /**
   * 定时重排序执行线程，定期触发全部分区节点重排序。
   */
  private class SortingThread implements Runnable {
    @Override
    public void run() {
      try {
        reSortClusterNodes();
      } catch (Throwable t) {
        // 排序过程异常不影响后续执行，仅记录日志后跳过本次执行
        LOG.error("Exception raised while executing multinode"
            + " sorter, skip this run..., exception=", t);
      }
    }
  }

  /**
   * 检查排序线程是否正在运行。
   *
   * @return true 排序线程正在运行，false 未启动或已停止
   */
  public boolean isSorterThreadRunning() {
    return (handler != null);
  }
}