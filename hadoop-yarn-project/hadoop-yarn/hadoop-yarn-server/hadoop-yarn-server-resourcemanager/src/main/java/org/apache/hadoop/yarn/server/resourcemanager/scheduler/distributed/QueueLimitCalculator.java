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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.distributed;

import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.server.api.records.ContainerQueuingLimit;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.distributed.NodeQueueLoadMonitor.LoadComparator;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 分布式调度中节点队列负载限制计算器，结合{@link NodeQueueLoadMonitor}维护所有节点负载指标的均值与标准差，
 * 用于计算节点队列的排队限制阈值，生成容器排队限制配置。
 * 每当{@link NodeQueueLoadMonitor}重新排序所有节点后，会触发本计算器更新统计数据。
 */
public class QueueLimitCalculator {

  /**
   * 存储所有节点负载指标的均值和标准差统计数据，负责更新计算统计值。
   */
  class Stats {
    private final AtomicInteger mean = new AtomicInteger(0);
    private final AtomicInteger stdev = new AtomicInteger(0);

    /**
     * 更新均值和标准差统计，非线程安全，调用方需要对排序节点列表同步。
     */
    void update() {
      // 获取已排序的节点列表
      List<NodeId> sortedNodes = nodeSelector.getSortedNodes();
      if (sortedNodes.size() > 0) {
        // 计算均值
        int sum = 0;
        for (NodeId n : sortedNodes) {
          sum += getMetric(getNode(n));
        }
        mean.set(sum / sortedNodes.size());

        // 计算标准差
        int sqrSumMean = 0;
        for (NodeId n : sortedNodes) {
          int val = getMetric(getNode(n));
          sqrSumMean += Math.pow(val - mean.get(), 2);
        }
        stdev.set(
            (int) Math.round(Math.sqrt(
                sqrSumMean / (float) sortedNodes.size())));
      }
    }

    private ClusterNode getNode(NodeId nId) {
      return nodeSelector.getClusterNodes().get(nId);
    }

    private int getMetric(ClusterNode cn) {
      return (cn != null) ? ((LoadComparator)nodeSelector.getComparator())
              .getMetric(cn) : 0;
    }

    public int getMean() {
      return mean.get();
    }

    public int getStdev() {
      return stdev.get();
    }
  }

  private final NodeQueueLoadMonitor nodeSelector;
  private final float sigma;
  private final int rangeMin;
  private final int rangeMax;
  private final Stats stats = new Stats();

  /**
   * 构造队列限制计算器。
   * @param selector 节点队列负载监控选择器
   * @param sigma 标准差倍数，用于计算阈值 = 均值 + sigma * 标准差
   * @param rangeMin 阈值最小允许值
   * @param rangeMax 阈值最大允许值
   */
  QueueLimitCalculator(NodeQueueLoadMonitor selector, float sigma,
      int rangeMin, int rangeMax) {
    this.nodeSelector = selector;
    this.sigma = sigma;
    this.rangeMax = rangeMax;
    this.rangeMin = rangeMin;
  }

  /**
   * 根据当前统计值计算原始阈值，公式为均值 + sigma * 标准差。
   * @return 原始阈值
   */
  private int determineThreshold() {
    return (int) (stats.getMean() + sigma * stats.getStdev());
  }

  /**
   * 触发统计数据更新，由NodeQueueLoadMonitor调用。
   */
  void update() {
    this.stats.update();
  }

  /**
   * 获取截断后的最终阈值，限制在[rangeMin, rangeMax]范围内。
   * @return 最终阈值
   */
  private int getThreshold() {
    int thres = determineThreshold();
    return Math.min(rangeMax, Math.max(rangeMin, thres));
  }

  /**
   * 根据当前计算结果创建容器排队限制对象，根据负载类型设置对应限制。
   * @return 容器排队限制实例
   */
  public ContainerQueuingLimit createContainerQueuingLimit() {
    ContainerQueuingLimit containerQueuingLimit =
        ContainerQueuingLimit.newInstance();
    // 如果按排队时间排序，设置最大等待时间阈值，禁用队列长度限制
    if (nodeSelector.getComparator() == LoadComparator.QUEUE_WAIT_TIME) {
      containerQueuingLimit.setMaxQueueWaitTimeInMs(getThreshold());
      containerQueuingLimit.setMaxQueueLength(-1);
    } else {
      // 如果按队列长度排序，设置最大队列长度阈值，禁用排队时间限制
      containerQueuingLimit.setMaxQueueWaitTimeInMs(-1);
      containerQueuingLimit.setMaxQueueLength(getThreshold());
    }
    return containerQueuingLimit;
  }
}