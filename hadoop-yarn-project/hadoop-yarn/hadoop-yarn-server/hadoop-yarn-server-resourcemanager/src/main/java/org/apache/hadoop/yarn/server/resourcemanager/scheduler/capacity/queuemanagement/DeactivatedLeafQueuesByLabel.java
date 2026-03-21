// 这个文件已经全部加上中文注释
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.queuemanagement;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacities;
import org.slf4j.Logger;

import java.util.Map;
import java.util.Set;

import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CSQueueUtils.EPSILON;

/**
 * 按节点标签分组管理已停用叶子队列的容量信息，用于自动动态激活队列的容量计算
 */
public class DeactivatedLeafQueuesByLabel {
  // 父队列路径
  private String parentQueuePath;
  // 节点标签
  private String nodeLabel;
  // 已停用叶子队列集合，key为队列路径，value为队列容量信息
  private Map<String, QueueCapacities> deactivatedLeafQueues;
  // 所有已激活子队列的容量总和
  private float sumOfChildQueueActivatedCapacity;
  // 父队列的绝对容量
  private float parentAbsoluteCapacity;
  // 叶子队列模板的绝对容量（用于自动创建/激活队列）
  private float leafQueueTemplateAbsoluteCapacity;
  // 当前可用于激活新队列的可用容量
  private float availableCapacity;
  // 所有已停用队列的总容量
  private float totalDeactivatedCapacity;

  @VisibleForTesting
  public DeactivatedLeafQueuesByLabel() {}

  /**
   * 构造按标签分组的已停用队列管理器，初始化可用容量计算
   * @param deactivatedLeafQueues 已停用叶子队列容量映射
   * @param parentQueuePath 父队列路径
   * @param nodeLabel 节点标签
   * @param sumOfChildQueueActivatedCapacity 已激活子队列容量总和
   * @param parentAbsoluteCapacity 父队列绝对容量
   * @param leafQueueTemplateAbsoluteCapacity 叶子队列模板绝对容量
   */
  public DeactivatedLeafQueuesByLabel(
      Map<String, QueueCapacities> deactivatedLeafQueues,
      String parentQueuePath,
      String nodeLabel,
      float sumOfChildQueueActivatedCapacity,
      float parentAbsoluteCapacity,
      float leafQueueTemplateAbsoluteCapacity) {
    this.parentQueuePath = parentQueuePath;
    this.nodeLabel = nodeLabel;
    this.deactivatedLeafQueues = deactivatedLeafQueues;
    this.sumOfChildQueueActivatedCapacity = sumOfChildQueueActivatedCapacity;
    this.parentAbsoluteCapacity = parentAbsoluteCapacity;
    this.leafQueueTemplateAbsoluteCapacity = leafQueueTemplateAbsoluteCapacity;

    this.totalDeactivatedCapacity = getTotalDeactivatedCapacity();
    this.availableCapacity = parentAbsoluteCapacity - sumOfChildQueueActivatedCapacity +
        this.totalDeactivatedCapacity + EPSILON;
  }

  /**
   * 计算所有已停用队列的总绝对容量
   * @return 所有已停用队列容量总和
   */
  float getTotalDeactivatedCapacity() {
    float deactivatedCapacity = 0;
    // 遍历所有已停用队列累加容量
    for (Map.Entry<String, QueueCapacities> deactivatedQueueCapacity :
        deactivatedLeafQueues.entrySet()) {
      deactivatedCapacity += deactivatedQueueCapacity.getValue().getAbsoluteCapacity(nodeLabel);
    }
    return deactivatedCapacity;
  }

  /**
   * 获取所有已停用队列的路径集合
   * @return 已停用队列路径集合
   */
  public Set<String> getQueues() {
    return deactivatedLeafQueues.keySet();
  }

  /**
   * 打印当前分组状态到调试日志
   * @param logger 日志实例
   */
  public void printToDebug(Logger logger) {
    if (logger.isDebugEnabled()) {
      logger.debug("Parent queue = {}, nodeLabel = {}, absCapacity = {}, " +
              "leafQueueAbsoluteCapacity = {}, deactivatedCapacity = {}, " +
              "absChildActivatedCapacity = {}, availableCapacity = {}",
          parentQueuePath, nodeLabel, parentAbsoluteCapacity,
          leafQueueTemplateAbsoluteCapacity, getTotalDeactivatedCapacity(),
          sumOfChildQueueActivatedCapacity, availableCapacity);
    }
  }

  /**
   * 计算当前可激活的最大叶子队列数量
   * @param numPendingApps 待处理应用数量
   * @return 可激活的最大队列数量
   */
  @VisibleForTesting
  public int getMaxLeavesToBeActivated(int numPendingApps) {
    float childQueueAbsoluteCapacity = leafQueueTemplateAbsoluteCapacity;
    if (childQueueAbsoluteCapacity > 0) {
      // 根据可用容量计算最多可激活的队列数，向下取整
      int numLeafQueuesNeeded = (int) Math.floor(availableCapacity / childQueueAbsoluteCapacity);
      // 不超过待处理应用数量，避免激活过多无用队列
      return Math.min(numLeafQueuesNeeded, numPendingApps);
    }
    return 0;
  }

  /**
   * 检查是否有足够容量激活至少一个新叶子队列
   * @return 是否可以激活队列
   */
  public boolean canActivateLeafQueues() {
    return availableCapacity >= leafQueueTemplateAbsoluteCapacity;
  }

  @VisibleForTesting
  public void setAvailableCapacity(float availableCapacity) {
    this.availableCapacity = availableCapacity;
  }

  @VisibleForTesting
  public void setLeafQueueTemplateAbsoluteCapacity(float leafQueueTemplateAbsoluteCapacity) {
    this.leafQueueTemplateAbsoluteCapacity = leafQueueTemplateAbsoluteCapacity;
  }
}