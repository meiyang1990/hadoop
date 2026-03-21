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
package org.apache.hadoop.yarn.server.resourcemanager.webapp.dao;

import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueResourceQuotas;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacities;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacityVector;

/**
 * YARN RM Web UI 数据访问对象，封装队列按节点标签分区划分的容量信息
 */
@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class QueueCapacitiesInfo {
  // 按分区存储的队列容量信息列表
  protected List<PartitionQueueCapacitiesInfo> queueCapacitiesByPartition =
      new ArrayList<>();

  public QueueCapacitiesInfo() {
  }

  /**
   * 从容量调度器队列构造队列容量信息对象
   * @param queue 容量调度器队列对象
   * @param considerAMUsage 是否考虑应用 master 资源占用
   */
  public QueueCapacitiesInfo(CSQueue queue, boolean considerAMUsage) {
    // 获取队列容量信息对象
    QueueCapacities capacities = queue.getQueueCapacities();
    // 获取队列资源配额对象
    QueueResourceQuotas resourceQuotas = queue.getQueueResourceQuotas();
    if (capacities == null) {
      return;
    }
    QueueCapacityVectorInfo queueCapacityVectorInfo;
    float capacity;
    float usedCapacity;
    float maxCapacity;
    float absCapacity;
    float absUsedCapacity;
    float absMaxCapacity;
    float maxAMLimitPercentage;
    float weight;
    float normalizedWeight;
    // 遍历所有存在的节点标签分区
    for (String partitionName : capacities.getExistingNodeLabels()) {
      // 获取该分区配置的容量向量
      QueueCapacityVector queueCapacityVector = queue.getConfiguredCapacityVector(partitionName);
      // 构造容量向量DTO，处理空值情况
      queueCapacityVectorInfo = queueCapacityVector == null ?
              new QueueCapacityVectorInfo(new QueueCapacityVector()) :
              new QueueCapacityVectorInfo(queue.getConfiguredCapacityVector(partitionName));
      // 计算已用容量百分比，转换为百分比格式
      usedCapacity = capacities.getUsedCapacity(partitionName) * 100;
      // 计算配置容量百分比，转换为百分比格式
      capacity = capacities.getCapacity(partitionName) * 100;
      // 获取最大容量比例
      maxCapacity = capacities.getMaximumCapacity(partitionName);
      // 限制绝对容量在0-1范围内，转换为百分比格式
      absCapacity = CapacitySchedulerQueueInfo
          .cap(capacities.getAbsoluteCapacity(partitionName), 0f, 1f) * 100;
      // 限制绝对已用容量在0-1范围内，转换为百分比格式
      absUsedCapacity = CapacitySchedulerQueueInfo
          .cap(capacities.getAbsoluteUsedCapacity(partitionName), 0f, 1f) * 100;
      // 限制绝对最大容量在0-1范围内，转换为百分比格式
      absMaxCapacity = CapacitySchedulerQueueInfo.cap(
          capacities.getAbsoluteMaximumCapacity(partitionName), 0f, 1f) * 100;
      // 计算AM资源限制百分比，转换为百分比格式
      maxAMLimitPercentage = capacities
          .getMaxAMResourcePercentage(partitionName) * 100;
      // 校正最大容量范围，确保在0-1之间
      if (maxCapacity < CapacitySchedulerQueueInfo.EPSILON || maxCapacity > 1f)
        maxCapacity = 1f;
      // 转换为百分比格式
      maxCapacity = maxCapacity * 100;
      // 获取队列权重
      weight = capacities.getWeight(partitionName);
      // 获取归一化后权重
      normalizedWeight = capacities.getNormalizedWeight(partitionName);
      // 添加当前分区容量信息到列表
      queueCapacitiesByPartition.add(new PartitionQueueCapacitiesInfo(
          partitionName, queueCapacityVectorInfo, capacity, usedCapacity, maxCapacity, absCapacity,
          absUsedCapacity, absMaxCapacity,
          considerAMUsage ? maxAMLimitPercentage : 0f,
          weight, normalizedWeight,
          resourceQuotas.getConfiguredMinResource(partitionName),
          resourceQuotas.getConfiguredMaxResource(partitionName),
          resourceQuotas.getEffectiveMinResource(partitionName),
          resourceQuotas.getEffectiveMaxResource(partitionName)));
    }
  }

  /**
   * 添加一个分区容量信息到列表
   * @param partitionQueueCapacitiesInfo 分区容量信息对象
   */
  public void add(PartitionQueueCapacitiesInfo partitionQueueCapacitiesInfo) {
    queueCapacitiesByPartition.add(partitionQueueCapacitiesInfo);
  }

  public List<PartitionQueueCapacitiesInfo> getQueueCapacitiesByPartition() {
    return queueCapacitiesByPartition;
  }

  public void setQueueCapacitiesByPartition(
      List<PartitionQueueCapacitiesInfo> capacities) {
    this.queueCapacitiesByPartition = capacities;
  }

  /**
   * 根据分区名称获取对应分区容量信息
   * @param partitionName 节点标签分区名称
   * @return 对应分区容量信息，未找到返回空对象
   */
  public PartitionQueueCapacitiesInfo getPartitionQueueCapacitiesInfo(
      String partitionName) {
    for (PartitionQueueCapacitiesInfo partitionQueueCapacitiesInfo : queueCapacitiesByPartition) {
      if (partitionQueueCapacitiesInfo.getPartitionName()
          .equals(partitionName)) {
        return partitionQueueCapacitiesInfo;
      }
    }
    return new PartitionQueueCapacitiesInfo();
  }
}