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

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.util.resource.Resources;

/**
 * 分区队列容量信息数据访问对象，用于YARN ResourceManager Web UI，封装指定节点分区下队列的容量信息
 */
@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class PartitionQueueCapacitiesInfo {
  private String partitionName;

  private QueueCapacityVectorInfo queueCapacityVectorInfo;
  private float capacity;
  private float usedCapacity;
  private float maxCapacity = 100;
  private float absoluteCapacity;
  private float absoluteUsedCapacity;
  private float absoluteMaxCapacity  = 100;
  private float maxAMLimitPercentage;
  private float weight;
  private float normalizedWeight;
  private ResourceInfo configuredMinResource;
  private ResourceInfo configuredMaxResource;
  private ResourceInfo effectiveMinResource;
  private ResourceInfo effectiveMaxResource;

  /**
   * 无参构造函数，用于XML序列化/反序列化
   */
  public PartitionQueueCapacitiesInfo() {
  }

  /**
   * 全参数构造函数，创建分区队列容量信息对象
   * @param partitionName 节点分区名称
   * @param queueCapacityVectorInfo 队列容量向量信息
   * @param capacity 队列容量占比
   * @param usedCapacity 已使用容量占比
   * @param maxCapacity 最大容量占比
   * @param absCapacity 相对于根队列的绝对容量占比
   * @param absUsedCapacity 相对于根队列的绝对已使用容量占比
   * @param absMaxCapacity 相对于根队列的绝对最大容量占比
   * @param maxAMLimitPercentage ApplicationMaster资源最大占比限制
   * @param weight 队列调度权重
   * @param normalizedWeight 标准化后的调度权重
   * @param confMinRes 配置的最小资源量
   * @param confMaxRes 配置的最大资源量
   * @param effMinRes 实际生效的最小资源量
   * @param effMaxRes 实际生效的最大资源量
   */
  public PartitionQueueCapacitiesInfo(String partitionName,
      QueueCapacityVectorInfo queueCapacityVectorInfo,
      float capacity, float usedCapacity, float maxCapacity, float absCapacity,
      float absUsedCapacity, float absMaxCapacity, float maxAMLimitPercentage,
      float weight, float normalizedWeight,
      Resource confMinRes, Resource confMaxRes, Resource effMinRes,
      Resource effMaxRes) {
    super();
    this.queueCapacityVectorInfo = queueCapacityVectorInfo;
    this.partitionName = partitionName;
    this.capacity = capacity;
    this.usedCapacity = usedCapacity;
    this.maxCapacity = maxCapacity;
    this.absoluteCapacity = absCapacity;
    this.absoluteUsedCapacity = absUsedCapacity;
    this.absoluteMaxCapacity = absMaxCapacity;
    this.maxAMLimitPercentage = maxAMLimitPercentage;
    this.weight = weight;
    this.normalizedWeight = normalizedWeight;
    this.configuredMinResource = new ResourceInfo(confMinRes);
    this.configuredMaxResource = new ResourceInfo(confMaxRes);
    this.effectiveMinResource = new ResourceInfo(effMinRes);
    this.effectiveMaxResource = new ResourceInfo(effMaxRes);
  }

  public QueueCapacityVectorInfo getQueueCapacityVectorInfo() {
    return queueCapacityVectorInfo;
  }

  public void setQueueCapacityVectorInfo(QueueCapacityVectorInfo queueCapacityVectorInfo) {
    this.queueCapacityVectorInfo = queueCapacityVectorInfo;
  }

  public float getCapacity() {
    return capacity;
  }

  public void setCapacity(float capacity) {
    this.capacity = capacity;
  }

  public float getUsedCapacity() {
    return usedCapacity;
  }

  public void setUsedCapacity(float usedCapacity) {
    this.usedCapacity = usedCapacity;
  }

  public float getMaxCapacity() {
    return maxCapacity;
  }

  public void setMaxCapacity(float maxCapacity) {
    this.maxCapacity = maxCapacity;
  }

  public String getPartitionName() {
    return partitionName;
  }

  public void setPartitionName(String partitionName) {
    this.partitionName = partitionName;
  }

  public float getAbsoluteCapacity() {
    return absoluteCapacity;
  }

  public void setAbsoluteCapacity(float absoluteCapacity) {
    this.absoluteCapacity = absoluteCapacity;
  }

  public float getAbsoluteUsedCapacity() {
    return absoluteUsedCapacity;
  }

  public void setAbsoluteUsedCapacity(float absoluteUsedCapacity) {
    this.absoluteUsedCapacity = absoluteUsedCapacity;
  }

  public float getAbsoluteMaxCapacity() {
    return absoluteMaxCapacity;
  }

  public void setAbsoluteMaxCapacity(float absoluteMaxCapacity) {
    this.absoluteMaxCapacity = absoluteMaxCapacity;
  }

  public float getMaxAMLimitPercentage() {
    return maxAMLimitPercentage;
  }

  public float getWeight() {
    return weight;
  }

  public void setWeight(float weight) {
    this.weight = weight;
  }

  public float getNormalizedWeight() {
    return normalizedWeight;
  }

  public void setNormalizedWeight(float normalizedWeight) {
    this.normalizedWeight = normalizedWeight;
  }

  public void setMaxAMLimitPercentage(float maxAMLimitPercentage) {
    this.maxAMLimitPercentage = maxAMLimitPercentage;
  }

  public ResourceInfo getConfiguredMinResource() {
    return configuredMinResource;
  }

  /**
   * 获取配置的最大资源量，空资源或未配置时返回null
   * @return 配置的最大资源量信息，未配置则返回null
   */
  public ResourceInfo getConfiguredMaxResource() {
    if (configuredMaxResource == null
        || configuredMaxResource.getResource().equals(Resources.none())) {
      return null;
    }
    return configuredMaxResource;
  }

  public ResourceInfo getEffectiveMinResource() {
    return effectiveMinResource;
  }

  public ResourceInfo getEffectiveMaxResource() {
    return effectiveMaxResource;
  }
}