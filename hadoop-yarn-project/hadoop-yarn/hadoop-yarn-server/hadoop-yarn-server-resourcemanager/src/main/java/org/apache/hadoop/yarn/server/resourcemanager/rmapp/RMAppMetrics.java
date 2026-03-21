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

package org.apache.hadoop.yarn.server.resourcemanager.rmapp;

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.server.resourcemanager.RMServerUtils;

import java.util.Map;

/**
 * RM应用指标容器，存储应用相关的资源抢占、资源使用累计等指标数据。
 */
public class RMAppMetrics {
  final Resource resourcePreempted;
  final int numNonAMContainersPreempted;
  final int numAMContainersPreempted;
  private final Map<String, Long> resourceSecondsMap;
  private final Map<String, Long> preemptedResourceSecondsMap;
  private int totalAllocatedContainers;

  /**
   * 构造RM应用指标对象，存储应用各项监控指标。
   * @param resourcePreempted 被抢占的总资源量
   * @param numNonAMContainersPreempted 被抢占的非ApplicationMaster容器数量
   * @param numAMContainersPreempted 被抢占的ApplicationMaster容器数量
   * @param resourceSecondsMap 各类资源累计使用秒数映射（key为资源名称，value为累计秒数）
   * @param preemptedResourceSecondsMap 被抢占资源累计秒数映射（key为资源名称，value为累计秒数）
   * @param totalAllocatedContainers 累计分配容器总数
   */
  public RMAppMetrics(Resource resourcePreempted,
      int numNonAMContainersPreempted, int numAMContainersPreempted,
      Map<String, Long> resourceSecondsMap,
      Map<String, Long> preemptedResourceSecondsMap,
      int totalAllocatedContainers) {
    this.resourcePreempted = resourcePreempted;
    this.numNonAMContainersPreempted = numNonAMContainersPreempted;
    this.numAMContainersPreempted = numAMContainersPreempted;
    this.resourceSecondsMap = resourceSecondsMap;
    this.preemptedResourceSecondsMap = preemptedResourceSecondsMap;
    this.totalAllocatedContainers = totalAllocatedContainers;
  }

  public Resource getResourcePreempted() {
    return resourcePreempted;
  }

  public int getNumNonAMContainersPreempted() {
    return numNonAMContainersPreempted;
  }

  public int getNumAMContainersPreempted() {
    return numAMContainersPreempted;
  }

  /**
   * 获取内存累计使用兆秒数（MB*秒）。
   * @return 内存累计兆秒数
   */
  public long getMemorySeconds() {
    return RMServerUtils.getOrDefault(resourceSecondsMap,
        ResourceInformation.MEMORY_MB.getName(), 0L);
  }

  /**
   * 获取vcore累计使用核秒数（核*秒）。
   * @return vcore累计核秒数
   */
  public long getVcoreSeconds() {
    return RMServerUtils
        .getOrDefault(resourceSecondsMap, ResourceInformation.VCORES.getName(),
            0L);
  }

  /**
   * 获取被抢占内存累计兆秒数。
   * @return 被抢占内存累计兆秒数
   */
  public long getPreemptedMemorySeconds() {
    return RMServerUtils.getOrDefault(preemptedResourceSecondsMap,
        ResourceInformation.MEMORY_MB.getName(), 0L);
  }

  /**
   * 获取被抢占vcore累计核秒数。
   * @return 被抢占vcore累计核秒数
   */
  public long getPreemptedVcoreSeconds() {
    return RMServerUtils.getOrDefault(preemptedResourceSecondsMap,
        ResourceInformation.VCORES.getName(), 0L);
  }

  public Map<String, Long> getResourceSecondsMap() {
    return resourceSecondsMap;
  }

  public Map<String, Long> getPreemptedResourceSecondsMap() {
    return preemptedResourceSecondsMap;
  }

  public int getTotalAllocatedContainers() {
    return totalAllocatedContainers;
  }
}