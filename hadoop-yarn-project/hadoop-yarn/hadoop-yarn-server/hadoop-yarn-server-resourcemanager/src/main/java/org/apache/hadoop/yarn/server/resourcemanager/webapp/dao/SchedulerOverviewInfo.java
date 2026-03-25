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

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.yarn.api.records.ResourceTypeInfo;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo.FifoScheduler;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.List;

/**
 * 调度器概览信息数据访问对象，为YARN ResourceManager Web UI提供调度器整体统计信息
 */
@XmlRootElement(name = "scheduler")
@XmlAccessorType(XmlAccessType.FIELD)
public class SchedulerOverviewInfo {

  private String schedulerType;
  private String schedulingResourceType;
  private ResourceInfo minimumAllocation;
  private ResourceInfo maximumAllocation;
  private int applicationPriority;
  private int schedulerBusy;
  private int rmDispatcherEventQueueSize;
  private int schedulerDispatcherEventQueueSize;

  // JAXB needs this
  public SchedulerOverviewInfo() {

  }

  /**
   * 从资源调度器实例构造调度器概览信息
   * @param rs 资源调度器实例
   */
  public SchedulerOverviewInfo(ResourceScheduler rs) {
    // 解析调度器类型
    this.schedulerType = getSchedulerName(rs);

    // 解析分配资源限制信息
    this.minimumAllocation = new ResourceInfo(rs.getMinimumResourceCapability());
    this.maximumAllocation = new ResourceInfo(rs.getMaximumResourceCapability());

    // 解析应用优先级上限
    this.applicationPriority = rs.getMaxClusterLevelAppPriority().getPriority();

    // 解析资源类型信息
    List<ResourceTypeInfo> resourceTypeInfos = ResourceUtils.getResourcesTypeInfo();
    // 按名称不区分大小写排序
    resourceTypeInfos.sort((o1, o2) -> o1.getName().compareToIgnoreCase(o2.getName()));
    // 将所有资源类型拼接为逗号分隔字符串
    this.schedulingResourceType = StringUtils.join(resourceTypeInfos, ",");

    // 获取集群指标并提取调度器相关指标
    ClusterMetricsInfo clusterMetrics = new ClusterMetricsInfo(rs);
    this.schedulerBusy = clusterMetrics.getRmSchedulerBusyPercent();
    this.rmDispatcherEventQueueSize = clusterMetrics.getRmEventQueueSize();
    this.schedulerDispatcherEventQueueSize = clusterMetrics.getSchedulerEventQueueSize();
  }

  /**
   * 根据调度器实例获取调度器显示名称
   * @param rs 资源调度器实例
   * @return 调度器显示名称
   */
  private static String getSchedulerName(ResourceScheduler rs) {
    if (rs instanceof CapacityScheduler) {
      return "Capacity Scheduler";
    }
    if (rs instanceof FairScheduler) {
      return "Fair Scheduler";
    }
    if (rs instanceof FifoScheduler) {
      return "Fifo Scheduler";
    }
    return rs.getClass().getSimpleName();
  }

  public String getSchedulerType() {
    return schedulerType;
  }

  public String getSchedulingResourceType() {
    return schedulingResourceType;
  }

  public ResourceInfo getMinimumAllocation() {
    return minimumAllocation;
  }

  public ResourceInfo getMaximumAllocation() {
    return maximumAllocation;
  }

  public int getApplicationPriority() {
    return applicationPriority;
  }

  public int getSchedulerBusy() {
    return schedulerBusy;
  }

  public int getRmDispatcherEventQueueSize() {
    return rmDispatcherEventQueueSize;
  }

  public int getSchedulerDispatcherEventQueueSize() {
    return schedulerDispatcherEventQueueSize;
  }
}