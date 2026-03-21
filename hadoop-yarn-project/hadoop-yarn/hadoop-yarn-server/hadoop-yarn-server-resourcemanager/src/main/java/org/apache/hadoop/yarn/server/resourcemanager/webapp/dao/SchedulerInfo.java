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

import java.util.Arrays;
import java.util.EnumSet;

import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlSeeAlso;

import org.apache.hadoop.yarn.proto.YarnServiceProtos.SchedulerResourceTypes;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo.FifoScheduler;

/**
 * YARN ResourceManager Web UI 调度器基本信息数据访问对象
 * 用于封装不同调度器的通用信息，支持JAXB序列化返回给前端
 */
@XmlRootElement
@XmlSeeAlso({ CapacitySchedulerInfo.class, FairSchedulerInfo.class,
  FifoSchedulerInfo.class })
public class SchedulerInfo {
  protected String schedulerName;
  protected ResourceInfo minAllocResource;
  protected ResourceInfo maxAllocResource;
  protected EnumSet<SchedulerResourceTypes> schedulingResourceTypes;
  protected int maximumClusterPriority;

  // JAXB needs this
  public SchedulerInfo() {
  }

  /**
   * 从ResourceManager中构造调度器基础信息对象
   * @param rm ResourceManager实例
   */
  public SchedulerInfo(final ResourceManager rm) {
    // 获取当前ResourceManager使用的资源调度器实例
    ResourceScheduler rs = rm.getResourceScheduler();

    // 根据调度器类型设置显示名称
    if (rs instanceof CapacityScheduler) {
      this.schedulerName = "Capacity Scheduler";
    } else if (rs instanceof FairScheduler) {
      this.schedulerName = "Fair Scheduler";
    } else if (rs instanceof FifoScheduler) {
      this.schedulerName = "Fifo Scheduler";
    } else {
      this.schedulerName = rs.getClass().getSimpleName();
    }
    // 封装容器最小申请资源信息
    this.minAllocResource = new ResourceInfo(rs.getMinimumResourceCapability());
    // 封装容器最大申请资源信息
    this.maxAllocResource = new ResourceInfo(rs.getMaximumResourceCapability());
    // 获取调度支持的资源类型
    this.schedulingResourceTypes = rs.getSchedulingResourceTypes();
    // 获取集群允许的最高应用优先级
    this.maximumClusterPriority =
        rs.getMaxClusterLevelAppPriority().getPriority();
  }

  /**
   * 获取调度器类型名称
   * @return 调度器显示名称
   */
  public String getSchedulerType() {
    return this.schedulerName;
  }

  /**
   * 获取容器最小分配资源信息
   * @return 最小资源信息对象
   */
  public ResourceInfo getMinAllocation() {
    return this.minAllocResource;
  }

  /**
   * 获取容器最大分配资源信息
   * @return 最大资源信息对象
   */
  public ResourceInfo getMaxAllocation() {
    return this.maxAllocResource;
  }

  /**
   * 获取调度资源类型字符串表示
   * @return 资源类型数组的字符串形式
   */
  public String getSchedulerResourceTypes() {
    if (minAllocResource != null) {
      return Arrays.toString(minAllocResource.getResource().getResources());
    }
    return null;
  }

  /**
   * 获取集群级别最大应用优先级
   * @return 最大优先级数值
   */
  public int getMaxClusterLevelAppPriority() {
    return this.maximumClusterPriority;
  }
}