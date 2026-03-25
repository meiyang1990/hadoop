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
import java.util.Collection;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlSeeAlso;
import javax.xml.bind.annotation.XmlTransient;

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueuePath;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.AllocationConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FSLeafQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler;
import org.apache.hadoop.yarn.util.resource.Resources;

/**
 * 公平调度器队列信息数据访问对象，封装队列的各项调度信息，供Web UI和REST API返回使用
 */
@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
@XmlSeeAlso({FairSchedulerLeafQueueInfo.class})
public class FairSchedulerQueueInfo {  
  private int maxApps;
  
  @XmlTransient
  private float fractionMemUsed;
  @XmlTransient
  private float fractionMemSteadyFairShare;
  @XmlTransient
  private float fractionMemFairShare;
  @XmlTransient
  private float fractionMemMaxShare;
  
  private ResourceInfo minResources;
  private ResourceInfo maxResources;
  private ResourceInfo usedResources;
  private ResourceInfo amUsedResources;
  private ResourceInfo amMaxResources;
  private ResourceInfo demandResources;
  private ResourceInfo steadyFairResources;
  private ResourceInfo fairResources;
  private ResourceInfo clusterResources;
  private ResourceInfo reservedResources;
  private ResourceInfo maxContainerAllocation;

  private long pendingContainers;
  private long allocatedContainers;
  private long reservedContainers;

  private String queueName;
  private String schedulingPolicy;

  private boolean preemptable;

  private FairSchedulerQueueInfoList childQueues;

  public FairSchedulerQueueInfo() {
  }
  
  /**
   * 从公平调度器队列构造Web API使用的队列信息对象
   * @param queue 调度队列对象
   * @param scheduler 公平调度器实例
   */
  public FairSchedulerQueueInfo(FSQueue queue, FairScheduler scheduler) {
    // 获取调度分配配置
    AllocationConfiguration allocConf = scheduler.getAllocationConfiguration();
    
    queueName = queue.getName();
    schedulingPolicy = queue.getPolicy().getName();
    
    // 初始化集群总资源信息
    clusterResources = new ResourceInfo(scheduler.getClusterResource());
    
    // 初始化AM资源使用和限制
    amUsedResources = new ResourceInfo(queue.getMetrics().getAMResourceUsage());
    amMaxResources = new ResourceInfo(queue.getMetrics().getMaxAMShare());
    // 初始化队列总资源使用和需求
    usedResources = new ResourceInfo(queue.getResourceUsage());
    demandResources = new ResourceInfo(queue.getDemand());
    // 计算已用内存占集群总内存比例
    fractionMemUsed = (float)usedResources.getMemorySize() /
        clusterResources.getMemorySize();

    // 初始化稳定公平份额和当前公平份额
    steadyFairResources = new ResourceInfo(queue.getSteadyFairShare());
    fairResources = new ResourceInfo(queue.getFairShare());
    // 初始化最小资源份额
    minResources = new ResourceInfo(queue.getMinShare());
    // 初始化最大资源份额（取队列最大份额和集群总资源的较小值）
    maxResources = new ResourceInfo(
        Resources.componentwiseMin(queue.getMaxShare(),
            scheduler.getClusterResource()));
    // 初始化单个容器最大可分配资源
    maxContainerAllocation =
        new ResourceInfo(scheduler.getMaximumResourceCapability(queueName));
    // 初始化预留资源
    reservedResources = new ResourceInfo(queue.getReservedResource());

    // 计算稳定公平份额内存占比
    fractionMemSteadyFairShare =
        (float)steadyFairResources.getMemorySize() / clusterResources.getMemorySize();
    // 计算当前公平份额内存占比
    fractionMemFairShare = (float) fairResources.getMemorySize()
        / clusterResources.getMemorySize();
    // 计算最大份额内存占比
    fractionMemMaxShare = (float)maxResources.getMemorySize() / clusterResources.getMemorySize();
    
    maxApps = queue.getMaxRunningApps();

    // 获取各类容器统计信息
    allocatedContainers = queue.getMetrics().getAllocatedContainers();
    reservedContainers = queue.getMetrics().getReservedContainers();
    pendingContainers = queue.getMetrics().getPendingContainers();

    QueuePath queuePath = new QueuePath(queueName);
    // 如果队列是可预留的且配置不显示预留队列，直接返回不处理子队列
    if (allocConf.isReservable(queuePath) &&
        !allocConf.getShowReservationAsQueues(queuePath)) {
      return;
    }

    preemptable = queue.isPreemptable();
    // 构造子队列信息列表
    childQueues = getChildQueues(queue, scheduler);
  }

  public long getAllocatedContainers() {
    return allocatedContainers;
  }

  public long getPendingContainers() { return pendingContainers; }

  public long getReservedContainers() {
    return reservedContainers;
  }

  /**
   * 构造子队列信息列表
   * @param queue 父队列
   * @param scheduler 公平调度器实例
   * @return 子队列信息列表，如果没有子队列返回null
   */
  protected FairSchedulerQueueInfoList getChildQueues(FSQueue queue,
                                                      FairScheduler scheduler) {
    // Return null to omit 'childQueues' field from the return value of
    // REST API if it is empty. We omit the field to keep the consistency
    // with CapacitySchedulerQueueInfo, which omits 'queues' field if empty.
    Collection<FSQueue> children = queue.getChildQueues();
    if (children.isEmpty()) {
      return null;
    }
    FairSchedulerQueueInfoList list = new FairSchedulerQueueInfoList();
    // 遍历子队列，叶子队列和父队列分别使用对应类型构造
    for (FSQueue child : children) {
      if (child instanceof FSLeafQueue) {
        list.addToQueueInfoList(
            new FairSchedulerLeafQueueInfo((FSLeafQueue) child, scheduler));
      } else {
        list.addToQueueInfoList(
            new FairSchedulerQueueInfo(child, scheduler));
      }
    }
    return list;
  }
  
  /**
   * Returns the steady fair share as a fraction of the entire cluster capacity.
   * @return steady fairshare memoryfraction.
   */
  public float getSteadyFairShareMemoryFraction() {
    return fractionMemSteadyFairShare;
  }

  /**
   * Returns the fair share as a fraction of the entire cluster capacity.
   * @return fair share memory fraction.
   */
  public float getFairShareMemoryFraction() {
    return fractionMemFairShare;
  }

  /**
   * Returns the steady fair share of this queue in megabytes.
   * @return steady fair share.
   */
  public ResourceInfo getSteadyFairShare() {
    return steadyFairResources;
  }

  /**
   * Returns the fair share of this queue in megabytes.
   * @return fair share.
   */
  public ResourceInfo getFairShare() {
    return fairResources;
  }

  public ResourceInfo getMinResources() {
    return minResources;
  }
  
  public ResourceInfo getMaxResources() {
    return maxResources;
  }

  public ResourceInfo getMaxContainerAllocation() {
    return maxContainerAllocation;
  }

  public ResourceInfo getReservedResources() {
    return reservedResources;
  }

  public int getMaxApplications() {
    return maxApps;
  }
  
  public String getQueueName() {
    return queueName;
  }
  
  public ResourceInfo getUsedResources() {
    return usedResources;
  }

  /**
   * @return the am used resource of this queue.
   */
  public ResourceInfo getAMUsedResources() {
    return amUsedResources;
  }

  /**
   * @return the am max resource of this queue.
   */
  public ResourceInfo getAMMaxResources() {
    return amMaxResources;
  }

  /**
   * @return the demand resource of this queue.
     */
  public ResourceInfo getDemandResources() {
    return demandResources;
  }

  /**
   * Returns the memory used by this queue as a fraction of the entire 
   * cluster capacity.
   * @return used memory fraction.
   */
  public float getUsedMemoryFraction() {
    return fractionMemUsed;
  }
  
  /**
   * Returns the capacity of this queue as a fraction of the entire cluster 
   * capacity.
   * @return max resources fraction.
   */
  public float getMaxResourcesFraction() {
    return fractionMemMaxShare;
  }
  
  /**
   * Returns the name of the scheduling policy used by this queue.
   * @return SchedulingPolicy.
   */
  public String getSchedulingPolicy() {
    return schedulingPolicy;
  }

  public Collection<FairSchedulerQueueInfo> getChildQueues() {
    return childQueues != null ? childQueues.getQueueInfoList() :
        new ArrayList<FairSchedulerQueueInfo>();
  }

  public boolean isPreemptable() {
    return preemptable;
  }
}