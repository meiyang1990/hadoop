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

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerHealth;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import java.util.ArrayList;
import java.util.List;

/**
 * 容量调度器健康信息数据访问对象，为Web UI提供调度器运行状态数据
 */
@XmlAccessorType(XmlAccessType.FIELD)
public class CapacitySchedulerHealthInfo {

  /**
   * 单条调度操作详细信息，用于Web UI展示最近一次操作详情
   */
  @XmlAccessorType(XmlAccessType.FIELD)
  public static class OperationInformation {
    String operation;
    String nodeId;
    String containerId;
    String queue;

    OperationInformation() {
    }

    /**
     * 根据调度器详细信息构造操作信息对象
     * @param operation 操作类型名称
     * @param di 调度器提供的详细信息
     */
    OperationInformation(String operation,
        SchedulerHealth.DetailedInformation di) {
      this.operation = operation;
      this.nodeId = di.getNodeId() == null ? "N/A" : di.getNodeId().toString();
      this.containerId =
          di.getContainerId() == null ? "N/A" : di.getContainerId().toString();
      this.queue = di.getQueue() == null ? "N/A" : di.getQueue();
    }

    public String getNodeId() {
      return nodeId;
    }

    public String getContainerId() {
      return containerId;
    }

    public String getQueue() {
      return queue;
    }

    public String getOperation() {
      return operation;
    }
  }

  /**
   * 上次调度运行统计详情，记录各类操作的统计数据与资源使用情况
   */
  @XmlAccessorType(XmlAccessType.FIELD)
  public static class LastRunDetails {
    String operation;
    long count;
    ResourceInfo resources;

    LastRunDetails() {
    }

    /**
     * 构造上次运行统计详情对象
     * @param operation 操作类型名称
     * @param count 操作次数
     * @param resource 操作涉及的资源总量
     */
    LastRunDetails(String operation, long count, Resource resource) {
      this.operation = operation;
      this.count = count;
      this.resources = new ResourceInfo(resource);
    }

    public String getOperation() {
      return operation;
    }

    public long getCount() {
      return count;
    }

    public ResourceInfo getResources() {
      return resources;
    }
  }

  // 上次调度运行时间戳
  long lastrun;
  // 各类操作的详细信息列表
  List<OperationInformation> operationsInfo;
  // 各类操作的统计详情列表
  List<LastRunDetails> lastRunDetails;

  CapacitySchedulerHealthInfo() {
  }

  public long getLastrun() {
    return lastrun;
  }

  public List<OperationInformation> getOperationsInfo() {
    return operationsInfo;
  }

  /**
   * 从容量调度器提取健康信息构造DAO对象
   * @param cs 容量调度器实例
   */
  CapacitySchedulerHealthInfo(CapacityScheduler cs) {
    // 获取调度器健康信息对象
    SchedulerHealth ht = cs.getSchedulerHealth();
    // 提取上次调度运行时间
    lastrun = ht.getLastSchedulerRunTime();
    // 初始化操作详情列表
    operationsInfo = new ArrayList<>();
    // 添加上次资源分配操作详情
    operationsInfo.add(new OperationInformation("last-allocation",
        ht.getLastAllocationDetails()));
    // 添加上次资源释放操作详情
    operationsInfo.add(
        new OperationInformation("last-release", ht.getLastReleaseDetails()));
    // 添加上次抢占操作详情
    operationsInfo.add(new OperationInformation("last-preemption",
        ht.getLastPreemptionDetails()));
    // 添加上次资源预留操作详情
    operationsInfo.add(new OperationInformation("last-reservation",
        ht.getLastReservationDetails()));

    // 初始化统计详情列表
    lastRunDetails = new ArrayList<>();
    // 添加资源释放统计
    lastRunDetails.add(new LastRunDetails("releases", ht.getReleaseCount(), ht
      .getResourcesReleased()));
    // 添加资源分配统计
    lastRunDetails.add(new LastRunDetails("allocations", ht
      .getAllocationCount(), ht.getResourcesAllocated()));
    // 添加资源预留统计
    lastRunDetails.add(new LastRunDetails("reservations", ht
      .getReservationCount(), ht.getResourcesReserved()));

  }
}