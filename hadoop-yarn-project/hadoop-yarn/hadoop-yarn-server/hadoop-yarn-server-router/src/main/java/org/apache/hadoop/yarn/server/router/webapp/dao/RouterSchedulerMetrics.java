// 这个文件已经全部加上中文注释
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.yarn.server.router.webapp.dao;


import org.apache.hadoop.yarn.server.federation.store.records.SubClusterInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.SchedulerOverviewInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * YARN联邦Router调度器指标数据对象，封装多个子集群调度器的监控指标信息，
 * 用于REST API返回聚合后的调度器指标数据。
 */
@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class RouterSchedulerMetrics {

  // 日志记录器
  private static final Logger LOG = LoggerFactory.getLogger(RouterSchedulerMetrics.class);

  // 子集群标识
  private String subCluster = "N/A";
  // 调度器类型
  private String schedulerType = "N/A";
  // 调度资源类型
  private String schedulingResourceType = "N/A";
  // 最小资源分配量
  private String minimumAllocation = "N/A";
  // 最大资源分配量
  private String maximumAllocation = "N/A";
  // 应用默认优先级
  private String applicationPriority = "N/A";
  // 调度器繁忙程度指标
  private String schedulerBusy = "N/A";
  // RM分发器事件队列长度
  private String rmDispatcherEventQueueSize = "N/A";
  // 调度器分发器事件队列长度
  private String schedulerDispatcherEventQueueSize = "N/A";

  public RouterSchedulerMetrics() {

  }

  /**
   * 构造方法，基于子集群信息和调度概览信息初始化Router调度器指标。
   * @param subClusterInfo 子集群基础信息
   * @param metrics 集群聚合指标（未在此构造中使用，保留接口兼容）
   * @param overview 对应子集群RM返回的调度器概览信息
   */
  public RouterSchedulerMetrics(SubClusterInfo subClusterInfo, RouterClusterMetrics metrics,
      SchedulerOverviewInfo overview) {
    if (subClusterInfo != null) {
      initRouterSchedulerMetrics(subClusterInfo.getSubClusterId().getId(), overview);
    }
  }

  /**
   * 构造方法，基于本地集群名称和调度概览信息初始化Router调度器指标。
   * @param localClusterName 本地集群名称
   * @param overview 调度器概览信息
   */
  public RouterSchedulerMetrics(String localClusterName, SchedulerOverviewInfo overview) {
    initRouterSchedulerMetrics(localClusterName, overview);
  }

  /**
   * 初始化调度器指标，从调度概览对象解析提取各指标字段。
   * @param subClusterName 所属子集群名称/ID
   * @param overview 调度器概览源数据对象
   */
  private void initRouterSchedulerMetrics(String subClusterName,
      SchedulerOverviewInfo overview) {
    try {
      // 解析调度器信息，填充到当前对象
      this.subCluster = subClusterName;
      this.schedulerType = overview.getSchedulerType();
      this.schedulingResourceType = overview.getSchedulingResourceType();
      this.minimumAllocation = overview.getMinimumAllocation().toString();
      this.maximumAllocation = overview.getMaximumAllocation().toString();
      this.applicationPriority = String.valueOf(overview.getApplicationPriority());
      if (overview.getSchedulerBusy() != -1) {
        this.schedulerBusy = String.valueOf(overview.getSchedulerBusy());
      }
      this.rmDispatcherEventQueueSize =
          String.valueOf(overview.getRmDispatcherEventQueueSize());
      this.schedulerDispatcherEventQueueSize =
          String.valueOf(overview.getSchedulerDispatcherEventQueueSize());
    } catch (Exception ex) {
      // 捕获解析过程异常，记录日志不抛出
      LOG.error("RouterSchedulerMetrics Error.", ex);
    }
  }

  public String getSubCluster() {
    return subCluster;
  }

  public String getSchedulerType() {
    return schedulerType;
  }

  public String getSchedulingResourceType() {
    return schedulingResourceType;
  }

  public String getMinimumAllocation() {
    return minimumAllocation;
  }

  public String getMaximumAllocation() {
    return maximumAllocation;
  }

  public String getApplicationPriority() {
    return applicationPriority;
  }

  public String getRmDispatcherEventQueueSize() {
    return rmDispatcherEventQueueSize;
  }

  public String getSchedulerDispatcherEventQueueSize() {
    return schedulerDispatcherEventQueueSize;
  }

  public String getSchedulerBusy() {
    return schedulerBusy;
  }
}