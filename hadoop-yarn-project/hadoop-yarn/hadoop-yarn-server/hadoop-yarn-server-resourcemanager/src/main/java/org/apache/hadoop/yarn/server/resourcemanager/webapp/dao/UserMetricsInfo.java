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
import javax.xml.bind.annotation.XmlTransient;

import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;

/**
 * 对应用户指标的数据传输对象，封装YARN Web UI展示的用户资源与应用统计指标
 */
@XmlRootElement(name = "userMetrics")
@XmlAccessorType(XmlAccessType.FIELD)
public class UserMetricsInfo {

  protected int appsSubmitted;
  protected int appsCompleted;
  protected int appsPending;
  protected int appsRunning;
  protected int appsFailed;
  protected int appsKilled;
  protected int runningContainers;
  protected int pendingContainers;
  protected int reservedContainers;
  protected long reservedMB;
  protected long pendingMB;
  protected long allocatedMB;
  protected long reservedVirtualCores;
  protected long pendingVirtualCores;
  protected long allocatedVirtualCores;

  @XmlTransient
  protected boolean userMetricsAvailable;

  public UserMetricsInfo() {
  } // JAXB needs this

  /**
   * 从ResourceManager中构造指定用户的指标信息对象
   * @param rm ResourceManager实例
   * @param user 目标用户名
   */
  public UserMetricsInfo(final ResourceManager rm, final String user) {
    // 获取ResourceScheduler实例
    ResourceScheduler rs = rm.getResourceScheduler();
    // 获取根队列总指标
    QueueMetrics metrics = rs.getRootQueueMetrics();
    // 获取指定用户的指标
    QueueMetrics userMetrics = metrics.getUserMetrics(user);
    // 默认标记指标不可用
    this.userMetricsAvailable = false;

    if (userMetrics != null) {
      // 标记用户指标可用
      this.userMetricsAvailable = true;

      // 填充各状态应用数量指标
      this.appsSubmitted = userMetrics.getAppsSubmitted();
      this.appsCompleted = userMetrics.getAppsCompleted();
      this.appsPending = userMetrics.getAppsPending();
      this.appsRunning = userMetrics.getAppsRunning();
      this.appsFailed = userMetrics.getAppsFailed();
      this.appsKilled = userMetrics.getAppsKilled();

      // 填充各状态容器数量指标
      this.runningContainers = userMetrics.getAllocatedContainers();
      this.pendingContainers = userMetrics.getPendingContainers();
      this.reservedContainers = userMetrics.getReservedContainers();

      // 填充内存资源指标（单位：MB）
      this.reservedMB = userMetrics.getReservedMB();
      this.pendingMB = userMetrics.getPendingMB();
      this.allocatedMB = userMetrics.getAllocatedMB();

      // 填充虚拟CPU核心资源指标
      this.reservedVirtualCores = userMetrics.getReservedVirtualCores();
      this.pendingVirtualCores = userMetrics.getPendingVirtualCores();
      this.allocatedVirtualCores = userMetrics.getAllocatedVirtualCores();
    }
  }

  /**
   * 获取用户指标是否可用
   * @return true表示指标存在可用，false表示无该用户指标
   */
  public boolean metricsAvailable() {
    return userMetricsAvailable;
  }

  public int getAppsSubmitted() {
    return this.appsSubmitted;
  }

  public int getAppsCompleted() {
    return appsCompleted;
  }

  public int getAppsPending() {
    return appsPending;
  }

  public int getAppsRunning() {
    return appsRunning;
  }

  public int getAppsFailed() {
    return appsFailed;
  }

  public int getAppsKilled() {
    return appsKilled;
  }

  public long getReservedMB() {
    return this.reservedMB;
  }

  public long getAllocatedMB() {
    return this.allocatedMB;
  }

  public long getPendingMB() {
    return this.pendingMB;
  }

  public long getReservedVirtualCores() {
    return this.reservedVirtualCores;
  }

  public long getAllocatedVirtualCores() {
    return this.allocatedVirtualCores;
  }

  public long getPendingVirtualCores() {
    return this.pendingVirtualCores;
  }

  public int getReservedContainers() {
    return this.reservedContainers;
  }

  public int getRunningContainers() {
    return this.runningContainers;
  }

  public int getPendingContainers() {
    return this.pendingContainers;
  }
}