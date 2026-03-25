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

package org.apache.hadoop.yarn.server.webapp.dao;

import static org.apache.hadoop.yarn.util.StringHelper.CSV_JOINER;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;

import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationResourceUsageReport;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.util.Times;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;
import org.apache.hadoop.yarn.util.StringHelper;

/**
 * YARN Web UI 应用信息数据访问对象，封装应用基本信息、运行状态、资源使用情况等数据，
 * 用于 REST API 返回给前端展示。
 */
@Public
@Evolving
@XmlRootElement(name = "app")
@XmlAccessorType(XmlAccessType.FIELD)
public class AppInfo {

  // 应用ID
  protected String appId;
  // 当前应用尝试ID
  protected String currentAppAttemptId;
  // 提交应用的用户名
  protected String user;
  // 应用名称
  protected String name;
  // 应用所属队列
  protected String queue;
  // 应用类型
  protected String type;
  // ApplicationMaster所在主机
  protected String host;
  // ApplicationMaster RPC端口
  protected int rpcPort;
  // 应用当前状态
  protected YarnApplicationState appState;
  // 当前运行容器数
  protected int runningContainers;
  // 应用进度百分比
  protected float progress;
  // 应用诊断信息
  protected String diagnosticsInfo;
  // 原始追踪URL
  protected String originalTrackingUrl;
  // 追踪URL
  protected FinalApplicationStatus finalAppStatus;
  // 应用提交时间
  private long submittedTime;
  // 应用启动时间
  protected long startedTime;
  // 应用启动完成时间
  private long launchTime;
  // 应用结束时间
  protected long finishedTime;
  // 应用已运行时间
  protected long elapsedTime;
  // 应用标签，逗号分隔
  protected String applicationTags;
  // 应用优先级
  protected int priority;
  // 已分配CPU核心数
  private long allocatedCpuVcores;
  // 已分配内存大小（MB）
  private long allocatedMemoryMB;
  // 已分配GPU数量
  private long allocatedGpus;
  // 预留CPU核心数
  private long reservedCpuVcores;
  // 预留内存大小（MB）
  private long reservedMemoryMB;
  // 预留GPU数量
  private long reservedGpus;
  // 是否为非托管应用
  protected boolean unmanagedApplication;
  // 应用节点标签表达式
  private String appNodeLabelExpression;
  // ApplicationMaster节点标签表达式
  private String amNodeLabelExpression;
  // 累计资源分配量（资源-秒）
  private String aggregateResourceAllocation;
  // 累计被抢占资源量（资源-秒）
  private String aggregatePreemptedResourceAllocation;

  /**
   * JAXB 反序列化需要的无参构造方法
   */
  public AppInfo() {
    // JAXB needs this
  }

  /**
   * 根据应用报告构造应用信息对象，提取所有需要展示的字段
   * @param app YARN应用报告对象
   */
  public AppInfo(ApplicationReport app) {
    appId = app.getApplicationId().toString();
    if (app.getCurrentApplicationAttemptId() != null) {
      currentAppAttemptId = app.getCurrentApplicationAttemptId().toString();
    }
    user = app.getUser();
    queue = app.getQueue();
    name = app.getName();
    type = app.getApplicationType();
    host = app.getHost();
    rpcPort = app.getRpcPort();
    appState = app.getYarnApplicationState();
    diagnosticsInfo = app.getDiagnostics();
    trackingUrl = app.getTrackingUrl();
    originalTrackingUrl = app.getOriginalTrackingUrl();
    submittedTime = app.getSubmitTime();
    startedTime = app.getStartTime();
    launchTime = app.getLaunchTime();
    finishedTime = app.getFinishTime();
    // 计算应用已运行时间
    elapsedTime = Times.elapsed(startedTime, finishedTime);
    finalAppStatus = app.getFinalApplicationStatus();
    priority = 0;
    if (app.getPriority() != null) {
      priority = app.getPriority().getPriority();
    }
    // 获取应用资源使用报告
    ApplicationResourceUsageReport usageReport =
        app.getApplicationResourceUsageReport();
    if (usageReport != null) {
      // 获取正在使用的容器数量
      runningContainers = usageReport
          .getNumUsedContainers();
      if (usageReport.getUsedResources() != null) {
        // 提取已分配的CPU和内存
        allocatedCpuVcores = usageReport
            .getUsedResources().getVirtualCores();
        allocatedMemoryMB = usageReport
            .getUsedResources().getMemorySize();
        // 提取预留的CPU和内存
        reservedCpuVcores = usageReport
            .getReservedResources().getVirtualCores();
        reservedMemoryMB = usageReport
            .getReservedResources().getMemorySize();
        // 获取GPU资源类型索引
        Integer gpuIndex = ResourceUtils.getResourceTypeIndex()
            .get(ResourceInformation.GPU_URI);
        allocatedGpus = -1;
        reservedGpus = -1;
        if (gpuIndex != null) {
          // 提取已分配和预留的GPU数量
          allocatedGpus = usageReport.getUsedResources()
              .getResourceValue(ResourceInformation.GPU_URI);
          reservedGpus = usageReport.getReservedResources()
              .getResourceValue(ResourceInformation.GPU_URI);
        }
      }
      // 格式化累计资源分配为字符串
      aggregateResourceAllocation = StringHelper.getResourceSecondsString(
          usageReport.getResourceSecondsMap());
      // 格式化累计被抢占资源为字符串
      aggregatePreemptedResourceAllocation = StringHelper
        .getResourceSecondsString(usageReport.getPreemptedResourceSecondsMap());
    }
    // 转换进度为百分比
    progress = app.getProgress() * 100; // in percent
    if (app.getApplicationTags() != null && !app.getApplicationTags().isEmpty()) {
      // 将标签集合拼接为CSV格式
      this.applicationTags = CSV_JOINER.join(app.getApplicationTags());
    }
    unmanagedApplication = app.isUnmanagedApp();
    appNodeLabelExpression = app.getAppNodeLabelExpression();
    amNodeLabelExpression = app.getAmNodeLabelExpression();
  }

  public String getAppId() {
    return appId;
  }

  public String getCurrentAppAttemptId() {
    return currentAppAttemptId;
  }

  public String getUser() {
    return user;
  }

  public String getName() {
    return name;
  }

  public String getQueue() {
    return queue;
  }

  public String getType() {
    return type;
  }

  public String getHost() {
    return host;
  }

  public int getRpcPort() {
    return rpcPort;
  }

  public YarnApplicationState getAppState() {
    return appState;
  }

  public int getRunningContainers() {
    return runningContainers;
  }

  public long getAllocatedCpuVcores() {
    return allocatedCpuVcores;
  }

  public long getAllocatedMemoryMB() {
    return allocatedMemoryMB;
  }

  public long getAllocatedGpus() {
    return allocatedGpus;
  }

  public long getReservedCpuVcores() {
    return reservedCpuVcores;
  }

  public long getReservedMemoryMB() {
    return reservedMemoryMB;
  }

  public long getReservedGpus() {
    return reservedGpus;
  }

  public float getProgress() {
    return progress;
  }

  public String getDiagnosticsInfo() {
    return diagnosticsInfo;
  }

  public String getOriginalTrackingUrl() {
    return originalTrackingUrl;
  }

  public String getTrackingUrl() {
    return trackingUrl;
  }

  public FinalApplicationStatus getFinalAppStatus() {
    return finalAppStatus;
  }

  public long getSubmittedTime() {
    return submittedTime;
  }

  public long getLaunchTime() {
    return launchTime;
  }

  public long getStartedTime() {
    return startedTime;
  }

  public long getFinishedTime() {
    return finishedTime;
  }

  public long getElapsedTime() {
    return elapsedTime;
  }

  public String getApplicationTags() {
    return applicationTags;
  }

  public boolean isUnmanagedApp() {
    return unmanagedApplication;
  }

  public int getPriority() {
    return priority;
  }

  public String getAppNodeLabelExpression() {
    return appNodeLabelExpression;
  }

  public String getAmNodeLabelExpression() {
    return amNodeLabelExpression;
  }

  public String getAggregateResourceAllocation() {
    return aggregateResourceAllocation;
  }

  public String getAggregatePreemptedResourceAllocation() {
    return aggregatePreemptedResourceAllocation;
  }
}