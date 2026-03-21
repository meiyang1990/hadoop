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

package org.apache.hadoop.yarn.server.resourcemanager.webapp;

import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceTypeInfo;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ClusterMetricsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.SchedulerInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.UserMetricsInfo;

import org.apache.hadoop.yarn.util.resource.ResourceUtils;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet.DIV;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;

import com.google.inject.Inject;

import java.util.Arrays;

/**
 * YARN ResourceManager Web UI 指标概览表格渲染块，用于展示集群全局指标，
 * 开启用户指标时同时展示当前登录用户的资源使用情况。
 */
public class MetricsOverviewTable extends HtmlBlock {
  private static final long BYTES_IN_MB = 1024 * 1024;

  private final ResourceManager rm;

  @Inject
  MetricsOverviewTable(ResourceManager rm, ViewContext ctx) {
    super(ctx);
    this.rm = rm;
  }


  @Override
  protected void render(Block html) {
    // 注入CSS样式，这是 hack 写法，无其他方式将CSS插入正确位置
    html.style(".metrics {margin-bottom:5px}"); 
    
    // 初始化集群指标信息对象
    ClusterMetricsInfo clusterMetrics = new ClusterMetricsInfo(this.rm);
    
    // 创建指标容器div
    DIV<Hamlet> div = html.div().$class("metrics");

    Resource usedResources;
    Resource totalResources;
    Resource reservedResources;
    int allocatedContainers;
    // 判断是否支持跨分区聚合指标
    if (clusterMetrics.getCrossPartitionMetricsAvailable()) {
      // 从跨分区指标获取已分配容器数
      allocatedContainers =
          clusterMetrics.getTotalAllocatedContainersAcrossPartition();
      // 从跨分区指标获取总已用资源
      usedResources =
          clusterMetrics.getTotalUsedResourcesAcrossPartition().getResource();
      // 从跨分区指标获取集群总资源
      totalResources =
          clusterMetrics.getTotalClusterResourcesAcrossPartition()
          .getResource();
      // 从跨分区指标获取总预留资源
      reservedResources =
          clusterMetrics.getTotalReservedResourcesAcrossPartition()
          .getResource();
      // 跨分区已用资源包含预留资源，这里扣除预留资源得到实际使用资源
      Resources.subtractFrom(usedResources, reservedResources);
    } else {
      // 不支持跨分区时，使用全局聚合指标
      allocatedContainers = clusterMetrics.getContainersAllocated();
      // 构造已用资源对象，兼容旧版指标结构
      usedResources = Resource.newInstance(
          clusterMetrics.getAllocatedMB(),
          (int) clusterMetrics.getAllocatedVirtualCores());
      // 构造总资源对象，兼容旧版指标结构
      totalResources = Resource.newInstance(
          clusterMetrics.getTotalMB(),
          (int) clusterMetrics.getTotalVirtualCores());
      // 构造预留资源对象，兼容旧版指标结构
      reservedResources = Resource.newInstance(
          clusterMetrics.getReservedMB(),
          (int) clusterMetrics.getReservedVirtualCores());
    }

    // 渲染集群整体指标表格
    div.h3("Cluster Metrics").
    table("#metricsoverview").
    thead().$class("ui-widget-header").
      tr().
        th().$class("ui-state-default").__("Apps Submitted").__().
        th().$class("ui-state-default").__("Apps Pending").__().
        th().$class("ui-state-default").__("Apps Running").__().
        th().$class("ui-state-default").__("Apps Completed").__().
        th().$class("ui-state-default").__("Containers Running").__().
        th().$class("ui-state-default").__("Used Resources").__().
        th().$class("ui-state-default").__("Total Resources").__().
        th().$class("ui-state-default").__("Reserved Resources").__().
        th().$class("ui-state-default").__("Physical Mem Used %").__().
        th().$class("ui-state-default").__("Physical VCores Used %").__().
        __().
        __().
    tbody().$class("ui-widget-content").
      tr().
        td(String.valueOf(clusterMetrics.getAppsSubmitted())).
        td(String.valueOf(clusterMetrics.getAppsPending())).
        td(String.valueOf(clusterMetrics.getAppsRunning())).
        td(
            // 已完成应用总数 = 正常完成 + 失败 +  killed
            String.valueOf(
                clusterMetrics.getAppsCompleted() + 
                clusterMetrics.getAppsFailed() + clusterMetrics.getAppsKilled()
                )
            ).
        td(String.valueOf(allocatedContainers)).
        td(usedResources.getFormattedString()).
        td(totalResources.getFormattedString()).
        td(reservedResources.getFormattedString()).
        td(String.valueOf(clusterMetrics.getUtilizedMBPercent())).
        td(String.valueOf(clusterMetrics.getUtilizedVirtualCoresPercent())).
        __().
        __().__();

    // 渲染节点状态指标表格
    div.h3("Cluster Nodes Metrics").
    table("#nodemetricsoverview").
    thead().$class("ui-widget-header").
      tr().
        th().$class("ui-state-default").__("Active Nodes").__().
        th().$class("ui-state-default").__("Decommissioning Nodes").__().
        th().$class("ui-state-default").__("Decommissioned Nodes").__().
        th().$class("ui-state-default").__("Lost Nodes").__().
        th().$class("ui-state-default").__("Unhealthy Nodes").__().
        th().$class("ui-state-default").__("Rebooted Nodes").__().
        th().$class("ui-state-default").__("Shutdown Nodes").__().
        __().
        __().
    tbody().$class("ui-widget-content").
      tr().
        // 添加链接可跳转对应状态的节点列表页面
        td().a(url("nodes"), String.valueOf(clusterMetrics.getActiveNodes())).__().
        td().a(url("nodes/decommissioning"), String.valueOf(clusterMetrics.getDecommissioningNodes())).__().
        td().a(url("nodes/decommissioned"), String.valueOf(clusterMetrics.getDecommissionedNodes())).__().
        td().a(url("nodes/lost"), String.valueOf(clusterMetrics.getLostNodes())).__().
        td().a(url("nodes/unhealthy"), String.valueOf(clusterMetrics.getUnhealthyNodes())).__().
        td().a(url("nodes/rebooted"), String.valueOf(clusterMetrics.getRebootedNodes())).__().
        td().a(url("nodes/shutdown"), String.valueOf(clusterMetrics.getShutdownNodes())).__().
        __().
        __().__();

    // 获取当前登录用户
    String user = request().getRemoteUser();
    if (user != null) {
      // 构造当前用户指标对象
      UserMetricsInfo userMetrics = new UserMetricsInfo(this.rm, user);
      if (userMetrics.metricsAvailable()) {
        // 指标可用时渲染当前用户指标表格
        div.h3("User Metrics for " + user).
        table("#usermetricsoverview").
        thead().$class("ui-widget-header").
          tr().
            th().$class("ui-state-default").__("Apps Submitted").__().
            th().$class("ui-state-default").__("Apps Pending").__().
            th().$class("ui-state-default").__("Apps Running").__().
            th().$class("ui-state-default").__("Apps Completed").__().
            th().$class("ui-state-default").__("Containers Running").__().
            th().$class("ui-state-default").__("Containers Pending").__().
            th().$class("ui-state-default").__("Containers Reserved").__().
            th().$class("ui-state-default").__("Memory Used").__().
            th().$class("ui-state-default").__("Memory Pending").__().
            th().$class("ui-state-default").__("Memory Reserved").__().
            th().$class("ui-state-default").__("VCores Used").__().
            th().$class("ui-state-default").__("VCores Pending").__().
            th().$class("ui-state-default").__("VCores Reserved").__().
            __().
            __().
        tbody().$class("ui-widget-content").
          tr().
            td(String.valueOf(userMetrics.getAppsSubmitted())).
            td(String.valueOf(userMetrics.getAppsPending())).
            td(String.valueOf(userMetrics.getAppsRunning())).
            td(
                // 用户已完成应用总数 = 正常完成 + 失败 + killed
                String.valueOf(
                    (userMetrics.getAppsCompleted() + 
                     userMetrics.getAppsFailed() + userMetrics.getAppsKilled())
                    )
              ).
            td(String.valueOf(userMetrics.getRunningContainers())).
            td(String.valueOf(userMetrics.getPendingContainers())).
            td(String.valueOf(userMetrics.getReservedContainers())).
            // MB转字节后格式化显示内存大小
            td(StringUtils.byteDesc(userMetrics.getAllocatedMB() * BYTES_IN_MB)).
            td(StringUtils.byteDesc(userMetrics.getPendingMB() * BYTES_IN_MB)).
            td(StringUtils.byteDesc(userMetrics.getReservedMB() * BYTES_IN_MB)).
            td(String.valueOf(userMetrics.getAllocatedVirtualCores())).
            td(String.valueOf(userMetrics.getPendingVirtualCores())).
            td(String.valueOf(userMetrics.getReservedVirtualCores())).
            __().
            __().__();
        
      }
    }

    // 初始化调度器指标
    SchedulerInfo schedulerInfo = new SchedulerInfo(this.rm);
    int schedBusy = clusterMetrics.getRmSchedulerBusyPercent();
    int rmEventQueueSize = clusterMetrics.getRmEventQueueSize();
    int schedulerEventQueueSize = clusterMetrics.getSchedulerEventQueueSize();

    // 渲染调度器指标表格
    div.h3("Scheduler Metrics").
    table("#schedulermetricsoverview").
    thead().$class("ui-widget-header").
      tr().
        th().$class("ui-state-default").__("Scheduler Type").__().
        th().$class("ui-state-default").__("Scheduling Resource Type").__().
        th().$class("ui-state-default").__("Minimum Allocation").__().
        th().$class("ui-state-default").__("Maximum Allocation").__().
        th().$class("ui-state-default")
            .__("Maximum Cluster Application Priority").__().
        th().$class("ui-state-default").__("Scheduler Busy %").__().
        th().$class("ui-state-default")
            .__("RM Dispatcher EventQueue Size").__().
        th().$class("ui-state-default")
            .__("Scheduler Dispatcher EventQueue Size").__().
        __().
        __().
    tbody().$class("ui-widget-content").
      tr().
        td(String.valueOf(schedulerInfo.getSchedulerType())).
        // 将资源类型列表转为字符串展示
        td(String.valueOf(Arrays.toString(ResourceUtils.getResourcesTypeInfo()
            .toArray(new ResourceTypeInfo[0])))).
        td(schedulerInfo.getMinAllocation().toString()).
        td(schedulerInfo.getMaxAllocation().toString()).
        td(String.valueOf(schedulerInfo.getMaxClusterLevelAppPriority())).
        // 指标不存在时显示不可用
        td(schedBusy == -1 ? UNAVAILABLE : String.valueOf(schedBusy)).
        td(String.valueOf(rmEventQueueSize)).
        td(String.valueOf(schedulerEventQueueSize)).
        __().
        __().__();

    // 关闭div标签
    div.__();
  }
}