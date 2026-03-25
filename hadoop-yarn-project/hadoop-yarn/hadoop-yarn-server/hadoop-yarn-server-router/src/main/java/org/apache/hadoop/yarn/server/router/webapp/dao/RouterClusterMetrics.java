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

import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ClusterMetricsInfo;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * YARN Router联邦集群聚合指标数据对象，用于在Web UI展示聚合后的集群指标信息。
 * 聚合多个子RM的集群指标，统一对外提供联邦层面的集群监控数据。
 */
@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class RouterClusterMetrics {

  // 1MB对应的字节数
  protected static final long BYTES_IN_MB = 1024 * 1024;
  private static final Logger LOG = LoggerFactory.getLogger(RouterClusterMetrics.class);

  // 网页标题前缀，标识联邦集群页面
  private String webPageTitlePrefix = "Federation";

  // 应用状态指标
  private String appsSubmitted = "N/A";
  private String appsCompleted = "N/A";
  private String appsPending = "N/A";
  private String appsRunning = "N/A";
  private String appsFailed = "N/A";
  private String appsKilled = "N/A";

  // 内存资源指标
  private String totalMemory = "N/A";
  private String reservedMemory = "N/A";
  private String availableMemory = "N/A";
  private String allocatedMemory = "N/A";
  private String pendingMemory = "N/A";

  // CPU核心指标
  private String reservedVirtualCores = "N/A";
  private String availableVirtualCores = "N/A";
  private String allocatedVirtualCores = "N/A";
  private String pendingVirtualCores = "N/A";
  private String totalVirtualCores = "N/A";

  // 通用资源指标
  private String usedResources = "N/A";
  private String totalResources = "N/A";
  private String reservedResources = "N/A";
  private String allocatedContainers = "N/A";

  // 资源利用率指标
  private String utilizedMBPercent = "N/A";
  private String utilizedVirtualCoresPercent = "N/A";

  // 节点状态指标
  private String activeNodes = "N/A";
  private String decommissioningNodes = "N/A";
  private String decommissionedNodes = "N/A";
  private String lostNodes = "N/A";
  private String unhealthyNodes = "N/A";
  private String rebootedNodes = "N/A";
  private String shutdownNodes = "N/A";

  public RouterClusterMetrics() {

  }

  /**
   * 构造函数，从单个RM的指标信息转换生成Router聚合指标。
   * @param metrics 单个RM的集群指标信息
   */
  public RouterClusterMetrics(ClusterMetricsInfo metrics) {
    if (metrics != null) {
      // 转换应用指标信息
      conversionApplicationInformation(metrics);

      // 转换内存指标信息
      conversionMemoryInformation(metrics);

      // 转换资源指标信息
      conversionResourcesInformation(metrics);

      // 转换资源利用率指标
      conversionResourcesPercent(metrics);

      // 转换节点指标信息
      conversionNodeInformation(metrics);
    }
  }

  /**
   * 构造函数，从单个RM的指标信息构造，并指定网页标题前缀。
   * @param metrics 单个RM的集群指标信息
   * @param webPageTitlePrefix 网页标题前缀
   */
  public RouterClusterMetrics(ClusterMetricsInfo metrics,
      String webPageTitlePrefix) {
    this(metrics);
    this.webPageTitlePrefix = webPageTitlePrefix;
  }

  // Getters for all metric fields
  public String getAppsSubmitted() {
    return appsSubmitted;
  }

  public String getAppsCompleted() {
    return appsCompleted;
  }

  public String getAppsPending() {
    return appsPending;
  }

  public String getAppsRunning() {
    return appsRunning;
  }

  public String getAppsFailed() {
    return appsFailed;
  }

  public String getAppsKilled() {
    return appsKilled;
  }

  public String getTotalMemory() {
    return totalMemory;
  }

  public String getReservedMemory() {
    return reservedMemory;
  }

  public String getAvailableMemory() {
    return availableMemory;
  }

  public String getAllocatedMemory() {
    return allocatedMemory;
  }

  public String getPendingMemory() {
    return pendingMemory;
  }

  public String getReservedVirtualCores() {
    return reservedVirtualCores;
  }

  public String getAvailableVirtualCores() {
    return availableVirtualCores;
  }

  public String getAllocatedVirtualCores() {
    return allocatedVirtualCores;
  }

  public String getPendingVirtualCores() {
    return pendingVirtualCores;
  }

  public String getTotalVirtualCores() {
    return totalVirtualCores;
  }

  public String getUsedResources() {
    return usedResources;
  }

  public String getTotalResources() {
    return totalResources;
  }

  public String getReservedResources() {
    return reservedResources;
  }

  public String getAllocatedContainers() {
    return allocatedContainers;
  }

  public String getUtilizedMBPercent() {
    return utilizedMBPercent;
  }

  public String getUtilizedVirtualCoresPercent() {
    return utilizedVirtualCoresPercent;
  }

  public String getActiveNodes() {
    return activeNodes;
  }

  public String getDecommissioningNodes() {
    return decommissioningNodes;
  }

  public String getDecommissionedNodes() {
    return decommissionedNodes;
  }

  public String getLostNodes() {
    return lostNodes;
  }

  public String getUnhealthyNodes() {
    return unhealthyNodes;
  }

  public String getRebootedNodes() {
    return rebootedNodes;
  }

  public String getShutdownNodes() {
    return shutdownNodes;
  }

  /**
   * 转换应用状态指标信息。
   * @param metrics 源RM集群指标信息
   */
  public void conversionApplicationInformation(ClusterMetricsInfo metrics) {
    try {
      // 提取各状态应用数量
      this.appsSubmitted = String.valueOf(metrics.getAppsSubmitted());
      this.appsCompleted = String.valueOf(metrics.getAppsCompleted() +
           metrics.getAppsFailed() + metrics.getAppsKilled());
      this.appsPending = String.valueOf(metrics.getAppsPending());
      this.appsRunning = String.valueOf(metrics.getAppsRunning());
      this.appsFailed = String.valueOf(metrics.getAppsFailed());
      this.appsKilled = String.valueOf(metrics.getAppsKilled());
    } catch (Exception e) {
      LOG.error("conversionApplicationInformation error.", e);
    }
  }

  /**
   * 转换内存资源指标信息。
   * @param metrics 源RM集群指标信息
   */
  public void conversionMemoryInformation(ClusterMetricsInfo metrics) {
    try {
      // 将MB转换为友好格式的字节描述
      this.totalMemory = StringUtils.byteDesc(metrics.getTotalMB() * BYTES_IN_MB);
      this.reservedMemory = StringUtils.byteDesc(metrics.getReservedMB() * BYTES_IN_MB);
      this.availableMemory = StringUtils.byteDesc(metrics.getAvailableMB() * BYTES_IN_MB);
      this.allocatedMemory = StringUtils.byteDesc(metrics.getAllocatedMB() * BYTES_IN_MB);
      this.pendingMemory = StringUtils.byteDesc(metrics.getPendingMB() * BYTES_IN_MB);
    } catch (Exception e) {
      LOG.error("conversionMemoryInformation error.", e);
    }
  }

  /**
   * 转换通用资源指标信息，支持跨资源分区的指标聚合。
   * @param metrics 源RM集群指标信息
   */
  public void conversionResourcesInformation(ClusterMetricsInfo metrics) {
    try {
      // 声明资源指标变量
      Resource metricUsedResources;
      Resource metricTotalResources;
      Resource metricReservedResources;

      int metricAllocatedContainers;
      // 若支持跨分区指标聚合，则从跨分区聚合结果获取数据
      if (metrics.getCrossPartitionMetricsAvailable()) {
        metricAllocatedContainers = metrics.getTotalAllocatedContainersAcrossPartition();
        metricUsedResources = metrics.getTotalUsedResourcesAcrossPartition().getResource();
        metricTotalResources = metrics.getTotalClusterResourcesAcrossPartition().getResource();
        metricReservedResources = metrics.getTotalReservedResourcesAcrossPartition().getResource();
        // 跨分区已用资源包含预留资源，减去预留资源得到实际已用
        Resources.subtractFrom(metricUsedResources, metricReservedResources);
      } else {
        // 不支持跨分区则从普通指标提取数据
        metricAllocatedContainers = metrics.getContainersAllocated();
        metricUsedResources = Resource.newInstance(metrics.getAllocatedMB(),
            (int) metrics.getAllocatedVirtualCores());
        metricTotalResources = Resource.newInstance(metrics.getTotalMB(),
            (int) metrics.getTotalVirtualCores());
        metricReservedResources = Resource.newInstance(metrics.getReservedMB(),
            (int) metrics.getReservedVirtualCores());
      }

      // 格式化为UI可展示的字符串
      usedResources = metricUsedResources.getFormattedString();
      totalResources = metricTotalResources.getFormattedString();
      reservedResources = metricReservedResources.getFormattedString();
      allocatedContainers =  String.valueOf(metricAllocatedContainers);

    } catch (Exception e) {
      LOG.error("conversionResourcesInformation error.", e);
    }
  }

  /**
   * 转换资源利用率指标信息。
   * @param metrics 源RM集群指标信息
   */
  public void conversionResourcesPercent(ClusterMetricsInfo metrics) {
    try {
      this.utilizedMBPercent = String.valueOf(metrics.getUtilizedMBPercent());
      this.utilizedVirtualCoresPercent = String.valueOf(metrics.getUtilizedVirtualCoresPercent());
    } catch (Exception e) {
      LOG.error("conversionResourcesPercent error.", e);
    }
  }

  /**
   * 转换节点状态指标信息。
   * @param metrics 源RM集群指标信息
   */
  public void conversionNodeInformation(ClusterMetricsInfo metrics) {
    try {
      this.activeNodes = String.valueOf(metrics.getActiveNodes());
      this.decommissioningNodes = String.valueOf(metrics.getDecommissioningNodes());
      this.decommissionedNodes = String.valueOf(metrics.getDecommissionedNodes());
      this.lostNodes = String.valueOf(metrics.getLostNodes());
      this.unhealthyNodes = String.valueOf(metrics.getUnhealthyNodes());
      this.rebootedNodes = String.valueOf(metrics.getRebootedNodes());
      this.shutdownNodes = String.valueOf(metrics.getShutdownNodes());
    } catch (Exception e) {
      LOG.error("conversionNodeInformation error.", e);
    }
  }

  public String getWebPageTitlePrefix() {
    return webPageTitlePrefix;
  }
}