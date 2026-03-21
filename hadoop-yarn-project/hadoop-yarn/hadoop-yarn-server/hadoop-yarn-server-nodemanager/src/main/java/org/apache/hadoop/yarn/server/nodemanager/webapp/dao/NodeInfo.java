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

package org.apache.hadoop.yarn.server.nodemanager.webapp.dao;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.VersionInfo;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.NodeManager;
import org.apache.hadoop.yarn.server.nodemanager.ResourceView;
import org.apache.hadoop.yarn.util.YarnVersionInfo;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;

/**
 * NodeManager节点信息数据访问对象，为Web UI提供节点信息序列化
 * 封装NodeManager的健康状态、资源使用、版本信息等核心数据，用于Web界面展示
 */
@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class NodeInfo {

  // 字节转MB的换算系数
  private static final long BYTES_IN_MB = 1024 * 1024;

  protected String healthReport;
  protected long totalVmemAllocatedContainersMB;
  protected long totalPmemAllocatedContainersMB;
  protected long totalVCoresAllocatedContainers;
  protected boolean vmemCheckEnabled;
  protected boolean pmemCheckEnabled;
  protected long lastNodeUpdateTime;
  protected String resourceTypes;
  protected boolean nodeHealthy;
  protected String nodeManagerVersion;
  protected String nodeManagerBuildVersion;
  protected String nodeManagerVersionBuiltOn;
  protected String hadoopVersion;
  protected String hadoopBuildVersion;
  protected String hadoopVersionBuiltOn;
  protected String id;
  protected String nodeHostName;
  protected long nmStartupTime;

  /**
   * 无参构造器，供JAXB序列化使用
   */
  public NodeInfo() {
  } // JAXB needs this

  /**
   * 构造NodeInfo对象，从NM上下文和资源视图中提取节点信息
   * @param context NodeManager上下文，包含节点状态信息
   * @param resourceView 节点资源视图，包含资源分配情况
   */
  public NodeInfo(final Context context, final ResourceView resourceView) {
    // 设置节点ID
    this.id = context.getNodeId().toString();
    // 设置节点主机名
    this.nodeHostName = context.getNodeId().getHost();
    // 转换分配给容器的虚拟内存为MB单位
    this.totalVmemAllocatedContainersMB = resourceView
        .getVmemAllocatedForContainers() / BYTES_IN_MB;
    // 获取虚拟内存检查是否开启
    this.vmemCheckEnabled = resourceView.isVmemCheckEnabled();
    // 转换分配给容器的物理内存为MB单位
    this.totalPmemAllocatedContainersMB = resourceView
        .getPmemAllocatedForContainers() / BYTES_IN_MB;
    // 获取物理内存检查是否开启
    this.pmemCheckEnabled = resourceView.isPmemCheckEnabled();
    // 获取已分配给容器的CPU核数
    this.totalVCoresAllocatedContainers = resourceView
        .getVCoresAllocatedForContainers();
    // 将资源类型信息拼接为逗号分隔的字符串
    this.resourceTypes = StringUtils.join(", ",
        ResourceUtils.getResourcesTypeInfo());
    // 获取节点是否健康状态
    this.nodeHealthy = context.getNodeHealthStatus().getIsNodeHealthy();
    // 获取上次健康状态更新时间
    this.lastNodeUpdateTime = context.getNodeHealthStatus()
        .getLastHealthReportTime();
    // 获取节点健康报告文本
    this.healthReport = context.getNodeHealthStatus().getHealthReport();
    // 获取Yarn版本信息
    this.nodeManagerVersion = YarnVersionInfo.getVersion();
    // 获取NodeManager构建版本信息
    this.nodeManagerBuildVersion = YarnVersionInfo.getBuildVersion();
    // 获取NodeManager编译时间
    this.nodeManagerVersionBuiltOn = YarnVersionInfo.getDate();
    // 获取Hadoop版本信息
    this.hadoopVersion = VersionInfo.getVersion();
    // 获取Hadoop构建版本信息
    this.hadoopBuildVersion = VersionInfo.getBuildVersion();
    // 获取Hadoop编译时间
    this.hadoopVersionBuiltOn = VersionInfo.getDate();
    // 获取NodeManager启动时间
    this.nmStartupTime = NodeManager.getNMStartupTime();
  }

  public String getNodeId() {
    return this.id;
  }

  public String getNodeHostName() {
    return this.nodeHostName;
  }

  public String getNMVersion() {
    return this.nodeManagerVersion;
  }

  public String getNMBuildVersion() {
    return this.nodeManagerBuildVersion;
  }

  public String getNMVersionBuiltOn() {
    return this.nodeManagerVersionBuiltOn;
  }

  public String getHadoopVersion() {
    return this.hadoopVersion;
  }

  public String getHadoopBuildVersion() {
    return this.hadoopBuildVersion;
  }

  public String getHadoopVersionBuiltOn() {
    return this.hadoopVersionBuiltOn;
  }

  public boolean getHealthStatus() {
    return this.nodeHealthy;
  }

  public long getLastNodeUpdateTime() {
    return this.lastNodeUpdateTime;
  }

  public String getHealthReport() {
    return this.healthReport;
  }

  public long getTotalVmemAllocated() {
    return this.totalVmemAllocatedContainersMB;
  }

  public long getTotalVCoresAllocated() {
    return this.totalVCoresAllocatedContainers;
  }

  public boolean isVmemCheckEnabled() {
    return this.vmemCheckEnabled;
  }

  public long getTotalPmemAllocated() {
    return this.totalPmemAllocatedContainersMB;
  }

  public boolean isPmemCheckEnabled() {
    return this.pmemCheckEnabled;
  }

  public String getResourceTypes() {
    return this.resourceTypes;
  }

  public long getNMStartupTime() {
    return nmStartupTime;
  }
}