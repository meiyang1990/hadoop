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

package org.apache.hadoop.yarn.server.resourcemanager;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ha.HAServiceProtocol;
import org.apache.hadoop.ha.HAServiceProtocol.HAServiceState;
import org.apache.hadoop.yarn.conf.ConfigurationProvider;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.server.resourcemanager.ahs.RMApplicationHistoryWriter;
import org.apache.hadoop.yarn.server.resourcemanager.metrics.SystemMetricsPublisher;
import org.apache.hadoop.yarn.server.resourcemanager.timelineservice.RMTimelineCollectorManager;

/**
 * ResourceManager 持久服务上下文，维护需要始终运行的核心服务，不受RM高可用状态切换影响。
 * 该对象在RMContextImpl初始化阶段创建。
 * <p>
 * <b>注意:</b> 向该类添加新服务时，必须保证服务可以在RM任意HA状态下持续运行，不需要随状态切换启停。
 */
@Private
@Unstable
public class RMServiceContext {

  // RM事件分发器实例
  private Dispatcher rmDispatcher;
  // 是否开启RM高可用
  private boolean isHAEnabled;
  // 当前RM高可用服务状态，初始为初始化中
  private HAServiceState haServiceState =
      HAServiceProtocol.HAServiceState.INITIALIZING;
  // RM管理服务实例
  private AdminService adminService;
  // 配置提供者实例
  private ConfigurationProvider configurationProvider;
  // YARN配置对象
  private Configuration yarnConfiguration;
  // RM应用历史写入器实例
  private RMApplicationHistoryWriter rmApplicationHistoryWriter;
  // 系统指标发布器实例
  private SystemMetricsPublisher systemMetricsPublisher;
  // 内嵌主节点选举器实例
  private EmbeddedElector elector;
  // HA状态修改锁对象，保证并发安全
  private final Object haServiceStateLock = new Object();
  // ResourceManager主实例引用
  private ResourceManager resourceManager;
  // RM时间线数据收集管理器实例
  private RMTimelineCollectorManager timelineCollectorManager;

  /**
   * 获取ResourceManager主实例引用。
   * @return ResourceManager实例
   */
  public ResourceManager getResourceManager() {
    return resourceManager;
  }

  /**
   * 设置ResourceManager主实例引用。
   * @param rm ResourceManager实例
   */
  public void setResourceManager(ResourceManager rm) {
    this.resourceManager = rm;
  }

  /**
   * 获取配置提供者实例。
   * @return 配置提供者
   */
  public ConfigurationProvider getConfigurationProvider() {
    return this.configurationProvider;
  }

  /**
   * 设置配置提供者实例。
   * @param configurationProvider 配置提供者实例
   */
  public void setConfigurationProvider(
      ConfigurationProvider configurationProvider) {
    this.configurationProvider = configurationProvider;
  }

  /**
   * 获取RM事件分发器。
   * @return 事件分发器实例
   */
  public Dispatcher getDispatcher() {
    return this.rmDispatcher;
  }

  /**
   * 设置RM事件分发器。
   * @param dispatcher 事件分发器实例
   */
  void setDispatcher(Dispatcher dispatcher) {
    this.rmDispatcher = dispatcher;
  }

  /**
   * 获取主节点选举服务实例。
   * @return 内嵌选举器实例
   */
  public EmbeddedElector getLeaderElectorService() {
    return this.elector;
  }

  /**
   * 设置主节点选举服务实例。
   * @param embeddedElector 内嵌选举器实例
   */
  public void setLeaderElectorService(EmbeddedElector embeddedElector) {
    this.elector = embeddedElector;
  }

  /**
   * 获取RM管理服务实例。
   * @return 管理服务实例
   */
  public AdminService getRMAdminService() {
    return this.adminService;
  }

  /**
   * 设置RM管理服务实例。
   * @param service 管理服务实例
   */
  void setRMAdminService(AdminService service) {
    this.adminService = service;
  }

  /**
   * 设置是否开启RM高可用。
   * @param rmHAEnabled 是否开启高可用
   */
  void setHAEnabled(boolean rmHAEnabled) {
    this.isHAEnabled = rmHAEnabled;
  }

  /**
   * 查询是否开启了RM高可用。
   * @return true表示开启高可用，false表示未开启
   */
  public boolean isHAEnabled() {
    return isHAEnabled;
  }

  /**
   * 获取当前RM高可用服务状态。
   * @return HA服务状态
   */
  public HAServiceState getHAServiceState() {
    synchronized (haServiceStateLock) {
      return haServiceState;
    }
  }

  /**
   * 设置当前RM高可用服务状态。
   * @param serviceState HA服务状态
   */
  void setHAServiceState(HAServiceState serviceState) {
    synchronized (haServiceStateLock) {
      this.haServiceState = serviceState;
    }
  }

  /**
   * 获取RM应用历史写入器实例。
   * @return 应用历史写入器
   */
  public RMApplicationHistoryWriter getRMApplicationHistoryWriter() {
    return this.rmApplicationHistoryWriter;
  }

  /**
   * 设置RM应用历史写入器实例。
   * @param applicationHistoryWriter 应用历史写入器实例
   */
  public void setRMApplicationHistoryWriter(
      RMApplicationHistoryWriter applicationHistoryWriter) {
    this.rmApplicationHistoryWriter = applicationHistoryWriter;
  }

  /**
   * 设置系统指标发布器实例。
   * @param metricsPublisher 系统指标发布器实例
   */
  public void setSystemMetricsPublisher(
      SystemMetricsPublisher metricsPublisher) {
    this.systemMetricsPublisher = metricsPublisher;
  }

  /**
   * 获取系统指标发布器实例。
   * @return 系统指标发布器
   */
  public SystemMetricsPublisher getSystemMetricsPublisher() {
    return this.systemMetricsPublisher;
  }

  /**
   * 获取YARN配置对象。
   * @return YARN配置
   */
  public Configuration getYarnConfiguration() {
    return this.yarnConfiguration;
  }

  /**
   * 设置YARN配置对象。
   * @param yarnConfiguration YARN配置对象
   */
  public void setYarnConfiguration(Configuration yarnConfiguration) {
    this.yarnConfiguration = yarnConfiguration;
  }

  /**
   * 获取RM时间线数据收集管理器实例。
   * @return 时间线收集管理器
   */
  public RMTimelineCollectorManager getRMTimelineCollectorManager() {
    return timelineCollectorManager;
  }

  /**
   * 设置RM时间线数据收集管理器实例。
   * @param collectorManager 时间线收集管理器实例
   */
  public void setRMTimelineCollectorManager(
      RMTimelineCollectorManager collectorManager) {
    this.timelineCollectorManager = collectorManager;
  }

  /**
   * 获取HA模式下Zookeeper连接状态，用于监控展示。
   * @return Zookeeper连接状态描述字符串
   */
  public String getHAZookeeperConnectionState() {
    if (elector == null) {
      return "Could not find leader elector. Verify both HA and automatic "
          + "failover are enabled.";
    } else {
      return elector.getZookeeperConnectionState();
    }
  }
}