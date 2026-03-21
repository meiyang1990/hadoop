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

package org.apache.hadoop.yarn.server.nodemanager;

import java.io.IOException;
import java.net.ConnectException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataInputByteBuffer;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.VersionUtil;
import org.apache.hadoop.util.concurrent.SubjectInheritingThread;
import org.apache.hadoop.yarn.api.protocolrecords.SignalContainerRequest;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeLabel;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceUtilization;
import org.apache.hadoop.yarn.api.records.NodeAttribute;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factories.impl.pb.RecordFactoryPBImpl;
import org.apache.hadoop.yarn.nodelabels.CommonNodeLabelsManager;
import org.apache.hadoop.yarn.nodelabels.NodeLabelUtil;
import org.apache.hadoop.yarn.server.api.ResourceManagerConstants;
import org.apache.hadoop.yarn.server.api.ResourceTracker;
import org.apache.hadoop.yarn.server.api.ServerRMProxy;
import org.apache.hadoop.yarn.server.api.protocolrecords.LogAggregationReport;
import org.apache.hadoop.yarn.server.api.protocolrecords.NMContainerStatus;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.UnRegisterNodeManagerRequest;
import org.apache.hadoop.yarn.server.api.records.AppCollectorData;
import org.apache.hadoop.yarn.server.api.records.ContainerQueuingLimit;
import org.apache.hadoop.yarn.server.api.records.MasterKey;
import org.apache.hadoop.yarn.server.api.records.NodeAction;
import org.apache.hadoop.yarn.server.api.records.NodeHealthStatus;
import org.apache.hadoop.yarn.server.api.records.NodeStatus;
import org.apache.hadoop.yarn.server.api.records.OpportunisticContainersStatus;
import org.apache.hadoop.yarn.server.nodemanager.NodeManager.NMContext;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.Application;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationState;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.monitor.ContainersMonitor;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.ResourcePlugin;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.ResourcePluginManager;
import org.apache.hadoop.yarn.server.nodemanager.health.NodeHealthCheckerService;
import org.apache.hadoop.yarn.server.nodemanager.metrics.NodeManagerMetrics;
import org.apache.hadoop.yarn.server.nodemanager.nodelabels.NodeAttributesProvider;
import org.apache.hadoop.yarn.server.nodemanager.nodelabels.NodeLabelsProvider;
import org.apache.hadoop.yarn.server.nodemanager.timelineservice.NMTimelinePublisher;
import org.apache.hadoop.yarn.server.nodemanager.util.NodeManagerHardwareUtils;
import org.apache.hadoop.yarn.server.utils.YarnServerBuilderUtils;
import org.apache.hadoop.yarn.util.ResourceCalculatorPlugin;
import org.apache.hadoop.yarn.util.YarnVersionInfo;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.VisibleForTesting;

/**
 * NodeManager节点状态更新器实现，负责向ResourceManager定期发送心跳、
 * 注册节点、上报容器状态，并处理RM返回的指令（关闭节点、重新同步、清理容器等）
 */
public class NodeStatusUpdaterImpl extends AbstractService implements
    NodeStatusUpdater {

  public static final String YARN_NODEMANAGER_DURATION_TO_TRACK_STOPPED_CONTAINERS =
      YarnConfiguration.NM_PREFIX + "duration-to-track-stopped-containers";

  private static final Logger LOG =
       LoggerFactory.getLogger(NodeStatusUpdaterImpl.class);

  private final Object heartbeatMonitor = new Object();
  private final Object shutdownMonitor = new Object();

  private final Context context;
  private final Dispatcher dispatcher;

  private NodeId nodeId;
  private long nextHeartBeatInterval;
  private ResourceTracker resourceTracker;
  private Resource totalResource;
  private Resource physicalResource;
  private int httpPort;
  private String nodeManagerVersionId;
  private String minimumResourceManagerVersion;
  private volatile boolean isStopped;
  private boolean tokenKeepAliveEnabled;
  private long tokenRemovalDelayMs;
  /** Keeps track of when the next keep alive request should be sent for an app*/
  private Map<ApplicationId, Long> appTokenKeepAliveMap =
      new HashMap<ApplicationId, Long>();
  private Random keepAliveDelayRandom = new Random();
  // 跟踪NM上最近停止的容器，避免RM重复停止已完成容器时NM输出不必要的异常日志
  private final Map<ContainerId, Long> recentlyStoppedContainers;
  // 保存已上报但未被RM确认的已完成容器，心跳失败时会重新上报
  private final Map<ContainerId, ContainerStatus> pendingCompletedContainers;
  // 最近停止容器的跟踪时长
  private long durationToTrackStoppedContainers;

  private boolean logAggregationEnabled;

  private final List<LogAggregationReport> logAggregationReportForAppsTempList;

  private final NodeHealthCheckerService healthChecker;
  private final NodeManagerMetrics metrics;

  private Runnable statusUpdaterRunnable;
  private Thread  statusUpdater;
  private boolean failedToConnect = false;
  private long rmIdentifier = ResourceManagerConstants.RM_INVALID_IDENTIFIER;
  private boolean registeredWithRM = false;
  Set<ContainerId> pendingContainersToRemove = new HashSet<ContainerId>();

  private NMNodeLabelsHandler nodeLabelsHandler;
  private NMNodeAttributesHandler nodeAttributesHandler;
  private NodeLabelsProvider nodeLabelsProvider;
  private NodeAttributesProvider nodeAttributesProvider;
  private long tokenSequenceNo;
  private boolean timelineServiceV2Enabled;

  /**
   * 构造函数，初始化节点状态更新器基础组件
   * @param context NM上下文对象
   * @param dispatcher 事件分发器
   * @param healthChecker 节点健康检查服务
   * @param metrics NM指标统计
   */
  public NodeStatusUpdaterImpl(Context context, Dispatcher dispatcher,
      NodeHealthCheckerService healthChecker, NodeManagerMetrics metrics) {
    super(NodeStatusUpdaterImpl.class.getName());
    this.healthChecker = healthChecker;
    this.context = context;
    this.dispatcher = dispatcher;
    this.metrics = metrics;
    this.recentlyStoppedContainers = new LinkedHashMap<ContainerId, Long>();
    this.pendingCompletedContainers =
        new HashMap<ContainerId, ContainerStatus>();
    this.logAggregationReportForAppsTempList =
        new ArrayList<LogAggregationReport>();
  }

  @Override
  public void setNodeAttributesProvider(NodeAttributesProvider provider) {
    this.nodeAttributesProvider = provider;
  }

  @Override
  public void setNodeLabelsProvider(NodeLabelsProvider provider) {
    this.nodeLabelsProvider = provider;
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    // 获取配置的节点总资源
    this.totalResource = NodeManagerHardwareUtils.getNodeResources(conf);
    long memoryMb = totalResource.getMemorySize();
    float vMemToPMem =
        conf.getFloat(
            YarnConfiguration.NM_VMEM_PMEM_RATIO,
            YarnConfiguration.DEFAULT_NM_VMEM_PMEM_RATIO);
    long virtualMemoryMb = (long)Math.ceil(memoryMb * vMemToPMem);
    int virtualCores = totalResource.getVirtualCores();

    // 通过资源插件更新配置的资源
    updateConfiguredResourcesViaPlugins(totalResource);

    LOG.info("Nodemanager resources is set to: {}.", totalResource);

    // 将节点资源添加到指标统计
    metrics.addResource(totalResource);

    // 获取节点实际物理资源
    long physicalMemoryMb = memoryMb;
    int physicalCores = virtualCores;
    ResourceCalculatorPlugin rcp =
        ResourceCalculatorPlugin.getNodeResourceMonitorPlugin(conf);
    if (rcp != null) {
      physicalMemoryMb = rcp.getPhysicalMemorySize() / (1024 * 1024);
      physicalCores = rcp.getNumProcessors();
    }
    this.physicalResource =
        Resource.newInstance(physicalMemoryMb, physicalCores);

    // 初始化令牌保活配置
    this.tokenKeepAliveEnabled = isTokenKeepAliveEnabled(conf);
    this.tokenRemovalDelayMs =
        conf.getInt(YarnConfiguration.RM_NM_EXPIRY_INTERVAL_MS,
            YarnConfiguration.DEFAULT_RM_NM_EXPIRY_INTERVAL_MS);

    // 获取RM最低兼容版本配置
    this.minimumResourceManagerVersion = conf.get(
        YarnConfiguration.NM_RESOURCEMANAGER_MINIMUM_VERSION,
        YarnConfiguration.DEFAULT_NM_RESOURCEMANAGER_MINIMUM_VERSION);

    // 创建节点标签和节点属性处理器
    nodeLabelsHandler =
        createNMNodeLabelsHandler(nodeLabelsProvider);
    nodeAttributesHandler =
        createNMNodeAttributesHandler(nodeAttributesProvider);

    // 读取最近停止容器跟踪时长配置
    durationToTrackStoppedContainers =
        conf.getLong(YARN_NODEMANAGER_DURATION_TO_TRACK_STOPPED_CONTAINERS,
          600000);
    if (durationToTrackStoppedContainers < 0) {
      String message = "Invalid configuration for "
        + YARN_NODEMANAGER_DURATION_TO_TRACK_STOPPED_CONTAINERS + " default "
          + "value is 10Min(600000).";
      LOG.error(message);
      throw new YarnException(message);
    }
    LOG.debug("{} :{}", YARN_NODEMANAGER_DURATION_TO_TRACK_STOPPED_CONTAINERS,
        durationToTrackStoppedContainers);
    super.serviceInit(conf);
    LOG.info("Initialized nodemanager with : physical-memory={} virtual-memory={} " +
        "virtual-cores={}.", memoryMb, virtualMemoryMb, virtualCores);

    // 初始化日志聚合和时间线服务配置
    this.logAggregationEnabled =
        conf.getBoolean(YarnConfiguration.LOG_AGGREGATION_ENABLED,
          YarnConfiguration.DEFAULT_LOG_AGGREGATION_ENABLED);
    this.timelineServiceV2Enabled = YarnConfiguration.
        timelineServiceV2Enabled(conf);

  }

  @Override
  protected void serviceStart() throws Exception {

    // NodeManager是最后启动的服务，此时NodeId已经可用
    this.nodeId = this.context.getNodeId();
    LOG.info("Node ID assigned is : {}.", this.nodeId);
    this.httpPort = this.context.getHttpPort();
    this.nodeManagerVersionId = YarnVersionInfo.getVersion();
    try {
      // 注册必须放在启动阶段，这样ContainerManager才能获取认证容器令牌所需的NM令牌
      this.resourceTracker = getRMClient();
      registerWithRM();
      super.serviceStart();
      startStatusUpdater();
    } catch (Exception e) {
      String errorMessage = "Unexpected error starting NodeStatusUpdater";
      LOG.error(errorMessage, e);
      throw new YarnRuntimeException(e);
    }
  }

  @Override
  protected void serviceStop() throws Exception {
    // isStopped检查避免重复注销
    synchronized(shutdownMonitor) {
      if (this.registeredWithRM && !this.isStopped
          && !isNMUnderSupervisionWithRecoveryEnabled()
          && !context.getDecommissioned() && !failedToConnect) {
        unRegisterNM();
      }
      // 中断状态更新线程
      this.isStopped = true;
      stopRMProxy();
      super.serviceStop();
    }
  }

  /**
   * 检查NM是否开启了恢复且处于被托管模式，该模式下退出不主动向RM注销
   * @return 是否处于托管恢复模式
   */
  private boolean isNMUnderSupervisionWithRecoveryEnabled() {
    Configuration config = getConfig();
    return config.getBoolean(YarnConfiguration.NM_RECOVERY_ENABLED,
        YarnConfiguration.DEFAULT_NM_RECOVERY_ENABLED)
        && config.getBoolean(YarnConfiguration.NM_RECOVERY_SUPERVISED,
            YarnConfiguration.DEFAULT_NM_RECOVERY_SUPERVISED);
  }

  /**
   * 向RM发送NM注销请求
   */
  private void unRegisterNM() {
    RecordFactory recordFactory = RecordFactoryPBImpl.get();
    UnRegisterNodeManagerRequest request = recordFactory
        .newRecordInstance(UnRegisterNodeManagerRequest.class);
    request.setNodeId(this.nodeId);
    try {
      resourceTracker.unRegisterNodeManager(request);
      LOG.info("Successfully Unregistered the Node {} with ResourceManager.", this.nodeId);
    } catch (Exception e) {
      LOG.warn("Unregistration of the Node {} failed.", this.nodeId, e);
    }
  }

  /**
   * 重启状态更新器并重新向RM注册NM，用于处理重新同步场景
   */
  protected void rebootNodeStatusUpdaterAndRegisterWithRM() {
    synchronized(shutdownMonitor) {
      if(this.isStopped) {
        LOG.info("Currently being shutdown. Aborting reboot");
        return;
      }
      this.isStopped = true;
      sendOutofBandHeartBeat();
      try {
        statusUpdater.join();
        registerWithRM();
        statusUpdater = new SubjectInheritingThread(statusUpdaterRunnable, "Node Status Updater");
        this.isStopped = false;
        statusUpdater.start();
        LOG.info("NodeStatusUpdater thread is reRegistered and restarted");
      } catch (Exception e) {
        String errorMessage = "Unexpected error rebooting NodeStatusUpdater";
        LOG.error(errorMessage, e);
        throw new YarnRuntimeException(e);
      }
    }
  }

  @VisibleForTesting
  protected void stopRMProxy() {
    if(this.resourceTracker != null) {
      RPC.stopProxy(this.resourceTracker);
    }
  }

  @Private
  protected boolean isTokenKeepAliveEnabled(Configuration conf) {
    return conf.getBoolean(YarnConfiguration.LOG_AGGREGATION_ENABLED,
        YarnConfiguration.DEFAULT_LOG_AGGREGATION_ENABLED)
        && UserGroupInformation.isSecurityEnabled();
  }

  @VisibleForTesting
  protected ResourceTracker getRMClient() throws IOException {
    Configuration conf = getConfig();
    return ServerRMProxy.createRMProxy(conf, ResourceTracker.class);
  }

  /**
   * 通过资源插件更新配置的节点资源，允许自定义资源类型扩展
   * @param configuredResource 初始配置的节点资源
   * @throws YarnException 资源更新失败时抛出异常
   */
  private void updateConfiguredResourcesViaPlugins(
      Resource configuredResource) throws YarnException {
    ResourcePluginManager pluginManager = context.getResourcePluginManager();
    if (pluginManager != null && pluginManager.getNameToPlugins() != null) {
      // 遍历所有资源插件更新节点资源
      for (ResourcePlugin resourcePlugin : pluginManager.getNameToPlugins()
          .values()) {
        if (resourcePlugin.getNodeResourceHandlerInstance() != null) {
          resourcePlugin.getNodeResourceHandlerInstance()
              .updateConfiguredResource(configuredResource);
        }
      }
    }
  }

  @VisibleForTesting
  protected void registerWithRM()
      throws YarnException, IOException {
    RegisterNodeManagerResponse regNMResponse;
    // 获取注册