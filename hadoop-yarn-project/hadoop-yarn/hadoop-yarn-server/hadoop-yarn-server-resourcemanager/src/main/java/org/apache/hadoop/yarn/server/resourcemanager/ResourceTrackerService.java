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

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.security.authorize.PolicyProvider;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.VersionUtil;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.NodeAttribute;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.nodelabels.NodeLabelUtil;
import org.apache.hadoop.yarn.server.api.ResourceTracker;
import org.apache.hadoop.yarn.server.api.protocolrecords.NMContainerStatus;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.UnRegisterNodeManagerRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.UnRegisterNodeManagerResponse;
import org.apache.hadoop.yarn.server.api.records.AppCollectorData;
import org.apache.hadoop.yarn.server.api.records.MasterKey;
import org.apache.hadoop.yarn.server.api.records.NodeAction;
import org.apache.hadoop.yarn.server.api.records.NodeStatus;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.NodeLabelsUtils;
import org.apache.hadoop.yarn.server.resourcemanager.resource.DynamicResourceConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.event.RMAppAttemptContainerFinishedEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeReconnectEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeStartedEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeStatusEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.security.NMTokenSecretManagerInRM;
import org.apache.hadoop.yarn.server.resourcemanager.security.RMContainerTokenSecretManager;
import org.apache.hadoop.yarn.server.resourcemanager.security.authorize.RMPolicyProvider;
import org.apache.hadoop.yarn.server.utils.YarnServerBuilderUtils;
import org.apache.hadoop.yarn.util.RackResolver;
import org.apache.hadoop.yarn.util.YarnVersionInfo;

import org.apache.hadoop.classification.VisibleForTesting;

/**
 * ResourceTrackerService 是YARN ResourceManager端处理NodeManager心跳和节点注册的核心服务，
 * 实现了ResourceTracker接口，负责处理NodeManager的注册、心跳、注销等请求，维护节点生命周期状态。
 */
public class ResourceTrackerService extends AbstractService implements
    ResourceTracker {

  private static final Logger LOG =
      LoggerFactory.getLogger(ResourceTrackerService.class);

  private static final RecordFactory recordFactory = 
    RecordFactoryProvider.getRecordFactory(null);

  private final RMContext rmContext;
  private final NodesListManager nodesListManager;
  private final NMLivelinessMonitor nmLivelinessMonitor;
  private final RMContainerTokenSecretManager containerTokenSecretManager;
  private final NMTokenSecretManagerInRM nmTokenSecretManager;

  private final ReadLock readLock;
  private final WriteLock writeLock;

  private long nextHeartBeatInterval;
  private boolean heartBeatIntervalScalingEnable;
  private long heartBeatIntervalMin;
  private long heartBeatIntervalMax;
  private float heartBeatIntervalSpeedupFactor;
  private float heartBeatIntervalSlowdownFactor;


  private Server server;
  private InetSocketAddress resourceTrackerAddress;
  private String minimumNodeManagerVersion;

  private int minAllocMb;
  private int minAllocVcores;

  private DecommissioningNodesWatcher decommissioningWatcher;

  private boolean isDistributedNodeLabelsConf;
  private boolean isDelegatedCentralizedNodeLabelsConf;
  private DynamicResourceConfiguration drConf;

  private final AtomicLong timelineCollectorVersion = new AtomicLong(0);
  private boolean checkIpHostnameInRegistration;
  private boolean timelineServiceV2Enabled;

  /**
   * 构造ResourceTrackerService实例，初始化核心依赖和锁、退役节点监视器
   */
  public ResourceTrackerService(RMContext rmContext,
      NodesListManager nodesListManager,
      NMLivelinessMonitor nmLivelinessMonitor,
      RMContainerTokenSecretManager containerTokenSecretManager,
      NMTokenSecretManagerInRM nmTokenSecretManager) {
    super(ResourceTrackerService.class.getName());
    this.rmContext = rmContext;
    this.nodesListManager = nodesListManager;
    this.nmLivelinessMonitor = nmLivelinessMonitor;
    this.containerTokenSecretManager = containerTokenSecretManager;
    this.nmTokenSecretManager = nmTokenSecretManager;
    ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    this.readLock = lock.readLock();
    this.writeLock = lock.writeLock();
    this.decommissioningWatcher = new DecommissioningNodesWatcher(rmContext);
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    // 从配置中读取ResourceTracker服务绑定地址
    resourceTrackerAddress = conf.getSocketAddr(
        YarnConfiguration.RM_BIND_HOST,
        YarnConfiguration.RM_RESOURCE_TRACKER_ADDRESS,
        YarnConfiguration.DEFAULT_RM_RESOURCE_TRACKER_ADDRESS,
        YarnConfiguration.DEFAULT_RM_RESOURCE_TRACKER_PORT);

    // 初始化机架解析器
    RackResolver.init(conf);

    // 读取是否开启节点注册时IP主机名校验配置
    checkIpHostnameInRegistration = conf.getBoolean(
        YarnConfiguration.RM_NM_REGISTRATION_IP_HOSTNAME_CHECK_KEY,
        YarnConfiguration.DEFAULT_RM_NM_REGISTRATION_IP_HOSTNAME_CHECK_KEY);
    // 读取最小容器分配内存配置
    minAllocMb = conf.getInt(
        YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB,
        YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_MB);
    // 读取最小容器分配CPU核数配置
    minAllocVcores = conf.getInt(
        YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES,
        YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES);

    // 读取允许的最小NodeManager版本配置
    minimumNodeManagerVersion = conf.get(
        YarnConfiguration.RM_NODEMANAGER_MINIMUM_VERSION,
        YarnConfiguration.DEFAULT_RM_NODEMANAGER_MINIMUM_VERSION);
    // 读取是否开启Timeline Service V2配置
    timelineServiceV2Enabled =  YarnConfiguration.
        timelineServiceV2Enabled(conf);

    // 如果开启了节点标签功能，读取节点标签配置模式
    if (YarnConfiguration.areNodeLabelsEnabled(conf)) {
      isDistributedNodeLabelsConf =
          YarnConfiguration.isDistributedNodeLabelConfiguration(conf);
      isDelegatedCentralizedNodeLabelsConf =
          YarnConfiguration.isDelegatedCentralizedNodeLabelConfiguration(conf);
    }
    // 更新心跳相关配置
    updateHeartBeatConfiguration(conf);
    // 加载动态资源配置
    loadDynamicResourceConfiguration(conf);
    // 初始化退役节点监视器
    decommissioningWatcher.init(conf);
    super.serviceInit(conf);
  }

  /**
   * 从dynamic-resources.xml加载动态资源配置。
   * @param conf 基础配置对象
   * @throws IOException 加载配置IO异常
   */
  public void loadDynamicResourceConfiguration(Configuration conf)
      throws IOException {
    try {
      // 从配置提供者获取动态资源配置输入流
      InputStream drInputStream = this.rmContext.getConfigurationProvider()
          .getConfigurationInputStream(conf,
          YarnConfiguration.DR_CONFIGURATION_FILE);
      // 此处不需要写锁，因为初始化阶段还没有并发读写
      if (drInputStream != null) {
        this.drConf = new DynamicResourceConfiguration(conf, drInputStream);
      } else {
        this.drConf = new DynamicResourceConfiguration(conf);
      }
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  /**
   * 更新动态资源配置。
   * @param conf 新的动态资源配置对象
   */
  public void updateDynamicResourceConfiguration(
      DynamicResourceConfiguration conf) {
    this.writeLock.lock();
    try {
      this.drConf = conf;
    } finally {
      this.writeLock.unlock();
    }
  }

  /**
   * 更新心跳相关配置，进行参数校验。
   * @param conf Yarn配置对象
   */
  public void updateHeartBeatConfiguration(Configuration conf) {
    this.writeLock.lock();
    try {
      // 读取默认心跳间隔
      nextHeartBeatInterval =
          conf.getLong(YarnConfiguration.RM_NM_HEARTBEAT_INTERVAL_MS,
              YarnConfiguration.DEFAULT_RM_NM_HEARTBEAT_INTERVAL_MS);
      // 读取是否开启心跳间隔动态调整
      heartBeatIntervalScalingEnable =
          conf.getBoolean(
              YarnConfiguration.RM_NM_HEARTBEAT_INTERVAL_SCALING_ENABLE,
              YarnConfiguration.
                  DEFAULT_RM_NM_HEARTBEAT_INTERVAL_SCALING_ENABLE);
      // 读取心跳间隔最小值
      heartBeatIntervalMin =
          conf.getLong(YarnConfiguration.RM_NM_HEARTBEAT_INTERVAL_MIN_MS,
              YarnConfiguration.DEFAULT_RM_NM_HEARTBEAT_INTERVAL_MIN_MS);
      // 读取心跳间隔最大值
      heartBeatIntervalMax =
          conf.getLong(YarnConfiguration.RM_NM_HEARTBEAT_INTERVAL_MAX_MS,
              YarnConfiguration.DEFAULT_RM_NM_HEARTBEAT_INTERVAL_MAX_MS);
      // 读取心跳加速因子
      heartBeatIntervalSpeedupFactor =
          conf.getFloat(
              YarnConfiguration.RM_NM_HEARTBEAT_INTERVAL_SPEEDUP_FACTOR,
              YarnConfiguration.
                  DEFAULT_RM_NM_HEARTBEAT_INTERVAL_SPEEDUP_FACTOR);
      // 读取心跳减速因子
      heartBeatIntervalSlowdownFactor =
          conf.getFloat(
              YarnConfiguration.RM_NM_HEARTBEAT_INTERVAL_SLOWDOWN_FACTOR,
              YarnConfiguration.
                  DEFAULT_RM_NM_HEARTBEAT_INTERVAL_SLOWDOWN_FACTOR);

      // 校验心跳间隔合法性，不合法则使用默认值
      if (nextHeartBeatInterval <= 0) {
        LOG.warn("HeartBeat interval: " + nextHeartBeatInterval
            + " must be greater than 0, using default.");
        nextHeartBeatInterval =
            YarnConfiguration.DEFAULT_RM_NM_HEARTBEAT_INTERVAL_MS;
      }

      // 如果开启动态调整，校验参数范围合法性
      if (heartBeatIntervalScalingEnable) {
        if (heartBeatIntervalMin <= 0
            || heartBeatIntervalMin > heartBeatIntervalMax
            || nextHeartBeatInterval < heartBeatIntervalMin
            || nextHeartBeatInterval > heartBeatIntervalMax) {
          LOG.warn("Invalid NM Heartbeat Configuration. "
              + "Required: 0 < minimum <= interval <= maximum. Got: 0 < "
              + heartBeatIntervalMin + " <= "
              + nextHeartBeatInterval + " <= "
              + heartBeatIntervalMax
              + " Setting min and max to configured interval.");
          heartBeatIntervalMin = nextHeartBeatInterval;
          heartBeatIntervalMax = nextHeartBeatInterval;
        }
        // 校验缩放因子合法性，不合法则使用默认值
        if (heartBeatIntervalSpeedupFactor < 0
            || heartBeatIntervalSlowdownFactor < 0) {
          LOG.warn(
              "Heartbeat scaling factors must be >= 0 "
                  + " SpeedupFactor:" + heartBeatIntervalSpeedupFactor
                  + " SlowdownFactor:" + heartBeatIntervalSlowdownFactor
                  + ". Using Defaults");
          heartBeatIntervalSlowdownFactor =
              YarnConfiguration.
                  DEFAULT_RM_NM_HEARTBEAT_INTERVAL_SLOWDOWN_FACTOR;
          heartBeatIntervalSpeedupFactor =
              YarnConfiguration.DEFAULT_RM_NM_HEARTBEAT_INTERVAL_SPEEDUP_FACTOR;
        }
        LOG.info("Heartbeat Scaling Configuration: "
            + " defaultInterval:" + nextHeartBeatInterval
            + " minimumInterval:" + heartBeatIntervalMin
            + " maximumInterval:" + heartBeatIntervalMax
            + " speedupFactor:" + heartBeatIntervalSpeedupFactor
            + " slowdownFactor:" + heartBeatIntervalSlowdownFactor);
      }
    } finally {
      this.writeLock.unlock();
    }
  }

  @Override
  protected void serviceStart() throws Exception {
    super.serviceStart();
    // 安全开启时，NodeManager会通过Kerberos认证，因此这里不需要传入secretManager
    Configuration conf = getConfig();
    YarnRPC rpc = YarnRPC.create(conf);
    // 创建并启动ResourceTracker RPC服务器
    this.server = rpc.getServer(
        ResourceTracker.class, this, resourceTrackerAddress, conf, null,
        conf.getInt(YarnConfiguration.RM_RESOURCE_TRACKER_CLIENT_THREAD_COUNT,
            YarnConfiguration.DEFAULT_RM_RESOURCE_TRACKER_CLIENT_THREAD_COUNT));

    // 如果开启服务授权，加载ACL配置
    if (conf.getBoolean(
        CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHORIZATION,
        false)) {
      InputStream inputStream =
          this.rmContext.getConfigurationProvider()
              .getConfigurationInputStream(conf,
                  YarnConfiguration.HADOOP_POLICY_CONFIGURATION_FILE);
      if (inputStream != null) {
        conf.addResource(inputStream);
      }
      refreshServiceAcls(conf, RMPolicyProvider.getInstance());
    }

    this.server.start();
    // 更新配置中连接地址，适配端口自动分配场景
    conf.updateConnectAddr(YarnConfiguration.RM_BIND_HOST,
        YarnConfiguration.RM_RESOURCE_TRACKER_ADDRESS,
        YarnConfiguration.DEFAULT_RM_RESOURCE_TRACKER_ADDRESS,
        server.getListenerAddress());
  }

  @Override
  protected void serviceStop() throws Exception {
    decommissioningWatcher.stop();
    if (this.server != null) {
      this.server.stop();
    }

    super.serviceStop();
  }

  /**
   * 处理NodeManager上报的容器状态，若为AM主容器完成则发送完成事件。
   */
  @SuppressWarnings("unchecked")
  @VisibleForTesting
  void handleNMContainerStatus(NMContainerStatus containerStatus, NodeId nodeId) {
    ApplicationAttemptId appAttemptId =
        containerStatus.getContainerId().getApplicationAttemptId();
    RMApp rmApp =
        rmContext.getRMApps().get(appAttemptId.getApplicationId());
    if (rmApp == null) {
      LOG.error("Received finished container : "
          + containerStatus.getContainerId()
          + " for unknown application " + appAttemptId.getApplicationId()
          + " Skipping.");
      return;