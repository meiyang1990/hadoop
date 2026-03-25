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

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.ipc.CallerContext;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.util.MBeans;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.JvmPauseMonitor;
import org.apache.hadoop.yarn.server.nodemanager.health.NodeHealthCheckerService;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.util.ShutdownHookManager;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.concurrent.SubjectInheritingThread;
import org.apache.hadoop.yarn.YarnUncaughtExceptionHandler;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.metrics.GenericEventTypeMetrics;
import org.apache.hadoop.yarn.server.api.protocolrecords.LogAggregationReport;
import org.apache.hadoop.yarn.server.api.records.AppCollectorData;
import org.apache.hadoop.yarn.server.api.records.NodeHealthStatus;
import org.apache.hadoop.yarn.server.nodemanager.collectormanager.NMCollectorService;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.AuxServices;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.ContainerManager;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.ContainerManagerImpl;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.Application;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationState;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerImpl;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerState;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.ResourcePluginManager;
import org.apache.hadoop.yarn.server.nodemanager.logaggregation.tracker.NMLogAggregationStatusTracker;
import org.apache.hadoop.yarn.server.nodemanager.metrics.NodeManagerMetrics;
import org.apache.hadoop.yarn.server.nodemanager.nodelabels.ConfigurationNodeLabelsProvider;
import org.apache.hadoop.yarn.server.nodemanager.nodelabels.NodeLabelsProvider;
import org.apache.hadoop.yarn.server.nodemanager.nodelabels.ScriptBasedNodeLabelsProvider;
import org.apache.hadoop.yarn.server.nodemanager.nodelabels.ScriptBasedNodeAttributesProvider;
import org.apache.hadoop.yarn.server.nodemanager.nodelabels.NodeAttributesProvider;
import org.apache.hadoop.yarn.server.nodemanager.nodelabels.ConfigurationNodeAttributesProvider;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMLeveldbStateStoreService;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMNullStateStoreService;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMStateStoreService;
import org.apache.hadoop.yarn.server.nodemanager.security.NMContainerTokenSecretManager;
import org.apache.hadoop.yarn.server.nodemanager.security.NMTokenSecretManagerInNM;
import org.apache.hadoop.yarn.server.nodemanager.timelineservice.NMTimelinePublisher;
import org.apache.hadoop.yarn.server.nodemanager.webapp.WebServer;
import org.apache.hadoop.yarn.server.scheduler.DistributedOpportunisticContainerAllocator;
import org.apache.hadoop.yarn.server.scheduler.OpportunisticContainerAllocator;
import org.apache.hadoop.yarn.server.security.ApplicationACLsManager;
import org.apache.hadoop.yarn.state.MultiStateTransitionListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * YARN NodeManager 主类，负责单个节点上的容器生命周期管理、资源监控、状态上报
 * 是YARN集群中每个工作节点的核心服务，接收并执行ResourceManager分配的容器任务
 */
public class NodeManager extends CompositeService
    implements EventHandler<NodeManagerEvent>, NodeManagerMXBean {

  /**
   * NodeManager 退出状态码枚举
   */
  public enum NodeManagerStatus {
    NO_ERROR(0),
    EXCEPTION(1);

    private int exitCode;

    NodeManagerStatus(int exitCode) {
      this.exitCode = exitCode;
    }

    public int getExitCode() {
      return exitCode;
    }
  }

  /**
   * NodeManager 关闭钩子优先级
   */
  public static final int SHUTDOWN_HOOK_PRIORITY = 30;

  private static final Logger LOG =
       LoggerFactory.getLogger(NodeManager.class);
  // NodeManager启动时间戳
  private static long nmStartupTime = System.currentTimeMillis();
  protected final NodeManagerMetrics metrics = NodeManagerMetrics.create();
  private JvmPauseMonitor pauseMonitor;
  private ApplicationACLsManager aclsManager;
  private NodeHealthCheckerService nodeHealthChecker;
  private NodeLabelsProvider nodeLabelsProvider;
  private NodeAttributesProvider nodeAttributesProvider;
  private LocalDirsHandlerService dirsHandler;
  private Context context;
  private AsyncDispatcher dispatcher;
  private ContainerManagerImpl containerManager;
  // 仅在开启时间线服务v2时才会初始化NM采集服务
  private NMCollectorService nmCollectorService;
  private NodeStatusUpdater nodeStatusUpdater;
  // 标记当前是否正在与RM重新同步，避免并发同步
  private AtomicBoolean resyncingWithRM = new AtomicBoolean(false);
  private NodeResourceMonitor nodeResourceMonitor;
  private static CompositeServiceShutdownHook nodeManagerShutdownHook;
  private NMStateStoreService nmStore = null;
  
  // 标记NodeManager是否正在停止
  private AtomicBoolean isStopping = new AtomicBoolean(false);
  // 是否开启RM工作保留恢复功能
  private boolean rmWorkPreservingRestartEnabled;
  // 关闭事件后是否需要退出进程
  private boolean shouldExitOnShutdownEvent = false;
  // 是否开启NM事件分发器指标监控
  private boolean nmDispatherMetricEnabled;

  private NMLogAggregationStatusTracker nmLogAggregationStatusTracker;

  /**
   * 默认容器状态转换监听器，聚合所有自定义监听器
   */
  public static class DefaultContainerStateListener extends
      MultiStateTransitionListener
          <ContainerImpl, ContainerEvent, ContainerState>
          implements ContainerStateTransitionListener {
    @Override
    public void init(Context context) {}
  }

  /**
   * 构造NodeManager实例
   */
  public NodeManager() {
    super(NodeManager.class.getName());
  }

  /**
   * 获取NodeManager启动时间戳
   * @return 启动时间戳
   */
  public static long getNMStartupTime() {
    return nmStartupTime;
  }

  /**
   * 创建节点状态更新器实例，负责向RM定期上报节点状态
   * @param context NM上下文
   * @param dispatcher 事件分发器
   * @param healthChecker 节点健康检查服务
   * @return 节点状态更新器实例
   */
  protected NodeStatusUpdater createNodeStatusUpdater(Context context,
      Dispatcher dispatcher, NodeHealthCheckerService healthChecker) {
    return new NodeStatusUpdaterImpl(context, dispatcher, healthChecker,
        metrics);
  }

  /**
   * 根据配置创建节点属性提供者
   * @param conf 配置对象
   * @return 节点属性提供者实例
   * @throws IOException 创建失败抛出异常
   */
  protected NodeAttributesProvider createNodeAttributesProvider(
      Configuration conf) throws IOException {
    NodeAttributesProvider attributesProvider = null;
    String providerString =
        conf.get(YarnConfiguration.NM_NODE_ATTRIBUTES_PROVIDER_CONFIG, null);
    if (providerString == null || providerString.trim().length() == 0) {
      return attributesProvider;
    }
    switch (providerString.trim().toLowerCase()) {
    case YarnConfiguration.CONFIG_NODE_DESCRIPTOR_PROVIDER:
      attributesProvider = new ConfigurationNodeAttributesProvider();
      break;
    case YarnConfiguration.SCRIPT_NODE_DESCRIPTOR_PROVIDER:
      attributesProvider = new ScriptBasedNodeAttributesProvider();
      break;
    default:
      try {
        Class<? extends NodeAttributesProvider> labelsProviderClass =
            conf.getClass(YarnConfiguration.NM_NODE_ATTRIBUTES_PROVIDER_CONFIG,
                null, NodeAttributesProvider.class);
        attributesProvider = labelsProviderClass.newInstance();
      } catch (InstantiationException | IllegalAccessException
          | RuntimeException e) {
        LOG.error("Failed to create NodeAttributesProvider"
                + " based on Configuration", e);
        throw new IOException(
            "Failed to create NodeAttributesProvider : "
                + e.getMessage(), e);
      }
    }
    LOG.debug("Distributed Node Attributes is enabled with provider class"
        + " as : {}", attributesProvider.getClass());
    return attributesProvider;
  }

  /**
   * 根据配置创建节点标签提供者
   * @param conf 配置对象
   * @return 节点标签提供者实例
   * @throws IOException 创建失败抛出异常
   */
  protected NodeLabelsProvider createNodeLabelsProvider(Configuration conf)
      throws IOException {
    NodeLabelsProvider provider = null;
    String providerString =
        conf.get(YarnConfiguration.NM_NODE_LABELS_PROVIDER_CONFIG, null);
    if (providerString == null || providerString.trim().length() == 0) {
      // Seems like Distributed Node Labels configuration is not enabled
      return provider;
    }
    switch (providerString.trim().toLowerCase()) {
    case YarnConfiguration.CONFIG_NODE_DESCRIPTOR_PROVIDER:
      provider = new ConfigurationNodeLabelsProvider();
      break;
    case YarnConfiguration.SCRIPT_NODE_DESCRIPTOR_PROVIDER:
      provider = new ScriptBasedNodeLabelsProvider();
      break;
    default:
      try {
        Class<? extends NodeLabelsProvider> labelsProviderClass =
            conf.getClass(YarnConfiguration.NM_NODE_LABELS_PROVIDER_CONFIG,
                null, NodeLabelsProvider.class);
        provider = labelsProviderClass.newInstance();
      } catch (InstantiationException | IllegalAccessException
          | RuntimeException e) {
        LOG.error("Failed to create NodeLabelsProvider based on Configuration",
            e);
        throw new IOException(
            "Failed to create NodeLabelsProvider : " + e.getMessage(), e);
      }
    }
    LOG.debug("Distributed Node Labels is enabled"
        + " with provider class as : {}", provider.getClass());
    return provider;
  }

  /**
   * 创建节点资源监控器实例，负责监控节点资源使用情况
   * @return 节点资源监控器实例
   */
  protected NodeResourceMonitor createNodeResourceMonitor() {
    return new NodeResourceMonitorImpl(context);
  }

  /**
   * 创建容器管理器实例，负责容器生命周期管理
   * @param context NM上下文
   * @param exec 容器执行器
   * @param del 删除服务，负责清理容器目录
   * @param nodeStatusUpdater 节点状态更新器
   * @param aclsManager ACL权限管理器
   * @param dirsHandler 本地目录处理器
   * @return 容器管理器实例
   */
  protected ContainerManagerImpl createContainerManager(Context context,
      ContainerExecutor exec, DeletionService del,
      NodeStatusUpdater nodeStatusUpdater, ApplicationACLsManager aclsManager,
      LocalDirsHandlerService dirsHandler) {
    return new ContainerManagerImpl(context, exec, del, nodeStatusUpdater,
        metrics, dirsHandler);
  }

  /**
   * 创建NM采集器服务实例，用于时间线服务v2
   * @param ctxt NM上下文
   * @return NM采集器服务实例
   */
  protected NMCollectorService createNMCollectorService(Context ctxt) {
    return new NMCollectorService(ctxt);
  }

  /**
   * 创建Web服务实例，提供NM REST API和Web UI
   * @param nmContext NM上下文
   * @param resourceView 资源视图
   * @param aclsManager ACL权限管理器
   * @param dirsHandler 本地目录处理器
   * @return Web服务实例
   */
  protected WebServer createWebServer(Context nmContext,
      ResourceView resourceView, ApplicationACLsManager aclsManager,
      LocalDirsHandlerService dirsHandler) {
    return new WebServer(nmContext, resourceView, aclsManager, dirsHandler);
  }

  /**
   * 创建删除服务实例，负责异步删除容器和应用目录
   * @param exec 容器执行器
   * @return 删除服务实例
   */
  protected DeletionService createDeletionService(ContainerExecutor exec) {
    return new DeletionService(exec, nmStore);
  }

  /**
   * 创建NM上下文实例，保存NM运行时所有核心组件引用和状态
   * @param containerTokenSecretManager 容器令牌密钥管理器
   * @param nmTokenSecretManager NM令牌密钥管理器
   * @param stateStore 状态存储服务
   * @param isDistSchedulerEnabled 是否启用分布式调度
   * @param conf 配置对象
   * @return NM上下文实例
   */
  protected NMContext createNMContext(
      NMContainerTokenSecretManager containerTokenSecretManager,
      NMTokenSecretManagerInNM nmTokenSecretManager,
      NMStateStoreService stateStore, boolean isDistSchedulerEnabled,
      Configuration conf) {
    List<ContainerStateTransitionListener> listeners =
        conf.getInstances(
            YarnConfiguration.NM_CONTAINER_STATE_TRANSITION_LISTENERS,
        ContainerStateTransitionListener.class);
    NMContext nmContext = new NMContext(containerTokenSecretManager,
        nmTokenSecretManager, dirsHandler, aclsManager, stateStore,
        isDistSchedulerEnabled, conf);
    nmContext.setNodeManagerMetrics(metrics);
    DefaultContainerStateListener defaultListener =
        new DefaultContainerStateListener();
    nmContext.setContainerStateTransitionListener(defaultListener);
    defaultListener.init(nmContext);
    for (ContainerStateTransitionListener listener : listeners) {
      listener.init(nmContext);
      defaultListener.addListener(listener);
    }
    return nmContext;
  }

  /**
   * 安全登录，获取Kerberos票据
   * @throws IOException 登录失败抛出异常
   */
  protected void doSecureLogin() throws IOException {
    SecurityUtil.login(getConfig(), YarnConfiguration.NM_KEYTAB,
        YarnConfiguration.NM_PRINCIPAL);
  }

  /**
   * 初始化并启动恢复状态存储，支持NM重启后恢复容器状态
   * @param conf 配置对象
   * @throws IOException 初始化失败抛出异常
   */
  private void initAndStartRecoveryStore(Configuration conf)
      throws IOException {
    boolean recoveryEnabled = conf.getBoolean(
        YarnConfiguration.NM_RECOVERY_ENABLED,
        YarnConfiguration.DEFAULT_NM_RECOVERY_ENABLED);
    if (recoveryEnabled) {
      FileSystem recoveryFs = FileSystem.getLocal(conf);
      String recoveryDirName = conf.get(YarnConfiguration.NM_RECOVERY_DIR);
      if (recoveryDirName == null) {
        throw new IllegalArgumentException("Recovery is enabled but " +
            YarnConfiguration.NM_RECOVERY_DIR + " is not set.");
      }
      Path recoveryRoot = new Path(recoveryDirName);
      recoveryFs.mkdirs(recoveryRoot, new FsPermission((short)0700));
      nmStore = new NMLeveldbStateStoreService();
    } else {
      nmStore = new NMNullStateStoreService();
    }
    nmStore.init(conf);
    nmStore.start();
  }

  /**
   * 停止恢复状态存储，节点下线时删除状态存储目录
   * @throws IOException 停止失败抛出异常
   */
  private void stopRecoveryStore() throws IOException {
    if