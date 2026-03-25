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
package org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer;

import static org.apache.hadoop.fs.CreateFlag.CREATE;
import static org.apache.hadoop.fs.CreateFlag.OVERWRITE;

import org.apache.hadoop.yarn.exceptions.ConfigurationException;
import org.apache.hadoop.yarn.server.nodemanager.recovery.RecoveryIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FSError;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.util.DiskChecker;
import org.apache.hadoop.util.DiskValidator;
import org.apache.hadoop.util.DiskValidatorFactory;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.concurrent.HadoopExecutors;
import org.apache.hadoop.util.concurrent.HadoopScheduledThreadPoolExecutor;
import org.apache.hadoop.util.concurrent.SubjectInheritingThread;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.URL;
import org.apache.hadoop.yarn.api.records.impl.pb.LocalResourcePBImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.proto.YarnProtos.LocalResourceProto;
import org.apache.hadoop.yarn.proto.YarnServerNodemanagerRecoveryProtos.LocalizedResourceProto;
import org.apache.hadoop.yarn.server.nodemanager.ContainerExecutor;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.DeletionService;
import org.apache.hadoop.yarn.server.nodemanager.DirectoryCollection.DirsChangeListener;
import org.apache.hadoop.yarn.server.nodemanager.LocalDirsHandlerService;
import org.apache.hadoop.yarn.server.nodemanager.api.LocalizationProtocol;
import org.apache.hadoop.yarn.server.nodemanager.api.ResourceLocalizationSpec;
import org.apache.hadoop.yarn.server.nodemanager.api.protocolrecords.LocalResourceStatus;
import org.apache.hadoop.yarn.server.nodemanager.api.protocolrecords.LocalizerAction;
import org.apache.hadoop.yarn.server.nodemanager.api.protocolrecords.LocalizerHeartbeatResponse;
import org.apache.hadoop.yarn.server.nodemanager.api.protocolrecords.LocalizerStatus;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.Application;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationInitedEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerResourceFailedEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerState;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.deletion.task.FileDeletionTask;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.LocalCacheCleaner.LocalCacheCleanerStats;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.ApplicationLocalizationEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.ContainerLocalizationCleanupEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.ContainerLocalizationEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.ContainerLocalizationRequestEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.LocalizationEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.LocalizationEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.LocalizerEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.LocalizerEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.LocalizerResourceRequestEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.ResourceFailedLocalizationEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.ResourceLocalizedEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.ResourceRecoveredEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.ResourceReleaseEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.ResourceRequestEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.security.LocalizerTokenIdentifier;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.security.LocalizerTokenSecretManager;
import org.apache.hadoop.yarn.server.nodemanager.executor.LocalizerStartContext;
import org.apache.hadoop.yarn.server.nodemanager.metrics.NodeManagerMetrics;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMStateStoreService;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMStateStoreService.LocalResourceTrackerState;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMStateStoreService.RecoveredLocalizationState;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMStateStoreService.RecoveredUserResources;
import org.apache.hadoop.yarn.server.nodemanager.security.authorize.NMPolicyProvider;
import org.apache.hadoop.yarn.server.nodemanager.util.NodeManagerBuilderUtils;
import org.apache.hadoop.yarn.util.FSDownload;
import org.apache.hadoop.yarn.util.LRUCacheHashMap;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.cache.CacheBuilder;
import org.apache.hadoop.thirdparty.com.google.common.cache.LoadingCache;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * YARN NodeManager 资源本地化服务，负责将容器需要的运行资源从HDFS下载到本地磁盘缓存，
 * 支持PUBLIC/PRIVATE/APPLICATION三种可见性资源的本地化，提供缓存清理、故障恢复能力。
 * 是NodeManager启动容器前准备工作的核心组件。
 */
public class ResourceLocalizationService extends CompositeService
    implements EventHandler<LocalizationEvent>, LocalizationProtocol {

  private static final Logger LOG =
       LoggerFactory.getLogger(ResourceLocalizationService.class);
  public static final String NM_PRIVATE_DIR = "nmPrivate";
  public static final FsPermission NM_PRIVATE_PERM = new FsPermission((short) 0700);
  private static final FsPermission PUBLIC_FILECACHE_FOLDER_PERMS =
      new FsPermission((short) 0755);

  private Server server;
  private InetSocketAddress localizationServerAddress;
  @VisibleForTesting
  long cacheTargetSize;
  private long cacheCleanupPeriod;

  private final ContainerExecutor exec;
  protected final Dispatcher dispatcher;
  private final DeletionService delService;
  private LocalizerTracker localizerTracker;
  private RecordFactory recordFactory;
  private final ScheduledExecutorService cacheCleanup;
  private LocalizerTokenSecretManager secretManager;
  private NMStateStoreService stateStore;
  @VisibleForTesting
  final NodeManagerMetrics metrics;

  @VisibleForTesting
  LocalResourcesTracker publicRsrc;

  private LocalDirsHandlerService dirsHandler;
  private DirsChangeListener localDirsChangeListener;
  private DirsChangeListener logDirsChangeListener;
  private Context nmContext;
  private DiskValidator diskValidator;

  /**
   * Map of LocalResourceTrackers keyed by username, for private
   * resources.
   */
  @VisibleForTesting
  final ConcurrentMap<String, LocalResourcesTracker> privateRsrc =
    new ConcurrentHashMap<String,LocalResourcesTracker>();

  /**
   * Map of LocalResourceTrackers keyed by appid, for application
   * resources.
   */
  private final ConcurrentMap<String,LocalResourcesTracker> appRsrc =
    new ConcurrentHashMap<String,LocalResourcesTracker>();
  
  FileContext lfs;

  /**
   * 构造资源本地化服务实例
   * @param dispatcher 事件分发器
   * @param exec 容器执行器
   * @param delService 删除服务
   * @param dirsHandler 本地目录处理器
   * @param context NodeManager上下文
   * @param metrics NodeManager指标收集器
   */
  public ResourceLocalizationService(Dispatcher dispatcher,
      ContainerExecutor exec, DeletionService delService,
      LocalDirsHandlerService dirsHandler, Context context,
      NodeManagerMetrics metrics) {

    super(ResourceLocalizationService.class.getName());
    this.exec = exec;
    this.dispatcher = dispatcher;
    this.delService = delService;
    this.dirsHandler = dirsHandler;

    this.cacheCleanup = new HadoopScheduledThreadPoolExecutor(1,
        new ThreadFactoryBuilder()
          .setNameFormat("ResourceLocalizationService Cache Cleanup")
          .build());
    this.stateStore = context.getNMStateStore();
    this.nmContext = context;
    this.metrics = metrics;
  }

  FileContext getLocalFileContext(Configuration conf) {
    try {
      return FileContext.getLocalFSFileContext(conf);
    } catch (IOException e) {
      throw new YarnRuntimeException("Failed to access local fs");
    }
  }

  private void validateConf(Configuration conf) {
    int perDirFileLimit =
        conf.getInt(YarnConfiguration.NM_LOCAL_CACHE_MAX_FILES_PER_DIRECTORY,
          YarnConfiguration.DEFAULT_NM_LOCAL_CACHE_MAX_FILES_PER_DIRECTORY);
    if (perDirFileLimit <= 36) {
      LOG.error(YarnConfiguration.NM_LOCAL_CACHE_MAX_FILES_PER_DIRECTORY
          + " parameter is configured with very low value.");
      throw new YarnRuntimeException(
        YarnConfiguration.NM_LOCAL_CACHE_MAX_FILES_PER_DIRECTORY
            + " parameter is configured with a value less than 37.");
    } else {
      LOG.info("per directory file limit = " + perDirFileLimit);
    }
  }

  @Override
  public void serviceInit(Configuration conf) throws Exception {
    this.validateConf(conf);
    // 创建公共资源跟踪器
    this.publicRsrc = new LocalResourcesTrackerImpl(null, null, dispatcher,
        true, conf, stateStore, dirsHandler);
    this.recordFactory = RecordFactoryProvider.getRecordFactory(conf);

    try {
      lfs = getLocalFileContext(conf);
      lfs.setUMask(new FsPermission((short) FsPermission.DEFAULT_UMASK));

      // 不可恢复或新建存储时，清理旧数据并初始化本地目录
      if (!stateStore.canRecover()|| stateStore.isNewlyCreated()) {
        cleanUpLocalDirs(lfs, delService);
        cleanupLogDirs(lfs, delService);
        initializeLocalDirs(lfs);
        initializeLogDirs(lfs);
      }
    } catch (Exception e) {
      throw new YarnRuntimeException(
        "Failed to initialize LocalizationService", e);
    }

    // 初始化磁盘验证器
    diskValidator = DiskValidatorFactory.getInstance(
        YarnConfiguration.DEFAULT_DISK_VALIDATOR);
    // 读取缓存配置：缓存最大总大小
    cacheTargetSize =
      conf.getLong(YarnConfiguration.NM_LOCALIZER_CACHE_TARGET_SIZE_MB, YarnConfiguration.DEFAULT_NM_LOCALIZER_CACHE_TARGET_SIZE_MB) << 20;
    // 读取缓存配置：清理周期
    cacheCleanupPeriod =
      conf.getLong(YarnConfiguration.NM_LOCALIZER_CACHE_CLEANUP_INTERVAL_MS, YarnConfiguration.DEFAULT_NM_LOCALIZER_CACHE_CLEANUP_INTERVAL_MS);
    // 读取本地化服务RPC地址配置
    localizationServerAddress = conf.getSocketAddr(
        YarnConfiguration.NM_BIND_HOST,
        YarnConfiguration.NM_LOCALIZER_ADDRESS,
        YarnConfiguration.DEFAULT_NM_LOCALIZER_ADDRESS,
        YarnConfiguration.DEFAULT_NM_LOCALIZER_PORT);

    // 创建Localizer跟踪器并注册事件处理
    localizerTracker = createLocalizerTracker(conf);
    addService(localizerTracker);
    dispatcher.register(LocalizerEventType.class, localizerTracker);
    // 注册本地目录变更监听器，目录变化时重新初始化
    localDirsChangeListener = new DirsChangeListener() {
      @Override
      public void onDirsChanged() {
        checkAndInitializeLocalDirs();
      }
    };
    // 注册日志目录变更监听器
    logDirsChangeListener = new DirsChangeListener() {
      @Override
      public void onDirsChanged() {
        initializeLogDirs(lfs);
      }
    };
    super.serviceInit(conf);
  }

  // Recover localized resources after an NM restart
  /**
   * NM重启后恢复已本地化的资源状态
   * @param state 恢复状态数据
   * @throws URISyntaxException URI语法错误
   * @throws IOException IO异常
   */
  public void recoverLocalizedResources(RecoveredLocalizationState state)
      throws URISyntaxException, IOException {
    // 恢复公共资源
    LocalResourceTrackerState trackerState = state.getPublicTrackerState();
    recoverTrackerResources(publicRsrc, trackerState);

    // 遍历恢复用户私有资源和应用私有资源
    try (RecoveryIterator<Map.Entry<String, RecoveredUserResources>> it
             = state.getIterator()) {
      while (it.hasNext()) {
        Map.Entry<String, RecoveredUserResources> userEntry = it.next();
        String user = userEntry.getKey();
        RecoveredUserResources userResources = userEntry.getValue();
        trackerState = userResources.getPrivateTrackerState();
        // 创建用户私有资源跟踪器
        LocalResourcesTracker tracker = new LocalResourcesTrackerImpl(user,
            null, dispatcher, true, super.getConfig(), stateStore,
            dirsHandler);
        LocalResourcesTracker oldTracker = privateRsrc.putIfAbsent(user,
            tracker);
        if (oldTracker != null) {
          tracker = oldTracker;
        }
        recoverTrackerResources(tracker, trackerState);

        // 逐个恢复应用资源
        for (Map.Entry<ApplicationId, LocalResourceTrackerState> appEntry :
            userResources.getAppTrackerStates().entrySet()) {
          trackerState = appEntry.getValue();
          ApplicationId appId = appEntry.getKey();
          String appIdStr = appId.toString();
          LocalResourcesTracker tracker1 = new LocalResourcesTrackerImpl(user,
              appId, dispatcher, false, super.getConfig(),