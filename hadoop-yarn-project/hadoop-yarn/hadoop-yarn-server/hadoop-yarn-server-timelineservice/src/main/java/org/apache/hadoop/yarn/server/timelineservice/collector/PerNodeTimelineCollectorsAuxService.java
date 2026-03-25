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

package org.apache.hadoop.yarn.server.timelineservice.collector;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.util.ShutdownHookManager;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.YarnUncaughtExceptionHandler;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.api.ApplicationInitializationContext;
import org.apache.hadoop.yarn.server.api.ApplicationTerminationContext;
import org.apache.hadoop.yarn.server.api.AuxiliaryService;
import org.apache.hadoop.yarn.server.api.ContainerInitializationContext;
import org.apache.hadoop.yarn.server.api.ContainerTerminationContext;
import org.apache.hadoop.yarn.server.api.ContainerType;

import org.apache.hadoop.classification.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 每个节点上时间线采集器管理器的顶级服务，当前作为YARN NodeManager的辅助服务运行，
 * 负责管理本节点上各个应用的时间线采集器生命周期。
 */
@Private
@Unstable
public class PerNodeTimelineCollectorsAuxService extends AuxiliaryService {
  private static final Logger LOG =
      LoggerFactory.getLogger(PerNodeTimelineCollectorsAuxService.class);
  private static final int SHUTDOWN_HOOK_PRIORITY = 30;

  private final NodeTimelineCollectorManager collectorManager;
  private long collectorLingerPeriod;
  private ScheduledExecutorService scheduler;
  /** 记录每个应用对应的AM容器集合，用于处理多尝试场景下延迟删除 */
  private Map<ApplicationId, Set<ContainerId>> appIdToContainerId =
      new ConcurrentHashMap<>();

  /** 默认构造函数 */
  public PerNodeTimelineCollectorsAuxService() {
    this(new NodeTimelineCollectorManager(true));
  }

  @VisibleForTesting PerNodeTimelineCollectorsAuxService(
      NodeTimelineCollectorManager collectorsManager) {
    super("timeline_collector");
    this.collectorManager = collectorsManager;
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    // 检查V2时间线服务是否启用，未启用则抛出异常提示移除该辅助服务
    if (!YarnConfiguration.timelineServiceV2Enabled(conf)) {
      throw new YarnException(
          "Looks like timeline_collector is set as an auxillary service in "
              + YarnConfiguration.NM_AUX_SERVICES
              + ". But Timeline service v2 is not enabled,"
              + " so timeline_collector needs to be removed"
              + " from that list of auxillary services.");
    }
    // 读取配置获取采集器延迟删除时间
    collectorLingerPeriod =
        conf.getLong(YarnConfiguration.ATS_APP_COLLECTOR_LINGER_PERIOD_IN_MS,
            YarnConfiguration.DEFAULT_ATS_APP_COLLECTOR_LINGER_PERIOD_IN_MS);
    // 创建单线程定时调度器，用于延迟删除应用采集器
    scheduler = Executors.newSingleThreadScheduledExecutor();
    collectorManager.init(conf);
    super.serviceInit(conf);
  }

  @Override
  protected void serviceStart() throws Exception {
    collectorManager.start();
    super.serviceStart();
  }

  @Override
  protected void serviceStop() throws Exception {
    // 关闭调度器，等待延迟任务完成
    scheduler.shutdown();
    if (!scheduler.awaitTermination(collectorLingerPeriod,
        TimeUnit.MILLISECONDS)) {
      LOG.warn(
          "Scheduler terminated before removing the application collectors");
    }
    collectorManager.stop();
    super.serviceStop();
  }

  // these methods can be used as the basis for future service methods if the
  // per-node collector runs separate from the node manager
  /**
   * 添加应用级采集器，不存在才创建。初始化并启动采集器，已存在则不操作。
   *
   * @param appId 应用ID
   * @param user AM容器对应用户
   * @return 是否成功添加
   */
  public boolean addApplicationIfAbsent(ApplicationId appId, String user) {
    AppLevelTimelineCollector collector =
        new AppLevelTimelineCollectorWithAgg(appId, user);
    return (collectorManager.putIfAbsent(appId, collector)
        == collector);
  }

  /**
   * 删除应用级采集器，停止采集器，不存在则不操作。
   *
   * @param appId 待删除应用ID
   * @return 是否成功删除
   */
  public boolean removeApplication(ApplicationId appId) {
    return collectorManager.remove(appId);
  }

  /**
   * 容器初始化回调，拦截AM容器创建事件，初始化应用级采集器。
   */
  @Override
  public void initializeContainer(ContainerInitializationContext context) {
    // 仅处理AM容器初始化事件
    if (context.getContainerType() == ContainerType.APPLICATION_MASTER) {
      ApplicationId appId = context.getContainerId().
          getApplicationAttemptId().getApplicationId();
      synchronized (appIdToContainerId){
        Set<ContainerId> masterContainers = appIdToContainerId.get(appId);
        if (masterContainers == null) {
          masterContainers = new HashSet<>();
          appIdToContainerId.put(appId, masterContainers);
        }
        // 将当前AM容器加入集合
        masterContainers.add(context.getContainerId());
      }
      // 添加应用采集器
      addApplicationIfAbsent(appId, context.getUser());
    }
  }

  /**
   * 容器停止回调，拦截AM容器停止事件，延迟删除应用级采集器。
   */
  @Override
  public void stopContainer(ContainerTerminationContext context) {
    // 仅处理AM容器停止事件
    if (context.getContainerType() == ContainerType.APPLICATION_MASTER) {
      final ContainerId containerId = context.getContainerId();
      // 触发应用采集器删除流程
      removeApplicationCollector(containerId);
    }
  }

  @VisibleForTesting
  protected Future removeApplicationCollector(final ContainerId containerId) {
    final ApplicationId appId =
        containerId.getApplicationAttemptId().getApplicationId();
    // 延迟指定时间后执行删除，应对AM快速重启场景
    return scheduler.schedule(new Runnable() {
      public void run() {
        boolean shouldRemoveApplication = false;
        synchronized (appIdToContainerId) {
          Set<ContainerId> masterContainers = appIdToContainerId.get(appId);
          if (masterContainers == null) {
            LOG.info("Stop container for {}"
                + " is called before initializing container.", containerId);
            return;
          }
          // 移除已停止的容器
          masterContainers.remove(containerId);
          // 当该应用没有剩余AM容器时，才删除整个应用采集器
          if (masterContainers.size() == 0) {
            shouldRemoveApplication = true;
            appIdToContainerId.remove(appId);
          }
        }

        if (shouldRemoveApplication) {
          removeApplication(appId);
        }
      }
    }, collectorLingerPeriod, TimeUnit.MILLISECONDS);
  }

  @VisibleForTesting
  boolean hasApplication(ApplicationId appId) {
    return collectorManager.containsTimelineCollector(appId);
  }

  @Override
  public void initializeApplication(ApplicationInitializationContext context) {
  }

  @Override
  public void stopApplication(ApplicationTerminationContext context) {
  }

  @Override
  public ByteBuffer getMetaData() {
    // TODO currently it is not used; we can return a more meaningful data when
    // we connect it with an AM
    return ByteBuffer.allocate(0);
  }

  @VisibleForTesting
  public static PerNodeTimelineCollectorsAuxService
      launchServer(String[] args, NodeTimelineCollectorManager collectorManager,
      Configuration conf) {
    // 设置默认未捕获异常处理器
    Thread
      .setDefaultUncaughtExceptionHandler(new YarnUncaughtExceptionHandler());
    // 打印启动信息日志
    StringUtils.startupShutdownMessage(
        PerNodeTimelineCollectorsAuxService.class, args, LOG);
    PerNodeTimelineCollectorsAuxService auxService = null;
    try {
      // 创建服务实例
      auxService = collectorManager == null ?
          new PerNodeTimelineCollectorsAuxService(
              new NodeTimelineCollectorManager(false)) :
          new PerNodeTimelineCollectorsAuxService(collectorManager);
      // 注册关闭钩子
      ShutdownHookManager.get().addShutdownHook(new ShutdownHook(auxService),
          SHUTDOWN_HOOK_PRIORITY);
      // 初始化并启动服务
      auxService.init(conf);
      auxService.start();
    } catch (Throwable t) {
      LOG.error("Error starting PerNodeTimelineCollectorServer", t);
      ExitUtil.terminate(-1, "Error starting PerNodeTimelineCollectorServer");
    }
    return auxService;
  }

  /** 服务关闭钩子，JVM退出时停止服务 */
  private static class ShutdownHook implements Runnable {
    private final PerNodeTimelineCollectorsAuxService auxService;

    public ShutdownHook(PerNodeTimelineCollectorsAuxService auxService) {
      this.auxService = auxService;
    }

    public void run() {
      auxService.stop();
    }
  }

  /** 独立启动服务入口 */
  public static void main(String[] args) {
    Configuration conf = new YarnConfiguration();
    conf.setBoolean(YarnConfiguration.TIMELINE_SERVICE_ENABLED, true);
    conf.setFloat(YarnConfiguration.TIMELINE_SERVICE_VERSION, 2.0f);
    launchServer(args, null, conf);
  }
}