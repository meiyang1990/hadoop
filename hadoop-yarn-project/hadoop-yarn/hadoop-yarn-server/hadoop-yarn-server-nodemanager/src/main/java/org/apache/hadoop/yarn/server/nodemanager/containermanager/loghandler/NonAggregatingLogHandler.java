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
package org.apache.hadoop.yarn.server.nodemanager.containermanager.loghandler;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.RejectedExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.util.concurrent.HadoopScheduledThreadPoolExecutor;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.proto.YarnServerNodemanagerRecoveryProtos.LogDeleterProto;
import org.apache.hadoop.yarn.server.nodemanager.DeletionService;
import org.apache.hadoop.yarn.server.nodemanager.LocalDirsHandlerService;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.deletion.task.FileDeletionTask;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.loghandler.event.LogHandlerAppFinishedEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.loghandler.event.LogHandlerAppStartedEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.loghandler.event.LogHandlerEvent;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMStateStoreService;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMStateStoreService.RecoveredLogDeleterState;

import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * 非聚合日志处理器，基于配置的日志保留时间调度删除本地日志文件。
 * 用于不开启日志聚合场景下，管理NodeManager本地容器日志生命周期。
 */
public class NonAggregatingLogHandler extends AbstractService implements
    LogHandler {

  private static final Logger LOG =
       LoggerFactory.getLogger(NonAggregatingLogHandler.class);
  // 事件分发器，用于发送应用状态事件
  private final Dispatcher dispatcher;
  // 文件删除服务，用于异步执行文件删除任务
  private final DeletionService delService;
  // 存储应用ID对应用户，用于日志删除权限控制
  private final Map<ApplicationId, String> appOwners;

  // 本地目录处理器，用于获取日志目录
  private final LocalDirsHandlerService dirsHandler;
  // NM状态存储服务，用于持久化日志删除任务，支持恢复
  private final NMStateStoreService stateStore;
  // 日志删除延迟秒数（应用结束后保留日志的时间）
  private long deleteDelaySeconds;
  // 是否开启按日志大小触发立即删除
  private boolean enableTriggerDeleteBySize;
  // 触发立即删除的日志大小阈值
  private long deleteThreshold;
  // 日志删除任务调度线程池
  private ScheduledThreadPoolExecutor sched;

  /**
   * 构造非聚合日志处理器实例。
   * @param dispatcher 事件分发器
   * @param delService 文件删除服务
   * @param dirsHandler 本地目录处理器
   * @param stateStore NM状态存储服务
   */
  public NonAggregatingLogHandler(Dispatcher dispatcher,
      DeletionService delService, LocalDirsHandlerService dirsHandler,
      NMStateStoreService stateStore) {
    super(NonAggregatingLogHandler.class.getName());
    this.dispatcher = dispatcher;
    this.delService = delService;
    this.dirsHandler = dirsHandler;
    this.stateStore = stateStore;
    this.appOwners = new ConcurrentHashMap<ApplicationId, String>();
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    // 读取日志保留时间配置，默认3小时
    this.deleteDelaySeconds =
        conf.getLong(YarnConfiguration.NM_LOG_RETAIN_SECONDS,
                YarnConfiguration.DEFAULT_NM_LOG_RETAIN_SECONDS);
    // 读取是否开启按大小触发删除配置
    this.enableTriggerDeleteBySize =
        conf.getBoolean(YarnConfiguration.NM_LOG_TRIGGER_DELETE_BY_SIZE_ENABLED,
        YarnConfiguration.DEFAULT_NM_LOG_TRIGGER_DELETE_BY_SIZE_ENABLED);
    // 读取触发删除的大小阈值
    this.deleteThreshold =
        conf.getLongBytes(YarnConfiguration.NM_LOG_DELETE_THRESHOLD,
        YarnConfiguration.DEFAULT_NM_LOG_DELETE_THRESHOLD);
    // 创建日志删除调度线程池
    sched = createScheduledThreadPoolExecutor(conf);
    super.serviceInit(conf);
    // 从状态存储恢复未完成的日志删除任务
    recover();
  }

  @Override
  protected void serviceStop() throws Exception {
    if (sched != null) {
      // 关闭线程池，不再接受新任务
      sched.shutdown();
      boolean isShutdown = false;
      try {
        // 等待10秒让现有任务完成
        isShutdown = sched.awaitTermination(10, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        // 中断后立即终止所有任务
        sched.shutdownNow();
        isShutdown = true;
      }
      // 超时未完成则强制终止
      if (!isShutdown) {
        sched.shutdownNow();
      }
    }
    super.serviceStop();
  }
  
  /**
   * 获取本地文件系统上下文实例。
   * @param conf 配置
   * @return 本地文件系统上下文
   */
  FileContext getLocalFileContext(Configuration conf) {
    try {
      return FileContext.getLocalFSFileContext(conf);
    } catch (IOException e) {
      throw new YarnRuntimeException("Failed to access local fs");
    }
  }

  /**
   * 从NM状态存储恢复之前未执行的日志删除任务。
   * @throws IOException 状态恢复IO异常
   */
  private void recover() throws IOException {
    if (stateStore.canRecover()) {
      // 加载已保存的日志删除任务状态
      RecoveredLogDeleterState state = stateStore.loadLogDeleterState();
      long now = System.currentTimeMillis();
      // 遍历所有恢复的删除任务
      for (Map.Entry<ApplicationId, LogDeleterProto> entry :
        state.getLogDeleterMap().entrySet()) {
        ApplicationId appId = entry.getKey();
        LogDeleterProto proto = entry.getValue();
        // 计算剩余延迟时间
        long deleteDelayMsec = proto.getDeletionTime() - now;
        LOG.debug("Scheduling deletion of {} logs in {} msec", appId,
            deleteDelayMsec);
        // 创建删除任务
        LogDeleterRunnable logDeleter =
            new LogDeleterRunnable(proto.getUser(), appId);
        try {
          // 调度删除任务
          sched.schedule(logDeleter, deleteDelayMsec, TimeUnit.MILLISECONDS);
        } catch (RejectedExecutionException e) {
          // 线程池已关闭，在当前线程直接执行删除
          logDeleter.run();
        }
      }
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public void handle(LogHandlerEvent event) {
    switch (event.getType()) {
      case APPLICATION_STARTED:
        // 应用启动事件处理
        LogHandlerAppStartedEvent appStartedEvent =
            (LogHandlerAppStartedEvent) event;
        // 保存应用对应用户
        this.appOwners.put(appStartedEvent.getApplicationId(),
            appStartedEvent.getUser());
        // 通知应用日志初始化完成
        this.dispatcher.getEventHandler().handle(
            new ApplicationEvent(appStartedEvent.getApplicationId(),
                ApplicationEventType.APPLICATION_LOG_HANDLING_INITED));
        break;
      case CONTAINER_FINISHED:
        // 容器结束不处理，非聚合日志只在应用结束后整体删除
        break;
      case APPLICATION_FINISHED:
        // 应用结束事件处理，调度日志删除
        LogHandlerAppFinishedEvent appFinishedEvent =
            (LogHandlerAppFinishedEvent) event;
        ApplicationId appId = appFinishedEvent.getApplicationId();
        String user = appOwners.remove(appId);
        if (user == null) {
          // 找不到应用对应用户，通知日志处理失败
          LOG.error("Unable to locate user for {}", appId);
          NonAggregatingLogHandler.this.dispatcher.getEventHandler().handle(
              new ApplicationEvent(appId,
                  ApplicationEventType.APPLICATION_LOG_HANDLING_FAILED));
          break;
        }
        // 创建日志删除任务
        LogDeleterRunnable logDeleter = new LogDeleterRunnable(user, appId);
        // 计算删除时间戳
        long deletionTimestamp = System.currentTimeMillis()
            + this.deleteDelaySeconds * 1000;
        // 构建删除任务proto，用于持久化
        LogDeleterProto deleterProto = LogDeleterProto.newBuilder()
            .setUser(user)
            .setDeletionTime(deletionTimestamp)
            .build();
        try {
          // 持久化删除任务到状态存储，支持NM重启恢复
          stateStore.storeLogDeleter(appId, deleterProto);
        } catch (IOException e) {
          LOG.error("Unable to record log deleter state", e);
        }
        try {
          boolean logDeleterStarted = false;
          // 如果开启按大小触发删除，检查日志大小
          if (enableTriggerDeleteBySize) {
            final long appLogSize = calculateSizeOfAppLogs(user, appId);
            // 日志大小超过阈值，立即删除不等待
            if (appLogSize >= deleteThreshold) {
              LOG.info("Log Deletion for application: {}, with no delay, size={}", appId, appLogSize);
              sched.schedule(logDeleter, 0, TimeUnit.SECONDS);
              logDeleterStarted = true;
            }
          }
          // 不需要立即删除，按配置延迟调度
          if (!logDeleterStarted) {
            LOG.info("Scheduling Log Deletion for application: {}, with delay of {} seconds",
                appId, this.deleteDelaySeconds);
            sched.schedule(logDeleter, this.deleteDelaySeconds, TimeUnit.SECONDS);
          }
        } catch (RejectedExecutionException e) {
          // 线程池已关闭，在当前线程直接执行删除
          logDeleter.run();
        }
        break;
      default:
    }
  }

  @Override
  public Set<ApplicationId> getInvalidTokenApps() {
    // 非聚合日志不处理token，返回空集合
    return Collections.emptySet();
  }

  /**
   * 创建日志删除任务调度线程池。
   * @param conf 配置
   * @return 调度线程池实例
   */
  ScheduledThreadPoolExecutor createScheduledThreadPoolExecutor(
      Configuration conf) {
    // 创建命名线程工厂
    ThreadFactory tf =
        new ThreadFactoryBuilder().setNameFormat("LogDeleter #%d").build();
    sched =
        new HadoopScheduledThreadPoolExecutor(conf.getInt(
            YarnConfiguration.NM_LOG_DELETION_THREADS_COUNT,
            YarnConfiguration.DEFAULT_NM_LOG_DELETE_THREAD_COUNT), tf);
    return sched;
  }

  /**
   * 计算应用所有本地日志的总大小。
   * @param user 应用对应用户
   * @param applicationId 应用ID
   * @return 应用日志总大小（字节）
   */
  private long calculateSizeOfAppLogs(String user, ApplicationId applicationId) {
    FileContext lfs = getLocalFileContext(getConfig());
    long appLogsSize = 0L;
    // 遍历所有日志根目录
    for (String rootLogDir : dirsHandler.getLogDirsForCleanup()) {
      Path logDir = new Path(rootLogDir, applicationId.toString());
      try {
        // 累加日志目录大小
        appLogsSize += lfs.getFileStatus(logDir).getLen();
      } catch (UnsupportedFileSystemException ue) {
        LOG.warn("Unsupported file system used for log dir {}", logDir, ue);
        continue;
      } catch (IOException ie) {
        LOG.error("Unable to getFileStatus for {}", logDir, ie);
        continue;
      }
    }
    return appLogsSize;
  }

  /**
   * 应用日志删除任务，负责删除指定应用的所有本地日志文件。
   */
  class LogDeleterRunnable implements Runnable {
    private String user;
    private ApplicationId applicationId;

    public LogDeleterRunnable(String user, ApplicationId applicationId) {
      this.user = user;
      this.applicationId = applicationId;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void run() {
      List<Path> localAppLogDirs = new ArrayList<Path>();
      FileContext lfs = getLocalFileContext(getConfig());
      // 收集所有存在的应用日志目录
      for (String rootLogDir : dirsHandler.getLogDirsForCleanup()) {
        Path logDir = new Path(rootLogDir, applicationId.toString());
        try {
          lfs.getFileStatus(logDir);
          localAppLogDirs.add(logDir);
        } catch (UnsupportedFileSystemException ue) {
          LOG.warn("Unsupported file system used for log dir " + logDir, ue);
          continue;
        } catch (IOException ie) {
          // 目录不存在直接忽略
          continue;
        }
      }

      // 先通知应用日志处理完成，让WebUI提前移除日志链接
      NonAggregatingLogHandler.this.dispatcher.getEventHandler().handle(
        new ApplicationEvent(this.applicationId,
          ApplicationEventType.APPLICATION_LOG_HANDLING_FINISHED));
      // 存在需要删除的日志目录，提交删除任务到删除服务
      if (localAppLogDirs.size() > 0) {
        FileDeletionTask deletionTask = new FileDeletionTask(
            NonAggregatingLogHandler.this.delService, user, null,
            localAppLogDirs);
        NonAggregatingLogHandler.this.delService.delete(deletionTask);
      }
      try {
        // 从状态存储中删除该日志删除任务记录
        NonAggregatingLogHandler.this.stateStore.removeLogDeleter(
            this.applicationId);
      } catch (IOException e) {
        LOG.error("Error removing log deletion state", e);
      }
    }

    @Override
    public String toString() {
      return "LogDeleter for AppId " + this.applicationId.toString()
          + ", owned by " + user;
    }
  }
}