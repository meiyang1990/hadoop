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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.launcher;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import org.apache.hadoop.util.Shell;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerExitEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.concurrent.HadoopExecutors;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.nodemanager.ContainerExecutor;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.LocalDirsHandlerService;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.ContainerManagerImpl;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.Application;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.ResourceLocalizationService;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * 容器启动器服务，负责NodeManager上所有容器的生命周期操作，包括启动、重启、恢复、清理、信号发送、暂停、恢复运行等。
 * 该服务必须在{@link ResourceLocalizationService}启动之后才能启动，因为它依赖本地化服务创建的本地文件系统目录。
 */
public class ContainersLauncher extends AbstractService
    implements AbstractContainersLauncher {

  private static final Logger LOG =
       LoggerFactory.getLogger(ContainersLauncher.class);

  // NodeManager全局上下文
  private Context context;
  // 容器执行器，负责实际执行容器启动命令
  private ContainerExecutor exec;
  // 事件分发器，用于分发容器相关事件
  private Dispatcher dispatcher;
  // 容器管理器实例
  private ContainerManagerImpl containerManager;

  // 本地目录处理器，用于管理容器运行的本地磁盘目录
  private LocalDirsHandlerService dirsHandler;
  @VisibleForTesting
  // 容器操作线程池，异步执行容器启动、清理等操作
  public ExecutorService containerLauncher =
      HadoopExecutors.newCachedThreadPool(
        new ThreadFactoryBuilder()
          .setNameFormat("ContainersLauncher #%d")
          .build());
  @VisibleForTesting
  // 当前正在运行的容器映射表，key为容器ID，value为容器启动任务实例
  public final Map<ContainerId, ContainerLaunch> running =
    Collections.synchronizedMap(new HashMap<ContainerId, ContainerLaunch>());

  /**
   * 构造函数，初始化服务名称。
   */
  public ContainersLauncher() {
    super("containers-launcher");
  }

  /**
   * 测试用构造函数，直接初始化所有依赖组件。
   * @param context NodeManager上下文
   * @param dispatcher 事件分发器
   * @param exec 容器执行器
   * @param dirsHandler 本地目录处理器
   * @param containerManager 容器管理器
   */
  @VisibleForTesting
  public ContainersLauncher(Context context, Dispatcher dispatcher,
      ContainerExecutor exec, LocalDirsHandlerService dirsHandler,
      ContainerManagerImpl containerManager) {
    this();
    init(context, dispatcher, exec, dirsHandler, containerManager);
  }

  @Override
  public void init(Context nmContext, Dispatcher nmDispatcher,
      ContainerExecutor containerExec, LocalDirsHandlerService nmDirsHandler,
      ContainerManagerImpl nmContainerManager) {
    // 初始化所有依赖组件
    this.exec = containerExec;
    this.context = nmContext;
    this.dispatcher = nmDispatcher;
    this.dirsHandler = nmDirsHandler;
    this.containerManager = nmContainerManager;
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    try {
      //TODO Is this required?
      // 初始化本地文件上下文
      FileContext.getLocalFSFileContext(conf);
    } catch (UnsupportedFileSystemException e) {
      throw new YarnRuntimeException("Failed to start ContainersLauncher", e);
    }
    super.serviceInit(conf);
  }

  @Override
  protected  void serviceStop() throws Exception {
    // 关闭线程池，中断所有正在执行的容器操作
    containerLauncher.shutdownNow();
    super.serviceStop();
  }

  @Override
  public void handle(ContainersLauncherEvent event) {
    // 获取事件对应容器和容器ID
    Container container = event.getContainer();
    ContainerId containerId = container.getContainerId();
    // 根据事件类型处理不同容器操作
    switch (event.getType()) {
      case LAUNCH_CONTAINER:
        // 获取容器所属应用信息
        Application app =
          context.getApplications().get(
              containerId.getApplicationAttemptId().getApplicationId());

        // 创建容器启动任务
        ContainerLaunch launch =
            new ContainerLaunch(context, getConfig(), dispatcher, exec, app,
              event.getContainer(), dirsHandler, containerManager);
        // 提交异步启动任务
        containerLauncher.submit(launch);
        // 记录到运行容器列表
        running.put(containerId, launch);
        break;
      case RELAUNCH_CONTAINER:
        app = context.getApplications().get(
                containerId.getApplicationAttemptId().getApplicationId());

        // 创建容器重启任务
        ContainerRelaunch relaunch =
            new ContainerRelaunch(context, getConfig(), dispatcher, exec, app,
                event.getContainer(), dirsHandler, containerManager);
        // 提交异步重启任务
        containerLauncher.submit(relaunch);
        // 记录到运行容器列表
        running.put(containerId, relaunch);
        break;
      case RECOVER_CONTAINER:
        app = context.getApplications().get(
            containerId.getApplicationAttemptId().getApplicationId());
        // 创建恢复容器启动任务（用于NM重启后恢复已有容器）
        launch = new RecoveredContainerLaunch(context, getConfig(), dispatcher,
            exec, app, event.getContainer(), dirsHandler, containerManager);
        // 提交异步恢复任务
        containerLauncher.submit(launch);
        // 记录到运行容器列表
        running.put(containerId, launch);
        break;
      case RECOVER_PAUSED_CONTAINER:
        app = context.getApplications().get(
            containerId.getApplicationAttemptId().getApplicationId());
        // 创建恢复暂停容器启动任务
        launch = new RecoverPausedContainerLaunch(context, getConfig(),
            dispatcher, exec, app, event.getContainer(), dirsHandler,
            containerManager);
        // 提交异步恢复任务
        containerLauncher.submit(launch);
        break;
      case CLEANUP_CONTAINER:
        // 清理容器，异步执行
        cleanup(event, containerId, true);
        break;
      case CLEANUP_CONTAINER_FOR_REINIT:
        // 清理容器，同步执行（用于容器重初始化场景）
        cleanup(event, containerId, false);
        break;
      case SIGNAL_CONTAINER:
        // 转换为信号事件实例
        SignalContainersLauncherEvent signalEvent =
            (SignalContainersLauncherEvent) event;
        // 获取正在运行的容器实例
        ContainerLaunch runningContainer = running.get(containerId);
        if (runningContainer == null) {
          // 容器未启动，无需发送信号
          LOG.info("Container " + containerId + " not running, nothing to signal.");
          return;
        }

        try {
          // 向容器发送指定信号
          runningContainer.signalContainer(signalEvent.getCommand());
        } catch (IOException e) {
          LOG.warn("Got exception while signaling container " + containerId
              + " with command " + signalEvent.getCommand());
        }
        break;
      case PAUSE_CONTAINER:
        // 获取正在运行的容器实例
        ContainerLaunch launchedContainer = running.get(containerId);
        if (launchedContainer == null) {
          // 容器未启动，无需操作
          return;
        }

        // 暂停容器运行
        try {
          launchedContainer.pauseContainer();
        } catch (Exception e) {
          LOG.info("Got exception while pausing container: " +
            StringUtils.stringifyException(e));
        }
        break;
      case RESUME_CONTAINER:
        // 获取正在运行的容器实例
        ContainerLaunch launchCont = running.get(containerId);
        if (launchCont == null) {
          // 容器未启动，无需操作
          return;
        }

        // 恢复已暂停容器运行
        try {
          launchCont.resumeContainer();
        } catch (Exception e) {
          LOG.info("Got exception while resuming container: " +
            StringUtils.stringifyException(e));
        }
        break;
    }
  }

  @VisibleForTesting
  /**
   * 清理容器资源，终止容器进程并从运行列表移除。
   * @param event 容器启动器事件
   * @param containerId 容器ID
   * @param async 是否异步执行清理
   */
  void cleanup(ContainersLauncherEvent event, ContainerId containerId,
      boolean async) {
    // 从运行列表移除容器
    ContainerLaunch existingLaunch = running.remove(containerId);
    if (existingLaunch == null) {
      // 容器未启动，发送容器被杀死事件，触发状态流转
      dispatcher.getEventHandler().handle(
          new ContainerExitEvent(containerId,
              ContainerEventType.CONTAINER_KILLED_ON_REQUEST,
              Shell.WINDOWS ?
                  ContainerExecutor.ExitCode.FORCE_KILLED.getExitCode() :
                  ContainerExecutor.ExitCode.TERMINATED.getExitCode(),
              "Container terminated before launch."));
      return;
    }

    // 创建容器清理任务，确保杀死所有子进程并清理资源
    ContainerCleanup cleanup = new ContainerCleanup(context, getConfig(),
        dispatcher, exec, event.getContainer(), existingLaunch);
    if (async) {
      // 异步提交清理任务
      containerLauncher.submit(cleanup);
    } else {
      // 同步执行清理
      cleanup.run();
    }
  }
}