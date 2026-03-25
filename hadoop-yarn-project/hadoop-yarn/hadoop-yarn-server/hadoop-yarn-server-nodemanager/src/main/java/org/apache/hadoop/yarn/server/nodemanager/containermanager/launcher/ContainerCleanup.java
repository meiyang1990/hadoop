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

import org.apache.hadoop.util.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.server.nodemanager.ContainerExecutor;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.DeletionService;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerDiagnosticsUpdateEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerExitEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.deletion.task.DockerContainerDeletionTask;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.DockerLinuxContainerRuntime;
import org.apache.hadoop.yarn.server.nodemanager.executor.ContainerSignalContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.apache.hadoop.yarn.server.nodemanager.containermanager.launcher.ContainerLaunch.EXIT_CODE_FILE_SUFFIX;

/**
 * 容器清理任务，负责在容器退出或被杀死后清理相关资源
 * 如果容器尚未启动则取消启动，否则会给容器进程发送信号终止进程并清理文件资源
 */
public class ContainerCleanup implements Runnable {

  private static final Logger LOG =
      LoggerFactory.getLogger(ContainerCleanup.class);

  private final Context context;
  private final Configuration conf;
  private final Dispatcher dispatcher;
  private final ContainerExecutor exec;
  private final Container container;
  private final ContainerLaunch launch;
  private final long sleepDelayBeforeSigKill;

  /**
   * 构造容器清理任务
   * @param context NodeManager全局上下文
   * @param configuration 配置对象
   * @param dispatcher 事件分发器
   * @param exec 容器执行器
   * @param container 待清理的容器
   * @param containerLaunch 容器启动对象
   */
  public ContainerCleanup(Context context, Configuration configuration,
      Dispatcher dispatcher, ContainerExecutor exec,
      Container container,
      ContainerLaunch containerLaunch) {

    this.context = Preconditions.checkNotNull(context, "context");
    this.conf = Preconditions.checkNotNull(configuration, "config");
    this.dispatcher = Preconditions.checkNotNull(dispatcher, "dispatcher");
    this.exec = Preconditions.checkNotNull(exec, "exec");
    this.container = Preconditions.checkNotNull(container, "container");
    this.launch = Preconditions.checkNotNull(containerLaunch, "launch");
    this.sleepDelayBeforeSigKill = conf.getLong(
        YarnConfiguration.NM_SLEEP_DELAY_BEFORE_SIGKILL_MS,
        YarnConfiguration.DEFAULT_NM_SLEEP_DELAY_BEFORE_SIGKILL_MS);
  }

  @Override
  public void run() {
    ContainerId containerId = container.getContainerId();
    String containerIdStr = containerId.toString();
    LOG.info("Cleaning up container " + containerIdStr);

    try {
      // 在状态存储中标记容器已被杀死
      context.getNMStateStore().storeContainerKilled(containerId);
    } catch (IOException e) {
      LOG.error("Unable to mark container " + containerId
          + " killed in store", e);
    }

    // 判断容器是否已经启动，只要启动完成/启动中/启动失败都算已启动
    boolean alreadyLaunched = !launch.markLaunched() ||
        launch.isLaunchCompleted();
    if (!alreadyLaunched) {
      LOG.info("Container " + containerIdStr + " not launched."
          + " No cleanup needed to be done");
      return;
    }
    LOG.debug("Marking container {} as inactive", containerIdStr);
    // 标记容器为非激活，确保未启动的容器不会再被启动
    exec.deactivateContainer(containerId);
    Path pidFilePath = launch.getPidFilePath();
    LOG.debug("Getting pid for container {} to kill"
        + " from pid file {}", containerIdStr, pidFilePath != null ?
        pidFilePath : "null");

    try {
      // 从pid文件或启动进程中获取容器进程ID
      String processId = launch.getContainerPid();

      String user = container.getUser();
      if (processId != null) {
        // 如果获取到进程ID，发送信号终止进程
        signalProcess(processId, user, containerIdStr);
      } else {
        // 如果还未生成pid文件，且启动未完成，直接发送容器杀死事件
        if (!launch.isLaunchCompleted()) {
          LOG.warn("Container clean up before pid file created "
              + containerIdStr);
          dispatcher.getEventHandler().handle(
              new ContainerExitEvent(container.getContainerId(),
                  ContainerEventType.CONTAINER_KILLED_ON_REQUEST,
                  Shell.WINDOWS ?
                      ContainerExecutor.ExitCode.FORCE_KILLED.getExitCode() :
                      ContainerExecutor.ExitCode.TERMINATED.getExitCode(),
                  "Container terminated before pid file created."));
        }
      }

      // 如果是Docker容器，提交延迟删除任务
      if (DockerLinuxContainerRuntime.isDockerContainerRequested(conf,
          container.getLaunchContext().getEnvironment())) {
        rmDockerContainerDelayed();
      }
    } catch (Exception e) {
      String message =
          "Exception when trying to cleanup container " + containerIdStr
              + ": " + StringUtils.stringifyException(e);
      LOG.warn(message);
      // 更新容器诊断信息通知异常
      dispatcher.getEventHandler().handle(
          new ContainerDiagnosticsUpdateEvent(containerId, message));
    } finally {
      // 清理pid文件和退出码文件
      if (pidFilePath != null) {
        try {
          FileContext lfs = FileContext.getLocalFSFileContext();
          lfs.delete(pidFilePath, false);
          lfs.delete(pidFilePath.suffix(EXIT_CODE_FILE_SUFFIX), false);
        } catch (IOException ioe) {
          LOG.warn("{} exception trying to delete pid file {}. Ignoring.",
              containerId, pidFilePath, ioe);
        }
      }
    }

    try {
      // 收割容器资源
      launch.reapContainer();
    } catch (IOException ioe) {
      LOG.warn("{} exception trying to reap container. Ignoring.", containerId,
          ioe);
    }
  }

  /**
   * 向删除服务提交Docker容器延迟删除任务
   */
  private void rmDockerContainerDelayed() {
    DeletionService deletionService = context.getDeletionService();
    DockerContainerDeletionTask deletionTask =
        new DockerContainerDeletionTask(deletionService, container.getUser(),
            container.getContainerId().toString());
    deletionService.delete(deletionTask);
  }

  /**
   * 向容器进程发送终止信号，先发送TERM，延迟后再发送KILL
   * @param processId 进程ID
   * @param user 容器所属用户
   * @param containerIdStr 容器ID字符串
   * @throws IOException 发送信号失败时抛出
   */
  private void signalProcess(String processId, String user,
      String containerIdStr) throws IOException {
    LOG.debug("Sending signal to pid {} as user {} for container {}",
        processId, user, containerIdStr);
    // 如果配置了延迟，则先发送TERM信号，否则直接发送KILL
    final ContainerExecutor.Signal signal =
        sleepDelayBeforeSigKill > 0 ? ContainerExecutor.Signal.TERM :
            ContainerExecutor.Signal.KILL;

    boolean result = sendSignal(user, processId, signal);
    LOG.debug("Sent signal {} to pid {} as user {} for container {},"
        + " result={}", signal, processId, user, containerIdStr,
        (result ? "success" : "failed"));

    if (sleepDelayBeforeSigKill > 0) {
      // 启动延迟杀死线程，等待配置时间后发送KILL信号
      new ContainerExecutor.DelayedProcessKiller(container, user, processId,
          sleepDelayBeforeSigKill, ContainerExecutor.Signal.KILL, exec).start();
    }
  }

  /**
   * 调用容器执行器发送信号给目标进程
   * @param user 进程所属用户
   * @param processId 目标进程ID
   * @param signal 要发送的信号
   * @return 发送是否成功
   * @throws IOException 发送失败时抛出
   */
  private boolean sendSignal(String user, String processId,
      ContainerExecutor.Signal signal)
      throws IOException {
    return exec.signalContainer(
        new ContainerSignalContext.Builder().setContainer(container)
            .setUser(user).setPid(processId).setSignal(signal).build());
  }
}