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

import java.io.File;
import java.io.InterruptedIOException;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.server.nodemanager.ContainerExecutor;
import org.apache.hadoop.yarn.server.nodemanager.ContainerExecutor.ExitCode;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.LocalDirsHandlerService;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.ContainerManagerImpl;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.Application;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerExitEvent;
import org.apache.hadoop.yarn.server.nodemanager.executor.ContainerReacquisitionContext;

/**
 * NodeManager重启（用于滚动升级）后恢复已有容器的启动处理类，负责接管已运行的容器
 */
public class RecoveredContainerLaunch extends ContainerLaunch {

  private static final Logger LOG =
       LoggerFactory.getLogger(RecoveredContainerLaunch.class);

  /**
   * 构造恢复容器启动处理对象，标记容器已启动
   */
  public RecoveredContainerLaunch(Context context, Configuration configuration,
      Dispatcher dispatcher, ContainerExecutor exec, Application app,
      Container container, LocalDirsHandlerService dirsHandler,
      ContainerManagerImpl containerManager)
  {
    super(context, configuration, dispatcher, exec, app, container, dirsHandler,
      containerManager);
    this.containerAlreadyLaunched.set(true);
  }

  /**
   * 等待已恢复容器进程退出并获取退出码，处理后续完成逻辑
   * @return 容器退出码
   */
  @SuppressWarnings("unchecked")
  @Override
  public Integer call() {
    // 默认退出码设为进程丢失
    int retCode = ExitCode.LOST.getExitCode();
    ContainerId containerId = container.getContainerId();
    String appIdStr =
        containerId.getApplicationAttemptId().getApplicationId().toString();
    String containerIdStr = containerId.toString();

    // 发送容器已启动事件
    dispatcher.getEventHandler().handle(new ContainerEvent(containerId,
        ContainerEventType.CONTAINER_LAUNCHED));

    boolean interrupted = false;
    try {
      // 查找容器PID文件
      File pidFile = locatePidFile(appIdStr, containerIdStr);
      if (pidFile != null) {
        String pidPathStr = pidFile.getPath();
        pidFilePath = new Path(pidPathStr);
        // 激活容器，关联PID文件
        exec.activateContainer(containerId, pidFilePath);
        // 重新获取已运行容器，等待其退出并获取退出码
        retCode = exec.reacquireContainer(
            new ContainerReacquisitionContext.Builder()
                .setContainer(container)
                .setUser(container.getUser())
                .setContainerId(containerId)
                .build());
      } else {
        LOG.warn("Unable to locate pid file for container " + containerIdStr);
      }
    } catch (InterruptedException | InterruptedIOException e) {
      LOG.warn("Interrupted while waiting for exit code from " + containerId);
      interrupted = true;
    } catch (IOException e) {
      LOG.error("Unable to recover container " + containerIdStr, e);
    } finally {
      if (!interrupted) {
        // 标记容器已完成
        this.completed.set(true);
        // 取消容器激活状态
        exec.deactivateContainer(containerId);
        try {
          // 持久化存储容器退出状态
          getContext().getNMStateStore().storeContainerCompleted(containerId,
              retCode);
        } catch (IOException e) {
          LOG.error("Unable to set exit code for container " + containerId);
        }
      }
    }

    // 非零退出码处理，发送容器失败退出事件
    if (retCode != 0) {
      LOG.warn("Recovered container exited with a non-zero exit code "
          + retCode);
      this.dispatcher.getEventHandler().handle(new ContainerExitEvent(
          containerId,
          ContainerEventType.CONTAINER_EXITED_WITH_FAILURE, retCode,
          "Container exited with a non-zero exit code " + retCode));
      return retCode;
    }

    // 零退出码处理，发送容器成功退出事件
    LOG.info("Recovered container " + containerId + " succeeded");
    dispatcher.getEventHandler().handle(
        new ContainerEvent(containerId,
            ContainerEventType.CONTAINER_EXITED_WITH_SUCCESS));
    return 0;
  }

  /**
   * 在所有本地目录中查找容器的PID文件
   * @param appIdStr 应用ID字符串
   * @param containerIdStr 容器ID字符串
   * @return 找到的PID文件，找不到返回null
   */
  private File locatePidFile(String appIdStr, String containerIdStr) {
    String pidSubpath= getPidFileSubpath(appIdStr, containerIdStr);
    // 遍历所有可读本地目录查找PID文件
    for (String dir : getContext().getLocalDirsHandler().
        getLocalDirsForRead()) {
      File pidFile = new File(dir, pidSubpath);
      if (pidFile.exists()) {
        return pidFile;
      }
    }
    return null;
  }
}