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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.exceptions.ConfigurationException;
import org.apache.hadoop.yarn.server.nodemanager.ContainerExecutor;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.LocalDirsHandlerService;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.ContainerManagerImpl;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.Application;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerExitEvent;
import org.apache.hadoop.yarn.server.nodemanager.executor.ContainerStartContext;
import org.apache.hadoop.yarn.server.security.AMSecretKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * YARN NodeManager容器重新启动处理器，负责执行已退出容器的重新启动流程。
 * 继承自ContainerLaunch，复用容器启动通用逻辑，针对重新启动场景做特殊处理。
 */
public class ContainerRelaunch extends ContainerLaunch {

  private static final Logger LOG =
       LoggerFactory.getLogger(ContainerRelaunch.class);

  /**
   * 构造容器重新启动任务实例，初始化所需上下文与依赖。
   * @param context NodeManager全局上下文
   * @param configuration 节点配置
   * @param dispatcher 事件分发器
   * @param exec 容器执行器
   * @param app 容器所属应用
   * @param container 待重新启动的容器
   * @param dirsHandler 本地目录处理器
   * @param containerManager 容器管理器实例
   */
  public ContainerRelaunch(Context context, Configuration configuration,
      Dispatcher dispatcher, ContainerExecutor exec, Application app,
      Container container, LocalDirsHandlerService dirsHandler,
      ContainerManagerImpl containerManager) {
    super(context, configuration, dispatcher, exec, app, container, dirsHandler,
        containerManager);
  }

  /**
   * 执行容器重新启动逻辑，是可调用任务的入口方法。
   * @return 容器最终退出码，0表示跳过处理，非0表示启动失败或退出状态
   */
  @Override
  public Integer call() {
    // 验证容器状态，不合法则直接返回
    if (!validateContainerState()) {
      return 0;
    }

    ContainerId containerId = container.getContainerId();
    String containerIdStr = containerId.toString();
    int ret = -1;
    Path containerLogDir;
    try {
      // 获取容器工作目录
      Path containerWorkDir = getContainerWorkDir();
      // 清理容器上一次运行残留文件，为重新启动做准备
      cleanupContainerFiles(containerWorkDir);

      containerLogDir = getContainerLogDir();

      // 获取已本地化完成的容器资源映射
      Map<Path, List<String>> localResources = getLocalizedResources();

      String appIdStr = app.getAppId().toString();
      // 获取NM私有目录下容器启动脚本路径
      Path nmPrivateContainerScriptPath =
          getNmPrivateContainerScriptPath(appIdStr, containerIdStr);
      // 获取NM私有目录下令牌文件路径
      Path nmPrivateTokensPath =
          getNmPrivateTokensPath(appIdStr, containerIdStr);
      // 如果凭证中存在keystore密钥，获取NM私有目录下keystore文件路径，否则为null
      Path nmPrivateKeystorePath = (container.getCredentials().getSecretKey(
          AMSecretKeys.YARN_APPLICATION_AM_KEYSTORE) == null) ? null :
          getNmPrivateKeystorePath(appIdStr, containerIdStr);
      // 如果凭证中存在truststore密钥，获取NM私有目录下truststore文件路径，否则为null
      Path nmPrivateTruststorePath = (container.getCredentials().getSecretKey(
          AMSecretKeys.YARN_APPLICATION_AM_TRUSTSTORE) == null) ? null :
          getNmPrivateTruststorePath(appIdStr, containerIdStr);
      try {
        // 尝试查找已存在的PID文件
        pidFilePath = getPidFilePath(appIdStr, containerIdStr);
      } catch (IOException e) {
        // PID文件不存在时，新建PID文件路径
        String pidFileSubpath = getPidFileSubpath(appIdStr, containerIdStr);
        pidFilePath = dirsHandler.getLocalPathForWrite(pidFileSubpath);
      }

      // 打印重新启动容器关键路径信息日志
      LOG.info("Relaunch container with "
          + "workDir = " + containerWorkDir.toString()
          + ", logDir = " + containerLogDir.toString()
          + ", nmPrivateContainerScriptPath = "
          + nmPrivateContainerScriptPath.toString()
          + ", nmPrivateTokensPath = " + nmPrivateTokensPath.toString()
          + ", pidFilePath = " + pidFilePath.toString());

      // 获取节点所有本地目录列表
      List<String> localDirs = dirsHandler.getLocalDirs();
      // 获取节点所有日志目录列表
      List<String> logDirs = dirsHandler.getLogDirs();
      // 获取容器专属本地目录
      List<String> containerLocalDirs = getContainerLocalDirs(localDirs);
      // 获取容器专属日志目录
      List<String> containerLogDirs = getContainerLogDirs(logDirs);
      // 获取NM文件缓存目录
      List<String> filecacheDirs = getNMFilecacheDirs(localDirs);
      // 获取用户本地目录
      List<String> userLocalDirs = getUserLocalDirs(localDirs);
      // 获取用户文件缓存目录
      List<String> userFilecacheDirs = getUserFilecacheDirs(localDirs);
      // 获取应用专属本地目录
      List<String> applicationLocalDirs = getApplicationLocalDirs(localDirs,
          appIdStr);

      // 检查磁盘健康状态，大部分磁盘失败则直接返回失败
      if (!dirsHandler.areDisksHealthy()) {
        ret = ContainerExitStatus.DISKS_FAILED;
        throw new IOException("Most of the disks failed. "
            + dirsHandler.getDisksHealthReport(false));
      }

      // 构建容器启动上下文，执行容器重新启动
      ret = relaunchContainer(new ContainerStartContext.Builder()
          .setContainer(container)
          .setLocalizedResources(localResources)
          .setNmPrivateContainerScriptPath(nmPrivateContainerScriptPath)
          .setNmPrivateTokensPath(nmPrivateTokensPath)
          .setNmPrivateKeystorePath(nmPrivateKeystorePath)
          .setNmPrivateTruststorePath(nmPrivateTruststorePath)
          .setUser(container.getUser())
          .setAppId(appIdStr)
          .setContainerWorkDir(containerWorkDir)
          .setLocalDirs(localDirs)
          .setLogDirs(logDirs)
          .setFilecacheDirs(filecacheDirs)
          .setUserLocalDirs(userLocalDirs)
          .setContainerLocalDirs(containerLocalDirs)
          .setContainerLogDirs(containerLogDirs)
          .setUserFilecacheDirs(userFilecacheDirs)
          .setApplicationLocalDirs(applicationLocalDirs)
          .build());
    } catch (ConfigurationException e) {
      // 配置错误导致重新启动失败，记录日志并发送容器退出失败事件
      LOG.error("Failed to launch container due to configuration error.", e);
      dispatcher.getEventHandler().handle(new ContainerExitEvent(
          containerId, ContainerEventType.CONTAINER_EXITED_WITH_FAILURE, ret,
          e.getMessage()));
      // 将节点标记为不健康，上报异常到ResourceManager
      getContext().getNodeStatusUpdater().reportException(e);
      return ret;
    } catch (Throwable e) {
      // 其他异常导致重新启动失败，记录日志并发送容器退出失败事件
      LOG.warn("Failed to relaunch container.", e);
      dispatcher.getEventHandler().handle(new ContainerExitEvent(
          containerId, ContainerEventType.CONTAINER_EXITED_WITH_FAILURE, ret,
          e.getMessage()));
      return ret;
    } finally {
      // 设置容器完成状态
      setContainerCompletedStatus(ret);
    }

    // 根据退出码做后续处理，例如日志归档
    handleContainerExitCode(ret, containerLogDir);

    return ret;
  }


  /**
   * 获取容器日志目录并验证目录可用性。
   * @return 容器日志目录Path对象
   * @throws IOException 当目录不存在或不可用抛出异常
   */
  private Path getContainerLogDir() throws IOException {
    String containerLogDir = container.getLogDir();
    if (containerLogDir == null || !dirsHandler.isGoodLogDir(containerLogDir)) {
      throw new IOException("Could not find a good log dir " + containerLogDir
          + " for container " + container);
    }

    return new Path(containerLogDir);
  }

  /**
   * 获取NM私有目录下容器启动脚本的可读路径。
   * @param appIdStr 应用ID字符串
   * @param containerIdStr 容器ID字符串
   * @return 启动脚本Path对象
   * @throws IOException 获取路径失败抛出异常
   */
  private Path getNmPrivateContainerScriptPath(String appIdStr,
      String containerIdStr) throws IOException {
    return dirsHandler.getLocalPathForRead(
        getContainerPrivateDir(appIdStr, containerIdStr) + Path.SEPARATOR
            + CONTAINER_SCRIPT);
  }

  /**
   * 获取NM私有目录下容器令牌文件的可读路径。
   * @param appIdStr 应用ID字符串
   * @param containerIdStr 容器ID字符串
   * @return 令牌文件Path对象
   * @throws IOException 获取路径失败抛出异常
   */
  private Path getNmPrivateTokensPath(String appIdStr,
       String containerIdStr) throws IOException {
    return dirsHandler.getLocalPathForRead(
        getContainerPrivateDir(appIdStr, containerIdStr) + Path.SEPARATOR
            + String.format(ContainerExecutor.TOKEN_FILE_NAME_FMT,
            containerIdStr));
  }

  /**
   * 获取NM私有目录下keystore文件的可读路径。
   * @param appIdStr 应用ID字符串
   * @param containerIdStr 容器ID字符串
   * @return keystore文件Path对象
   * @throws IOException 获取路径失败抛出异常
   */
  private Path getNmPrivateKeystorePath(String appIdStr,
      String containerIdStr) throws IOException {
    return dirsHandler.getLocalPathForRead(
        getContainerPrivateDir(appIdStr, containerIdStr) + Path.SEPARATOR
            + ContainerLaunch.KEYSTORE_FILE);
  }

  /**
   * 获取NM私有目录下truststore文件的可读路径。
   * @param appIdStr 应用ID字符串
   * @param containerIdStr 容器ID字符串
   * @return truststore文件Path对象
   * @throws IOException 获取路径失败抛出异常
   */
  private Path getNmPrivateTruststorePath(String appIdStr,
      String containerIdStr) throws IOException {
    return dirsHandler.getLocalPathForRead(
        getContainerPrivateDir(appIdStr, containerIdStr) + Path.SEPARATOR
            + ContainerLaunch.TRUSTSTORE_FILE);
  }

  /**
   * 获取容器PID文件的可读路径。
   * @param appIdStr 应用ID字符串
   * @param containerIdStr 容器ID字符串
   * @return PID文件Path对象
   * @throws IOException 获取路径失败抛出异常
   */
  private Path getPidFilePath(String appIdStr,
      String containerIdStr) throws IOException {
    return dirsHandler.getLocalPathForRead(
        getPidFileSubpath(appIdStr, containerIdStr));
  }
}