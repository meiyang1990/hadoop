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
package org.apache.hadoop.yarn.server.nodemanager.webapp;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SecureIOUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.LocalDirsHandlerService;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.Application;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerState;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.launcher.ContainerLaunch;

import org.apache.hadoop.yarn.webapp.NotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * NodeManager Web界面获取容器日志的工具类，提供安全方式获取用户日志文件的能力。
 * 包含权限检查、日志目录定位、安全打开日志文件等核心功能。
 */
public class ContainerLogsUtils {
  public static final Logger LOG = LoggerFactory.getLogger(ContainerLogsUtils.class);
  
  /**
   * 获取指定容器的所有本地日志目录列表，会验证访问权限和容器状态。
   * @param containerId 目标容器ID
   * @param remoteUser 请求访问的远程用户
   * @param context NodeManager上下文对象
   * @return 容器日志所在的所有本地目录列表
   * @throws YarnException 权限检查失败或容器不存在时抛出异常
   */
  public static List<File> getContainerLogDirs(ContainerId containerId,
      String remoteUser, Context context) throws YarnException {
    Container container = context.getContainers().get(containerId);

    Application application = getApplicationForContainer(containerId, context);
    checkAccess(remoteUser, application, context);
    // It is not required to have null check for container ( container == null )
    // and throw back exception.Because when container is completed, NodeManager
    // remove container information from its NMContext.Configuring log
    // aggregation to false, container log view request is forwarded to NM. NM
    // does not have completed container information,but still NM serve request for
    // reading container logs. 
    if (container != null) {
      checkState(container.getContainerState());
    }
    
    return getContainerLogDirs(containerId, context.getLocalDirsHandler());
  }
  
  /**
   * 根据本地目录处理器构造容器日志目录列表
   */
  static List<File> getContainerLogDirs(ContainerId containerId,
      LocalDirsHandlerService dirsHandler) throws YarnException {
    // 获取所有可读取的日志根目录
    List<String> logDirs = dirsHandler.getLogDirsForRead();
    List<File> containerLogDirs = new ArrayList<File>(logDirs.size());
    // 遍历每个日志根目录，构造当前容器对应的日志子目录
    for (String logDir : logDirs) {
      logDir = new File(logDir).toURI().getPath();
      String appIdStr = containerId
          .getApplicationAttemptId().getApplicationId().toString();
      File appLogDir = new File(logDir, appIdStr);
      containerLogDirs.add(new File(appLogDir, containerId.toString()));
    }
    return containerLogDirs;
  }
  
  /**
   * 获取指定容器下指定名称的日志文件，会验证访问权限和容器状态。
   * @param containerId 目标容器ID
   * @param fileName 日志文件名
   * @param remoteUser 请求访问的远程用户
   * @param context NodeManager上下文对象
   * @return 匹配的日志文件对象
   * @throws YarnException 权限检查失败或容器不存在时抛出异常
   */
  public static File getContainerLogFile(ContainerId containerId,
      String fileName, String remoteUser, Context context) throws YarnException {
    Container container = context.getContainers().get(containerId);
    
    Application application = getApplicationForContainer(containerId, context);
    checkAccess(remoteUser, application, context);
    if (container != null) {
      checkState(container.getContainerState());
    }
    
    try {
      LocalDirsHandlerService dirsHandler = context.getLocalDirsHandler();
      // 构造容器日志目录的相对路径
      String relativeContainerLogDir = ContainerLaunch.getRelativeContainerLogDir(
          application.getAppId().toString(), containerId.toString());
      // 获取完整日志文件路径
      Path logPath = dirsHandler.getLogPathToRead(
          relativeContainerLogDir + Path.SEPARATOR + fileName);
      URI logPathURI = new File(logPath.toString()).toURI();
      File logFile = new File(logPathURI.getPath());
      return logFile;
    } catch (IOException e) {
      LOG.warn("Failed to find log file", e);
      throw new NotFoundException("Cannot find this log on the local disk.");
    }
  }
  
  /**
   * 根据容器ID获取所属应用，不存在则抛出404异常
   */
  private static Application getApplicationForContainer(ContainerId containerId,
      Context context) {
    ApplicationId applicationId = containerId.getApplicationAttemptId()
        .getApplicationId();
    Application application = context.getApplications().get(
        applicationId);
    
    if (application == null) {
      throw new NotFoundException(
          "Unknown container. Container either has not started or "
              + "has already completed or "
              + "doesn't belong to this node at all.");
    }
    return application;
  }
  
  /**
   * 检查用户是否有查看应用日志的权限，无权限则抛出异常
   */
  private static void checkAccess(String remoteUser, Application application,
      Context context) throws YarnException {
    UserGroupInformation callerUGI = null;
    if (remoteUser != null) {
      // 创建远程用户的UGI对象
      callerUGI = UserGroupInformation.createRemoteUser(remoteUser);
    }
    if (callerUGI != null
        && !context.getApplicationACLsManager().checkAccess(callerUGI,
            ApplicationAccessType.VIEW_APP, application.getUser(),
            application.getAppId())) {
      // 权限检查不通过，抛出异常
      throw new YarnException(
          "User [" + remoteUser
              + "] is not authorized to view the logs for application "
              + application.getAppId());
    }
  }
  
  /**
   * 检查容器状态，未启动完成则抛出404异常
   */
  private static void checkState(ContainerState state) {
    if (state == ContainerState.NEW || state == ContainerState.LOCALIZING ||
        state == ContainerState.SCHEDULED) {
      throw new NotFoundException("Container is not yet running. Current state is "
          + state);
    }
    if (state == ContainerState.LOCALIZATION_FAILED) {
      throw new NotFoundException("Container wasn't started. Localization failed.");
    }
  }
  
  /**
   * 以安全权限校验方式打开日志文件进行读取，验证文件所有者匹配。
   * @param containerIdStr 容器ID字符串
   * @param logFile 要打开的日志文件
   * @param context NodeManager上下文对象
   * @return 日志文件输入流
   * @throws IOException 打开失败或权限不匹配时抛出异常
   */
  public static FileInputStream openLogFileForRead(String containerIdStr, File logFile,
      Context context) throws IOException {
    ContainerId containerId = ContainerId.fromString(containerIdStr);
    ApplicationId applicationId = containerId.getApplicationAttemptId()
        .getApplicationId();
    // 获取提交应用的用户，用于权限校验
    String user = context.getApplications().get(
        applicationId).getUser();
    
    try {
      // 使用安全IO工具打开文件，验证所有者
      return SecureIOUtils.openForRead(logFile, user, null);
    } catch (IOException e) {
      // 处理所有者不匹配的情况，给出明确错误信息
      if (e.getMessage().contains(
        "did not match expected owner '" + user
            + "'")) {
        LOG.error(
            "Exception reading log file " + logFile.getAbsolutePath(), e);
        throw new IOException("Exception reading log file. Application submitted by '"
            + user
            + "' doesn't own requested log file : "
            + logFile.getName(), e);
      } else {
        // 其他IO异常，推测可能是日志已经被聚合清理
        throw new IOException("Exception reading log file. It might be because log "
            + "file was aggregated : " + logFile.getName(), e);
      }
    }
  }
}