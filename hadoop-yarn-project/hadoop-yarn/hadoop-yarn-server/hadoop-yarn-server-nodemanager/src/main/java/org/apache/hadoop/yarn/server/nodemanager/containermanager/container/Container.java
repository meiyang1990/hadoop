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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.container;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.LocalizationStatus;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.security.ContainerTokenIdentifier;
import org.apache.hadoop.yarn.server.api.protocolrecords.NMContainerStatus;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.ResourceSet;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.runtime.ContainerExecutionException;

import java.util.List;
import java.util.Map;

/**
 * YARN NodeManager 容器抽象接口，定义容器核心行为与属性访问方法，
 * 容器是YARN中运行用户任务的基本单位，负责管理单个任务的生命周期、资源和状态。
 */
public interface Container extends EventHandler<ContainerEvent> {

  /**
   * 获取容器唯一编号。
   * @return 容器ID
   */
  ContainerId getContainerId();

  /**
   * The timestamp when the container start request is received.
   */
  long getContainerStartTime();

  /**
   * The timestamp when the container is allowed to be launched.
   */
  long getContainerLaunchTime();

  /**
   * 获取容器分配的资源量。
   * @return 容器分配资源（包含内存、CPU等）
   */
  Resource getResource();

  /**
   * 获取容器令牌标识符，用于容器安全认证。
   * @return 容器令牌标识符
   */
  ContainerTokenIdentifier getContainerTokenIdentifier();

  /**
   * 设置容器令牌标识符。
   * @param token 新的容器令牌标识符
   */
  void setContainerTokenIdentifier(ContainerTokenIdentifier token);

  /**
   * 获取提交该容器的用户名称。
   * @return 用户名
   */
  String getUser();
  
  /**
   * 获取容器当前状态。
   * @return 容器状态枚举
   */
  ContainerState getContainerState();

  /**
   * 获取容器启动上下文，包含启动命令、环境变量、应用数据等。
   * @return 容器启动上下文
   */
  ContainerLaunchContext getLaunchContext();

  /**
   * 获取容器安全凭证，用于访问集群资源时认证。
   * @return 安全凭证
   */
  Credentials getCredentials();

  /**
   * 获取已本地化完成的资源映射。
   * @return 资源路径与对应的链接名称列表映射
   */
  Map<Path,List<String>> getLocalizedResources();

  /**
   * 克隆并获取容器当前状态信息。
   * @return 容器状态
   */
  ContainerStatus cloneAndGetContainerStatus();

  /**
   * 获取NodeManager侧容器状态，用于节点上报给ResourceManager。
   * @return NM容器状态
   */
  NMContainerStatus getNMContainerStatus();

  /**
   * 检查是否设置了容器重试上下文。
   * @return 是否设置重试上下文
   */
  boolean isRetryContextSet();

  /**
   * 根据错误码判断容器是否应该重试启动。
   * @param errorCode 错误码
   * @return 是否需要重试
   */
  boolean shouldRetry(int errorCode);

  /**
   * 获取容器工作目录路径。
   * @return 工作目录路径
   */
  String getWorkDir();

  /**
   * 设置容器工作目录路径。
   * @param workDir 工作目录路径
   */
  void setWorkDir(String workDir);

  /**
   * 获取容器CSI存储卷根目录路径。
   * @return CSI卷根目录路径
   */
  String getCsiVolumesRootDir();

  /**
   * 设置容器CSI存储卷根目录路径。
   * @param volumesRootDir CSI卷根目录路径
   */
  void setCsiVolumesRootDir(String volumesRootDir);

  /**
   * 获取容器日志目录路径。
   * @return 日志目录路径
   */
  String getLogDir();

  /**
   * 设置容器日志目录路径。
   * @param logDir 日志目录路径
   */
  void setLogDir(String logDir);

  /**
   * 设置容器IP与主机信息。
   * @param ipAndHost IP与主机数组
   */
  void setIpAndHost(String[] ipAndHost);

  /**
   * 设置容器暴露的端口列表。
   * @param ports 暴露端口字符串
   */
  void setExposedPorts(String ports);

  String toString();

  /**
   * 获取容器调度优先级。
   * @return 容器优先级
   */
  Priority getPriority();

  /**
   * 获取容器资源集合，管理容器所有需要本地化的资源。
   * @return 资源集合对象
   */
  ResourceSet getResourceSet();

  /**
   * 检查容器是否处于运行状态。
   * @return 是否正在运行
   */
  boolean isRunning();

  /**
   * 设置容器是否正在重新初始化。
   * @param isReInitializing 是否正在重新初始化
   */
  void setIsReInitializing(boolean isReInitializing);

  /**
   * 检查容器是否正在重新初始化。
   * @return 是否正在重新初始化
   */
  boolean isReInitializing();

  /**
   * 检查容器是否已被标记为需要杀死。
   * @return 是否标记为杀死
   */
  boolean isMarkedForKilling();

  /**
   * 检查容器是否支持升级回滚。
   * @return 是否可以回滚
   */
  boolean canRollback();

  /**
   * 提交容器升级，确认升级完成。
   */
  void commitUpgrade();

  /**
   * 发送容器启动事件，触发容器启动流程。
   */
  void sendLaunchEvent();

  /**
   * 发送容器杀死事件，触发容器终止流程。
   * @param exitStatus 退出状态码
   * @param description 杀死原因描述
   */
  void sendKillEvent(int exitStatus, String description);

  /**
   * 检查容器是否正在恢复中。
   * @return 是否正在恢复
   */
  boolean isRecovering();

  /**
   * 设置容器运行时数据，存储不同容器运行时的私有数据。
   * @param containerRuntimeData 运行时数据对象
   */
  void setContainerRuntimeData(Object containerRuntimeData);

  /**
   * 获取容器运行时数据，按类型转换返回。
   * @param runtimeClazz 期望的数据类型
   * @return 转换后的运行时数据
   * @throws ContainerExecutionException 类型转换失败时抛出异常
   */
  <T> T getContainerRuntimeData(Class<T> runtimeClazz)
      throws ContainerExecutionException;

  /**
   * Get assigned resource mappings to the container.
   *
   * @return Resource Mappings of the container
   */
  ResourceMappings getResourceMappings();

  /**
   * 发送容器暂停事件，触发容器暂停流程。
   * @param description 暂停原因描述
   */
  void sendPauseEvent(String description);

  /**
   * Verify container is in final states.
   * @return true/false based on container's state
   */
  boolean isContainerInFinalStates();

  /**
   * Get the localization statuses.
   * @return localization statuses.
   */
  List<LocalizationStatus> getLocalizationStatuses();

  /**
   * Vector of localization counters to be passed from NM to application
   * container via environment variable {@code $LOCALIZATION_COUNTERS}. See
   * {@link org.apache.hadoop.yarn.api.ApplicationConstants.Environment#LOCALIZATION_COUNTERS}
   *
   * @return coma-separated counter values
   */
  String localizationCountersAsString();

}