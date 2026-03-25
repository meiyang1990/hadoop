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

package org.apache.hadoop.yarn.server.api.protocolrecords;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.yarn.api.protocolrecords.SignalContainerRequest;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.SystemCredentialsForAppsProto;
import org.apache.hadoop.yarn.server.api.records.AppCollectorData;
import org.apache.hadoop.yarn.server.api.records.ContainerQueuingLimit;
import org.apache.hadoop.yarn.server.api.records.MasterKey;
import org.apache.hadoop.yarn.server.api.records.NodeAction;

/**
 * NodeManager向ResourceManager发送心跳后的响应抽象类，
 * 包含ResourceManager对NodeManager的各项指令和状态信息。
 */
public abstract class NodeHeartbeatResponse {
  /**
   * 获取响应ID，用于判断心跳响应是否重复。
   * @return 响应ID
   */
  public abstract int getResponseId();

  /**
   * 获取ResourceManager要求NodeManager执行的节点动作。
   * @return 节点动作枚举
   */
  public abstract NodeAction getNodeAction();

  /**
   * 获取需要清理的容器ID列表。
   * @return 待清理容器ID列表
   */
  public abstract List<ContainerId> getContainersToCleanup();

  /**
   * 获取需要从NodeManager上下文中移除的容器ID列表。
   * @return 待移除容器ID列表
   */
  public abstract List<ContainerId> getContainersToBeRemovedFromNM();

  /**
   * 获取需要清理的应用ID列表。
   * @return 待清理应用ID列表
   */
  public abstract List<ApplicationId> getApplicationsToCleanup();

  /**
   * 获取关联应用的应用收集器地址信息，用于告知NodeManager日志收集位置。
   * @return 应用ID对应收集器数据的映射
   */
  public abstract Map<ApplicationId, AppCollectorData> getAppCollectors();
  
  /**
   * 设置应用收集器地址映射。
   * @param appCollectorsMap 应用ID对应收集器数据的映射
   */
  public abstract void setAppCollectors(
      Map<ApplicationId, AppCollectorData> appCollectorsMap);

  /**
   * 设置响应ID。
   * @param responseId 响应ID
   */
  public abstract void setResponseId(int responseId);

  /**
   * 设置要求NodeManager执行的节点动作。
   * @param action 节点动作枚举
   */
  public abstract void setNodeAction(NodeAction action);

  /**
   * 获取容器令牌主密钥。
   * @return 容器令牌主密钥
   */
  public abstract MasterKey getContainerTokenMasterKey();

  /**
   * 设置容器令牌主密钥。
   * @param secretKey 容器令牌主密钥
   */
  public abstract void setContainerTokenMasterKey(MasterKey secretKey);

  /**
   * 获取NodeManager令牌主密钥。
   * @return NodeManager令牌主密钥
   */
  public abstract MasterKey getNMTokenMasterKey();

  /**
   * 设置NodeManager令牌主密钥。
   * @param secretKey NodeManager令牌主密钥
   */
  public abstract void setNMTokenMasterKey(MasterKey secretKey);

  /**
   * 批量添加需要清理的容器。
   * @param containers 待清理容器ID列表
   */
  public abstract void addAllContainersToCleanup(List<ContainerId> containers);

  /**
   * 批量添加需要从NodeManager上下文移除的容器，
   * 仅在ApplicationMaster确认已收到容器完成状态后才会移除。
   * @param containers 待移除容器ID列表
   */
  public abstract void addContainersToBeRemovedFromNM(
      List<ContainerId> containers);

  /**
   * 批量添加需要清理的应用。
   * @param applications 待清理应用ID列表
   */
  public abstract void addAllApplicationsToCleanup(
      List<ApplicationId> applications);

  /**
   * 获取需要发送信号的容器请求列表。
   * @return 信号容器请求列表
   */
  public abstract List<SignalContainerRequest> getContainersToSignalList();

  /**
   * 批量添加需要发送信号的容器请求。
   * @param containers 信号容器请求列表
   */
  public abstract void addAllContainersToSignal(
      List<SignalContainerRequest> containers);

  /**
   * 获取下一次心跳的间隔时间。
   * @return 下一次心跳间隔（毫秒）
   */
  public abstract long getNextHeartBeatInterval();

  /**
   * 设置下一次心跳的间隔时间。
   * @param nextHeartBeatInterval 下一次心跳间隔（毫秒）
   */
  public abstract void setNextHeartBeatInterval(long nextHeartBeatInterval);

  /**
   * 获取ResourceManager返回的诊断信息。
   * @return 诊断信息字符串
   */
  public abstract String getDiagnosticsMessage();

  /**
   * 设置ResourceManager返回的诊断信息。
   * @param diagnosticsMessage 诊断信息字符串
   */
  public abstract void setDiagnosticsMessage(String diagnosticsMessage);

  /**
   * 获取ResourceManager是否已接受NodeManager上报的节点标签。
   * @return true表示已接受，false表示未接受
   */
  public abstract boolean getAreNodeLabelsAcceptedByRM();

  /**
   * 设置节点标签是否被ResourceManager接受的标记。
   * @param areNodeLabelsAcceptedByRM true表示已接受，false表示未接受
   */
  public abstract void setAreNodeLabelsAcceptedByRM(
      boolean areNodeLabelsAcceptedByRM);

  /**
   * 获取ResourceManager确认的节点总资源量。
   * @return 节点总资源
   */
  public abstract Resource getResource();

  /**
   * 设置节点总资源量。
   * @param resource 节点总资源
   */
  public abstract void setResource(Resource resource);

  /**
   * 获取需要更新资源的容器列表。
   * @return 待更新容器列表
   */
  public abstract List<Container> getContainersToUpdate();

  /**
   * 批量添加需要更新资源的容器。
   * @param containersToUpdate 待更新容器集合
   */
  public abstract void addAllContainersToUpdate(
      Collection<Container> containersToUpdate);

  /**
   * 获取容器排队限制配置。
   * @return 容器排队限制
   */
  public abstract ContainerQueuingLimit getContainerQueuingLimit();

  /**
   * 设置容器排队限制配置。
   * @param containerQueuingLimit 容器排队限制
   */
  public abstract void setContainerQueuingLimit(
      ContainerQueuingLimit containerQueuingLimit);

  /**
   * 获取需要减少资源的容器列表。
   * @return 待资源缩减容器列表
   */
  public abstract List<Container> getContainersToDecrease();

  /**
   * 批量添加需要减少资源的容器。
   * @param containersToDecrease 待资源缩减容器集合
   */
  public abstract void addAllContainersToDecrease(
      Collection<Container> containersToDecrease);

  /**
   * 获取ResourceManager是否已接受NodeManager上报的节点属性。
   * @return true表示已接受，false表示未接受
   */
  public abstract boolean getAreNodeAttributesAcceptedByRM();

  /**
   * 设置节点属性是否被ResourceManager接受的标记。
   * @param areNodeAttributesAcceptedByRM true表示已接受，false表示未接受
   */
  public abstract void setAreNodeAttributesAcceptedByRM(
      boolean areNodeAttributesAcceptedByRM);

  /**
   * 设置令牌序列号，用于令牌版本控制。
   * @param tokenSequenceNo 令牌序列号
   */
  public abstract void setTokenSequenceNo(long tokenSequenceNo);

  /**
   * 获取令牌序列号。
   * @return 令牌序列号
   */
  public abstract long getTokenSequenceNo();

  /**
   * 设置应用系统凭证，供NodeManager进行应用本地化和日志聚合时使用。
   * @param systemCredentials 系统凭证Proto集合
   */
  public abstract void setSystemCredentialsForApps(
      Collection<SystemCredentialsForAppsProto> systemCredentials);

  /**
   * 获取供NodeManager使用的应用系统凭证集合。
   * @return 系统凭证Proto集合
   */
  public abstract Collection<SystemCredentialsForAppsProto>
      getSystemCredentialsForApps();
}