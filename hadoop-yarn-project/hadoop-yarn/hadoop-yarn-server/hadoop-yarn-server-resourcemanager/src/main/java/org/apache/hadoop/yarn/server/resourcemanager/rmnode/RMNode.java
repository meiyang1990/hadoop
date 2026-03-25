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

package org.apache.hadoop.yarn.server.resourcemanager.rmnode;


import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.net.Node;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceUtilization;
import org.apache.hadoop.yarn.api.records.NodeAttribute;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatResponse;
import org.apache.hadoop.yarn.server.api.records.OpportunisticContainersStatus;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.util.resource.Resources;

/**
 * YARN ResourceManager 中对 NodeManager 节点的抽象接口，定义了节点需要维护的资源信息、状态信息和核心操作。
 * 记录集群中单个NodeManager节点的可用资源、健康状态、容器信息等核心数据。
 */
public interface RMNode {

  /**
   * 获取当前节点的唯一标识ID。
   * @return 当前节点的NodeId
   */
  public NodeId getNodeID();
  
  /**
   * 获取当前节点的主机名。
   * @return 当前节点的主机名
   */
  public String getHostName();
  
  /**
   * 获取当前节点的命令服务端口。
   * @return 当前节点的命令服务端口
   */
  public int getCommandPort();
  
  /**
   * 获取当前节点的HTTP服务端口。
   * @return 当前节点的HTTP服务端口
   */
  public int getHttpPort();


  /**
   * 获取当前节点ContainerManager服务的完整地址。
   * @return ContainerManager服务地址
   */
  public String getNodeAddress();
  
  /**
   * 获取当前节点HTTP服务的完整地址。
   * @return HTTP服务地址
   */
  public String getHttpAddress();
  
  /**
   * 获取从当前节点收到的最新健康报告。
   * @return 最新健康报告文本
   */
  public String getHealthReport();
  
  /**
   * 获取最近一次收到健康报告的时间戳。
   * @return 最近一次健康报告的时间戳
   */
  public long getLastHealthReportTime();

  /**
   * 获取节点注册时上报的NodeManager版本号。
   * @return NodeManager版本号
   */
  public String getNodeManagerVersion();

  /**
   * 获取当前节点的总可用资源能力。
   * @return 节点总资源能力
   */
  public Resource getTotalCapability();

  /**
   * 获取当前节点已分配给容器的总资源，包含所有状态（排队、运行、暂停）的保障型和机会型容器。
   * @return 所有已分配容器的总资源
   */
  default Resource getAllocatedContainerResource() {
    return Resources.none();
  }

  /**
   * 检查节点总资源能力是否已更新。
   * @return 若资源能力已更新返回true，否则返回false
   */
  boolean isUpdatedCapability();

  /**
   * 标记资源更新事件已处理，重置更新标记。
   */
  void resetUpdatedCapability();

  /**
   * 获取节点上所有容器的聚合资源使用率。
   * @return 容器聚合资源使用率
   */
  public ResourceUtilization getAggregatedContainersUtilization();

  /**
   * 获取整个节点的总资源使用率。
   * @return 节点总资源使用率
   */
  public ResourceUtilization getNodeUtilization();

  /**
   * 获取节点的物理资源总量。
   * @return 节点物理资源总量
   */
  Resource getPhysicalResource();

  /**
   * 获取当前节点所属机架名称。
   * @return 机架名称
   */
  public String getRackName();
  
  /**
   * 获取当前节点对应的底层网络Node对象。
   * @return 网络层Node对象
   */
  public Node getNode();
  
  /**
   * 获取当前节点的状态。
   * @return 节点状态
   */
  public NodeState getState();

  /**
   * 获取需要清理的容器ID列表。
   * @return 需要清理的容器ID列表
   */
  public List<ContainerId> getContainersToCleanUp();

  /**
   * 获取需要清理的应用ID列表。
   * @return 需要清理的应用ID列表
   */
  public List<ApplicationId> getAppsToCleanup();

  /**
   * 获取当前节点上正在运行的应用ID列表。
   * @return 正在运行的应用ID列表
   */
  List<ApplicationId> getRunningApps();

  /**
   * 更新心跳响应，填入需要本节点清理的容器、应用列表，以及需要更新的容器信息。
   *
   * @param response 待更新的NodeManager心跳响应对象
   */
  void setAndUpdateNodeHeartbeatResponse(NodeHeartbeatResponse response);

  /**
   * 获取上一次发送给NodeManager的心跳响应。
   * @return 上一次心跳响应
   */
  public NodeHeartbeatResponse getLastNodeHeartBeatResponse();

  /**
   * 重置上一次心跳响应的ID为0。
   */
  void resetLastNodeHeartBeatResponse();

  /**
   * 获取并清空累计多次心跳的容器更新列表。
   * 
   * @return 累计的容器更新列表
   */
  public List<UpdatedContainerInfo> pullContainerUpdates();
  
  /**
   * 获取当前节点拥有的标签集合。
   * 
   * @return 节点标签集合
   */
  public Set<String> getNodeLabels();

  /**
   * 获取并清空新增容器列表。
   * @return 新增容器列表
   */
  public List<Container> pullNewlyIncreasedContainers();

  /**
   * 获取当前节点机会型容器的状态。
   * @return 机会型容器状态
   */
  OpportunisticContainersStatus getOpportunisticContainersStatus();

  /**
   * 获取节点被标记为未跟踪的时间戳。
   * @return 未跟踪时间戳
   */
  long getUntrackedTimeStamp();

  /**
   * 设置节点未跟踪时间戳。
   * @param timeStamp 未跟踪时间戳
   */
  void setUntrackedTimeStamp(long timeStamp);
  
  /**
   * 获取节点退役超时时间（秒），null表示无超时。
   * @return 退役超时时间（秒）
   */
  Integer getDecommissioningTimeout();

  /**
   * 获取当前节点关联的分配标签及其计数。
   * @return 分配标签到计数的映射
   */
  Map<String, Long> getAllocationTagsWithCount();

  /**
   * 获取当前节点所属的ResourceManager上下文。
   * @return ResourceManager上下文对象
   */
  RMContext getRMContext();

  /**
   * 获取当前节点所有属性集合。
   * @return 节点属性集合
   */
  Set<NodeAttribute> getAllNodeAttributes();

  /**
   * 根据节点负载情况计算下一次心跳的间隔时间。
   * @param defaultInterval 默认心跳间隔
   * @param minInterval 最小心跳间隔
   * @param maxInterval 最大心跳间隔
   * @param speedupFactor 负载加速因子
   * @param slowdownFactor 负载减速因子
   * @return 计算得到的下一次心跳间隔
   */
  long calculateHeartBeatInterval(long defaultInterval,
      long minInterval, long maxInterval, float speedupFactor,
      float slowdownFactor);
}