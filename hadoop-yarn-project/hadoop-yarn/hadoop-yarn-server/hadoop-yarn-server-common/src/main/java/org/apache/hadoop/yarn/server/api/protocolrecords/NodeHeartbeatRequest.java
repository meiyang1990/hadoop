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

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.NodeLabel;
import org.apache.hadoop.yarn.server.api.records.AppCollectorData;
import org.apache.hadoop.yarn.server.api.records.MasterKey;
import org.apache.hadoop.yarn.server.api.records.NodeStatus;
import org.apache.hadoop.yarn.util.Records;
import org.apache.hadoop.yarn.api.records.NodeAttribute;

/**
 * NodeManager向ResourceManager发送心跳请求的协议记录
 * 封装心跳上报所需的全部节点状态信息
 */
public abstract class NodeHeartbeatRequest {
  
  /**
   * 创建NodeHeartbeatRequest实例（基础版本）
   * @param nodeStatus 节点状态信息
   * @param lastKnownContainerTokenMasterKey 节点已知最新容器令牌主密钥
   * @param lastKnownNMTokenMasterKey 节点已知最新NM令牌主密钥
   * @param nodeLabels 节点标签集合
   * @return 构建完成的心跳请求对象
   */
  public static NodeHeartbeatRequest newInstance(NodeStatus nodeStatus,
      MasterKey lastKnownContainerTokenMasterKey,
      MasterKey lastKnownNMTokenMasterKey, Set<NodeLabel> nodeLabels) {
    NodeHeartbeatRequest nodeHeartbeatRequest =
        Records.newRecord(NodeHeartbeatRequest.class);
    nodeHeartbeatRequest.setNodeStatus(nodeStatus);
    nodeHeartbeatRequest
        .setLastKnownContainerTokenMasterKey(lastKnownContainerTokenMasterKey);
    nodeHeartbeatRequest
        .setLastKnownNMTokenMasterKey(lastKnownNMTokenMasterKey);
    nodeHeartbeatRequest.setNodeLabels(nodeLabels);
    return nodeHeartbeatRequest;
  }

  /**
   * 创建NodeHeartbeatRequest实例（增加注册收集器信息）
   * @param nodeStatus 节点状态信息
   * @param lastKnownContainerTokenMasterKey 节点已知最新容器令牌主密钥
   * @param lastKnownNMTokenMasterKey 节点已知最新NM令牌主密钥
   * @param nodeLabels 节点标签集合
   * @param registeringCollectors 本节点上正在注册的应用收集器信息映射
   * @return 构建完成的心跳请求对象
   */
  public static NodeHeartbeatRequest newInstance(NodeStatus nodeStatus,
      MasterKey lastKnownContainerTokenMasterKey,
      MasterKey lastKnownNMTokenMasterKey, Set<NodeLabel> nodeLabels,
      Map<ApplicationId, AppCollectorData> registeringCollectors) {
    NodeHeartbeatRequest nodeHeartbeatRequest =
        Records.newRecord(NodeHeartbeatRequest.class);
    nodeHeartbeatRequest.setNodeStatus(nodeStatus);
    nodeHeartbeatRequest
        .setLastKnownContainerTokenMasterKey(lastKnownContainerTokenMasterKey);
    nodeHeartbeatRequest
        .setLastKnownNMTokenMasterKey(lastKnownNMTokenMasterKey);
    nodeHeartbeatRequest.setNodeLabels(nodeLabels);
    nodeHeartbeatRequest.setRegisteringCollectors(registeringCollectors);
    return nodeHeartbeatRequest;
  }

  /**
   * 创建NodeHeartbeatRequest实例（全参数版本，增加节点属性）
   * @param nodeStatus 节点状态信息
   * @param lastKnownContainerTokenMasterKey 节点已知最新容器令牌主密钥
   * @param lastKnownNMTokenMasterKey 节点已知最新NM令牌主密钥
   * @param nodeLabels 节点标签集合
   * @param nodeAttributes 节点属性集合
   * @param registeringCollectors 本节点上正在注册的应用收集器信息映射
   * @return 构建完成的心跳请求对象
   */
  public static NodeHeartbeatRequest newInstance(NodeStatus nodeStatus,
      MasterKey lastKnownContainerTokenMasterKey,
      MasterKey lastKnownNMTokenMasterKey, Set<NodeLabel> nodeLabels,
      Set<NodeAttribute> nodeAttributes,
      Map<ApplicationId, AppCollectorData> registeringCollectors) {
    NodeHeartbeatRequest request = NodeHeartbeatRequest
        .newInstance(nodeStatus, lastKnownContainerTokenMasterKey,
            lastKnownNMTokenMasterKey, nodeLabels, registeringCollectors);
    request.setNodeAttributes(nodeAttributes);
    return request;
  }

  /** 获取节点状态信息 */
  public abstract NodeStatus getNodeStatus();
  /** 设置节点状态信息 */
  public abstract void setNodeStatus(NodeStatus status);

  /** 获取节点已知最新容器令牌主密钥 */
  public abstract MasterKey getLastKnownContainerTokenMasterKey();
  /** 设置节点已知最新容器令牌主密钥 */
  public abstract void setLastKnownContainerTokenMasterKey(MasterKey secretKey);
  
  /** 获取节点已知最新NM令牌主密钥 */
  public abstract MasterKey getLastKnownNMTokenMasterKey();
  /** 设置节点已知最新NM令牌主密钥 */
  public abstract void setLastKnownNMTokenMasterKey(MasterKey secretKey);
  
  /** 获取节点标签集合 */
  public abstract Set<NodeLabel> getNodeLabels();
  /** 设置节点标签集合 */
  public abstract void setNodeLabels(Set<NodeLabel> nodeLabels);

  /** 获取应用日志聚合上报报告列表 */
  public abstract List<LogAggregationReport>
      getLogAggregationReportsForApps();

  /** 设置应用日志聚合上报报告列表 */
  public abstract void setLogAggregationReportsForApps(
      List<LogAggregationReport> logAggregationReportsForApps);

  /** 获取本节点正在注册的应用收集器信息映射（通知RM收集器地址信息） */
  public abstract Map<ApplicationId, AppCollectorData>
      getRegisteringCollectors();

  /** 设置本节点正在注册的应用收集器信息映射 */
  public abstract void setRegisteringCollectors(Map<ApplicationId,
      AppCollectorData> appCollectorsMap);

  /** 获取节点属性集合 */
  public abstract Set<NodeAttribute> getNodeAttributes();
  /** 设置节点属性集合 */
  public abstract void setNodeAttributes(Set<NodeAttribute> nodeAttributes);

  /** 设置令牌序列号 */
  public abstract void setTokenSequenceNo(long tokenSequenceNo);
  /** 获取令牌序列号 */
  public abstract long getTokenSequenceNo();
}