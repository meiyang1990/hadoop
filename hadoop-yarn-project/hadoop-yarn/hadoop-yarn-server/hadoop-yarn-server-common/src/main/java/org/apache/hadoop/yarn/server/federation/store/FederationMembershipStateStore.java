// 这个文件已经全部加上中文注释
/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.yarn.server.federation.store;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.federation.store.records.GetSubClusterInfoRequest;
import org.apache.hadoop.yarn.server.federation.store.records.GetSubClusterInfoResponse;
import org.apache.hadoop.yarn.server.federation.store.records.GetSubClustersInfoRequest;
import org.apache.hadoop.yarn.server.federation.store.records.GetSubClustersInfoResponse;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterDeregisterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterDeregisterResponse;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterHeartbeatRequest;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterHeartbeatResponse;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterRegisterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterRegisterResponse;

/**
 * YARN联邦集群成员状态存储接口，负责维护所有加入联邦的子集群的状态信息，每个子集群状态由{@code SubClusterInfo}封装。
 */
@Private
@Unstable
public interface FederationMembershipStateStore {

  /**
   * 注册子集群到联邦集群，发布子集群的资源能力信息，通常在子集群ResourceManager初始化、重启或故障转移时调用。
   * 注册成功后返回全局唯一的子集群ID，该ID在重启和故障转移后保持不变。
   *
   * @param registerSubClusterRequest 注册请求，包含子集群资源能力信息，如果是重启/故障转移场景还会包含已有子集群ID
   * @return 注册成功返回空响应
   * @throws YarnException 请求无效或注册失败时抛出异常
   */
  SubClusterRegisterResponse registerSubCluster(
      SubClusterRegisterRequest registerSubClusterRequest) throws YarnException;

  /**
   * 注销指定子集群，修改其在联邦中的状态，可用于标记子集群失联、注销或退服。
   *
   * @param subClusterDeregisterRequest 注销请求，包含待注销子集群ID
   * @return 注销成功返回空响应
   * @throws YarnException 请求无效或注销失败时抛出异常
   */
  SubClusterDeregisterResponse deregisterSubCluster(
      SubClusterDeregisterRequest subClusterDeregisterRequest)
      throws YarnException;

  /**
   * 子集群ResourceManager定期发送心跳，维持子集群在线状态，同时更新当前子集群的资源能力信息。
   * 操作成功时响应为空，失败则抛出异常说明原因。
   *
   * @param subClusterHeartbeatRequest 心跳请求，包含子集群当前资源能力信息
   * @return 心跳处理成功返回空响应
   * @throws YarnException 请求无效或心跳处理失败时抛出异常
   */
  SubClusterHeartbeatResponse subClusterHeartbeat(
      SubClusterHeartbeatRequest subClusterHeartbeatRequest)
      throws YarnException;

  /**
   * 根据子集群ID查询指定子集群的成员信息，包含子集群访问地址和当前资源能力。
   *
   * @param subClusterRequest 查询请求，包含目标子集群ID
   * @return 查询到的子集群信息，如果子集群不存在则返回null
   * @throws YarnException 请求无效或查询失败时抛出异常
   */
  GetSubClusterInfoResponse getSubCluster(
      GetSubClusterInfoRequest subClusterRequest) throws YarnException;

  /**
   * 查询当前所有加入联邦的子集群成员信息，每个子集群信息包含访问地址和当前资源能力。
   *
   * @param subClustersRequest 查询所有子集群信息的请求
   * @return 子集群信息映射，以子集群ID为键
   * @throws YarnException 请求无效或查询失败时抛出异常
   */
  GetSubClustersInfoResponse getSubClusters(
      GetSubClustersInfoRequest subClustersRequest) throws YarnException;

}