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

package org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

import javax.crypto.SecretKey;

import org.apache.hadoop.classification.InterfaceAudience.LimitedPrivate;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptReport;
import org.apache.hadoop.yarn.api.records.ApplicationResourceUsageReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.YarnApplicationAttemptState;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.security.client.ClientToAMTokenIdentifier;
import org.apache.hadoop.yarn.server.resourcemanager.blacklist.BlacklistManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;

/**
 * YARN ResourceManager中应用尝试的接口定义。
 * 一个{@link RMApp}应用可以根据{@link YarnConfiguration#RM_AM_MAX_ATTEMPTS}配置，
 * 拥有多次应用尝试（AM重启重试），具体实现参考{@link RMAppAttemptImpl}。
 */
public interface RMAppAttempt extends EventHandler<RMAppAttemptEvent> {

  /**
   * 获取本次应用尝试的唯一ID。
   * @return 本次应用尝试的{@link ApplicationAttemptId}
   */
  ApplicationAttemptId getAppAttemptId();

  /**
   * 获取本次应用尝试的内部状态。
   * @return 本次应用尝试的{@link RMAppAttemptState}状态
   */
  RMAppAttemptState getAppAttemptState();

  /**
   * 获取运行本次ApplicationMaster的节点主机名。
   * @return ApplicationMaster所在的主机名
   */
  String getHost();

  /**
   * 获取ApplicationMaster的RPC服务端口。
   * @return 客户端可连接的ApplicationMaster RPC端口
   */
  int getRpcPort();

  /**
   * 获取应用尝试状态追踪的访问URL（经过代理转发）。
   * @return 可访问的应用尝试状态追踪URL
   */
  String getTrackingUrl();

  /**
   * 获取应用尝试状态追踪的原始URL（未经过代理转发，仅供代理服务使用）。
   * @return 未经过代理转发的原始应用尝试状态追踪URL
   */
  String getOriginalTrackingUrl();

  /**
   * 获取非相对Web URL的基础前缀路径。
   * @return 非相对Web URL需要前置添加的基础URL路径
   */
  String getWebProxyBase();

  /**
   * 获取应用尝试的诊断信息。
   * @return 应用尝试的诊断信息文本
   */
  String getDiagnostics();

  /**
   * 获取应用尝试的进度（0~1）。
   * @return 本次应用尝试的进度值
   */
  float getProgress();

  /**
   * 获取ApplicationMaster设置的应用最终状态。
   * @return ApplicationMaster注销时设置的最终状态，AM未注销时返回null
   */
  FinalApplicationStatus getFinalApplicationStatus();

  /**
   * 取出最近完成的容器列表，取出后清空本地缓存的完成容器。
   * @return 刚完成的容器状态列表，取出后重置缓存
   */
  List<ContainerStatus> pullJustFinishedContainers();

  /**
   * 获取最近完成容器按节点分组的引用，不会重置完成容器缓存。
   * @return 按节点分组的最近完成容器状态引用，不重置缓存
   */
  ConcurrentMap<NodeId, List<ContainerStatus>>
      getJustFinishedContainersReference();

  /**
   * 获取最近完成的容器列表，不会重置完成容器缓存。
   * @return 最近完成容器状态列表，不重置缓存
   */
  List<ContainerStatus> getJustFinishedContainers();

  /**
   * 获取已经发送给AM的完成容器按节点分组的映射。
   * @return 已经发送给AM的按节点分组的完成容器状态映射
   */
  ConcurrentMap<NodeId, List<ContainerStatus>>
      getFinishedContainersSentToAMReference();

  /**
   * 获取运行ApplicationMaster的容器。
   * @return 运行ApplicationMaster的{@link Container}对象
   */
  Container getMasterContainer();

  /**
   * 获取本次应用尝试的提交上下文信息。
   * @return 本次应用的提交上下文
   */
  ApplicationSubmissionContext getSubmissionContext();

  /**
   * 获取本次应用尝试对应的AMRM令牌（用于AM和RM之间的认证）。
   * @return 本次应用尝试的AMRM令牌
   */
  Token<AMRMTokenIdentifier> getAMRMToken();

  /**
   * 获取本次应用尝试客户端到AM令牌的主密钥。仅用于RM状态存储，
   * 正常运行需要通过密钥管理器获取密钥，不能直接使用本地密钥。
   * @return 本次应用尝试客户端到AM令牌的主密钥
   */
  @LimitedPrivate("RMStateStore")
  SecretKey getClientTokenMasterKey();

  /**
   * 创建客户端连接到本次应用尝试的认证令牌。
   * @param clientName 请求令牌的客户端名称
   * @return 认证令牌，尝试未运行时返回null
   */
  Token<ClientToAMTokenIdentifier> createClientToken(String clientName);

  /**
   * 获取应用容器和资源使用情况报告。
   * @return 应用资源使用报告对象
   */
  ApplicationResourceUsageReport getApplicationResourceUsageReport();

  /**
   * 获取管理AM失败黑名单的黑名单管理器。
   * @return 跟踪AM失败的黑名单管理器
   */
  BlacklistManager getAMBlacklistManager();

  /**
   * 获取应用尝试的启动时间戳。
   * @return 应用尝试的启动时间戳
   */
  long getStartTime();

  /**
   * 获取本次应用尝试当前状态。
   * 
   * @return 本次应用尝试当前的{@link RMAppAttemptState}
   */
  RMAppAttemptState getState();

  /**
   * 获取本次应用尝试当前状态之前的上一个状态。
   *
   * @return 本次应用尝试上一个状态
   */
  RMAppAttemptState getPreviousState();

  /**
   * 根据当前内部状态转换为对用户公开的Yarn应用尝试状态。
   * 
   * @return 对用户公开的应用尝试状态
   */
  YarnApplicationAttemptState createApplicationAttemptState();
  
  /**
   * 根据当前信息生成应用尝试报告对象，供API返回。
   * 
   * @return 生成的应用尝试报告对象
   */
  ApplicationAttemptReport createApplicationAttemptReport();

  /**
   * 返回本次尝试失败是否应该计入最大重试次数。
   * <p>
   * 以下失败类型不应该计入重试次数：
   * <ul>
   *   <li>被调度器抢占</li>
   *   <li>节点硬件故障，如NM宕机、节点失联、NM磁盘错误</li>
   *   <li>RM重启或故障转移导致的杀死</li>
   * </ul>
   *
   * @return 是否应该计入最大重试次数
   */
  boolean shouldCountTowardsMaxAttemptRetry();
  
  /**
   * 获取本次应用尝试的度量指标。
   * @return 应用尝试度量指标
   */
  RMAppAttemptMetrics getRMAppAttemptMetrics();

  /**
   * 获取应用尝试的完成时间戳。
   * @return 应用尝试的完成时间戳
   */
  long getFinishTime();

  /**
   * 更新AM启动失败的诊断信息。
   * @param amLaunchDiagnostics AM启动诊断信息
   */
  void updateAMLaunchDiagnostics(String amLaunchDiagnostics);

  /**
   * 获取应用黑名单中的节点集合。
   * @return 被应用加入黑名单的节点地址集合
   */
  Set<String> getBlacklistedNodes();
}