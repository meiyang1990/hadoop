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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler;

import java.util.List;
import java.util.Set;

import org.apache.hadoop.classification.InterfaceAudience.LimitedPrivate;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.QueueUserACLInfo;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;

/**
 * YARN ResourceManager 调度队列抽象接口，定义了调度队列必须实现的核心能力。
 * 所有资源调度器（容量调度、公平调度等）的队列实现都需要遵循该接口规范。
 */
@Evolving
@LimitedPrivate("yarn")
public interface Queue {
  /**
   * 获取当前队列的名称
   * @return 队列名称
   */
  String getQueueName();

  /**
   * 获取当前队列的调度 metrics 指标
   * @return 队列调度指标对象
   */
  QueueMetrics getMetrics();

  /**
   * 获取当前队列的信息，可选择是否包含子队列信息
   * @param includeChildQueues 是否包含子队列
   * @param recursive 是否递归获取所有层级子队列信息
   * @return 队列信息对象
   */
  QueueInfo getQueueInfo(boolean includeChildQueues, boolean recursive);
  
  /**
   * 获取指定用户在当前队列上的权限信息列表
   * @param user 目标用户
   * @return 用户对队列的ACL权限列表
   */
  List<QueueUserACLInfo> getQueueUserAclInfo(UserGroupInformation user);

  /**
   * 检查指定用户是否拥有当前队列的指定ACL权限
   * @param acl 待检查的权限类型
   * @param user 目标用户
   * @return true表示有权限，false表示无权限
   */
  boolean hasAccess(QueueACL acl, UserGroupInformation user);
  
  /**
   * 获取当前队列的用户管理器，用于管理队列用户资源使用情况
   * @return 用户管理器实例
   */
  public AbstractUsersManager getAbstractUsersManager();

  /**
   * 恢复RM重启后已分配容器的队列状态，用于故障恢复场景
   * @param clusterResource 集群总资源
   * @param schedulerAttempt 对应应用的调度尝试
   * @param rmContainer 需要恢复的容器对象
   */
  public void recoverContainer(Resource clusterResource,
      SchedulerApplicationAttempt schedulerAttempt, RMContainer rmContainer);
  
  /**
   * 获取当前队列允许访问的节点标签集合
   * 特殊规则：labels={*}表示可访问任何标签；labels={ }表示仅可访问无标签节点；labels={a,b,c}表示可访问任一指定标签节点
   * @return 当前队列可访问的节点标签集合
   */
  public Set<String> getAccessibleNodeLabels();
  
  /**
   * 获取当前队列默认节点标签表达式，当应用提交和资源请求都未指定标签时使用该默认值
   * @return 默认节点标签表达式
   */
  public String getDefaultNodeLabelExpression();

  /**
   * 增加队列待分配资源统计，用于记录等待分配的资源总量
   * @param nodeLabel 资源请求对应的节点标签
   * @param resourceToInc 需要增加的待分配资源量
   */
  public void incPendingResource(String nodeLabel, Resource resourceToInc);
  
  /**
   * 减少队列待分配资源统计，当请求被分配或取消时调用
   * @param nodeLabel 资源请求对应的节点标签
   * @param resourceToDec 需要减少的待分配资源量
   */
  public void decPendingResource(String nodeLabel, Resource resourceToDec);

  /**
   * 获取当前队列的默认应用优先级，当提交应用未指定优先级时使用该默认值
   *
   * @return 默认应用优先级
   */
  public Priority getDefaultApplicationPriority();

  /**
   * 增加队列预留资源统计，用于记录已预留待使用的资源总量
   *
   * @param partition 资源所在分区（节点标签）
   * @param reservedRes 需要增加的预留资源量
   */
  public void incReservedResource(String partition, Resource reservedRes);

  /**
   * 减少队列预留资源统计，当预留资源被使用或释放时调用
   *
   * @param partition 资源所在分区（节点标签）
   * @param reservedRes 需要减少的预留资源量
   */
  public void decReservedResource(String partition, Resource reservedRes);
}