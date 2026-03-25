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

package org.apache.hadoop.yarn.server.resourcemanager.rmcontainer;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerReport;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ExecutionType;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.SchedulingRequest;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.ContainerRequest;
import org.apache.hadoop.yarn.server.scheduler.SchedulerRequestKey;


/**
 * YARN ResourceManager 端容器抽象接口，代表RM对应用容器的视角管理。
 * 实现类见{@link RMContainerImpl}，容器可处于{@link RMContainerState}定义的多种状态之一。
 * 即使没有实际运行的容器，RMContainer实例也可能存在（例如为未来容器分配预留资源时）。
 */
public interface RMContainer extends EventHandler<RMContainerEvent>,
    Comparable<RMContainer> {

  /** 获取容器ID */
  ContainerId getContainerId();

  /** 设置容器ID */
  void setContainerId(ContainerId containerId);

  /** 获取所属应用尝试ID */
  ApplicationAttemptId getApplicationAttemptId();

  /** 获取当前RM容器状态 */
  RMContainerState getState();

  /** 获取底层容器对象实例 */
  Container getContainer();

  /** 获取预留资源 */
  Resource getReservedResource();

  /** 获取预留资源所在节点ID */
  NodeId getReservedNode();
  
  /** 获取预留资源对应的调度请求键 */
  SchedulerRequestKey getReservedSchedulerKey();

  /** 获取已分配资源 */
  Resource getAllocatedResource();

  /** 获取最后确认的资源量 */
  Resource getLastConfirmedResource();

  /** 获取已分配节点ID */
  NodeId getAllocatedNode();

  /** 获取已分配对应的调度请求键 */
  SchedulerRequestKey getAllocatedSchedulerKey();

  /** 获取已分配优先级 */
  Priority getAllocatedPriority();

  /** 获取容器创建时间戳 */
  long getCreationTime();

  /** 获取容器完成时间戳 */
  long getFinishTime();

  /** 获取容器诊断信息 */
  String getDiagnosticsInfo();

  /** 获取容器日志访问URL */
  String getLogURL();

  /** 获取容器退出状态码 */
  int getContainerExitStatus();

  /** 获取容器状态 */
  ContainerState getContainerState();
  
  /** 创建容器报表，用于API返回 */
  ContainerReport createContainerReport();
  
  /** 判断是否为ApplicationMaster容器 */
  boolean isAMContainer();

  /** 获取对应的容器请求 */
  ContainerRequest getContainerRequest();

  /** 获取节点HTTP地址 */
  String getNodeHttpAddress();

  /** 获取暴露的端口信息 */
  Map<String, List<Map<String, String>>> getExposedPorts();

  /** 设置暴露的端口信息 */
  void setExposedPorts(Map<String, List<Map<String, String>>> exposed);
  
  /** 获取节点标签表达式 */
  String getNodeLabelExpression();

  /** 获取所属队列名称 */
  String getQueueName();

  /** 获取容器执行类型（比如机会型/保障型） */
  ExecutionType getExecutionType();

  /**
   * 判断容器是否由非ResourceManager分配（例如NodeManager本地调度器分配）。
   * @return 如果是远程分配返回true，否则返回false
   */
  boolean isRemotelyAllocated();

  /*
   * 预留容器返回预留资源，其他容器返回已分配资源
   */
  Resource getAllocatedOrReservedResource();

  /** 判断容器是否已完成 */
  boolean completed();

  /** 获取容器所在节点ID */
  NodeId getNodeId();

  /**
   * 返回AM指定的分配标签，来自{@link SchedulingRequest#getAllocationTags()}。
   * @return 分配标签集合，可能为null或空
   */
  Set<String> getAllocationTags();
}