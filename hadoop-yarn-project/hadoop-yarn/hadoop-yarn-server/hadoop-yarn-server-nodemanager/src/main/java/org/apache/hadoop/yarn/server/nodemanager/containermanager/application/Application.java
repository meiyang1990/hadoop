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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.application;

import java.util.Map;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;

/**
 * NodeManager上应用实例的抽象接口，定义应用在本节点的核心操作
 * 负责管理本节点上该应用所属的所有容器，处理应用生命周期相关事件
 */
public interface Application extends EventHandler<ApplicationEvent> {

  /**
   * 获取提交该应用的用户名
   * @return 提交应用的用户名称
   */
  String getUser();

  /**
   * 获取该应用在本节点上运行的所有容器集合
   * @return 容器ID到容器实例的映射表
   */
  Map<ContainerId, Container> getContainers();

  /**
   * 获取该应用的全局唯一ID
   * @return 应用ID实例
   */
  ApplicationId getAppId();

  /**
   * 获取该应用当前的生命周期状态
   * @return 应用状态枚举实例
   */
  ApplicationState getApplicationState();

  /**
   * 获取应用所属的流名称，用于流级别的监控追踪
   * @return 流名称
   */
  String getFlowName();

  /**
   * 获取应用所属的流版本，用于流级别的监控追踪
   * @return 流版本
   */
  String getFlowVersion();

  /**
   * 获取当前流运行的唯一标识ID，用于流级别的监控追踪
   * @return 流运行ID
   */
  long getFlowRunId();
}