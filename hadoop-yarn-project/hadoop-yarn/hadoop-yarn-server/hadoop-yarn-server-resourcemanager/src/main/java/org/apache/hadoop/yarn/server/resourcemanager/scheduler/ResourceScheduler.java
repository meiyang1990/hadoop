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

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience.LimitedPrivate;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.SchedulingRequest;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.Recoverable;

/**
 * YARN资源调度器核心接口，所有具体调度器实现都需要继承该接口。
 * 扩展了YarnScheduler基础接口，并增加了恢复、初始化等调度器生命周期相关方法。
 */
@LimitedPrivate("yarn")
@Evolving
public interface ResourceScheduler extends YarnScheduler, Recoverable {

  /**
   * 为资源调度器设置RM上下文对象，
   * 该方法仅需在调度器实例化后调用一次。
   * @param rmContext ResourceManager创建的上下文对象
   */
  void setRMContext(RMContext rmContext);

  /**
   * 重新初始化资源调度器，用于配置更新后重载调度器。
   * @param conf 新的配置对象
   * @param rmContext RM上下文对象
   * @throws IOException 初始化过程中发生I/O异常时抛出
   */
  void reinitialize(Configuration conf, RMContext rmContext) throws IOException;

  /**
   * 根据资源名称获取集群中匹配该资源的可用节点ID列表。
   * @param resourceName 资源名称（通常指节点标签）
   * @return 匹配该资源名称的可用节点ID列表
   */
  List<NodeId> getNodeIds(String resourceName);

  /**
   * 尝试在指定节点上为调度请求分配容器，
   * 注意：该方法忽略请求中的分配数量，仅尝试分配单个容器。
   * @param appAttempt 应用尝试上下文
   * @param schedulingRequest 调度请求
   * @param schedulerNode 目标节点
   * @return 分配成功返回true，否则返回false
   */
  boolean attemptAllocationOnNode(SchedulerApplicationAttempt appAttempt,
      SchedulingRequest schedulingRequest, SchedulerNode schedulerNode);

  /**
   * 重置调度器指标统计。
   */
  void resetSchedulerMetrics();
}