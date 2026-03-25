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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.scheduler;

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceUtilization;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;

/**
 * 资源利用率追踪器接口，抽象定义容器对节点资源利用率的贡献计算逻辑。
 * 被{@link ContainerScheduler}用于决策是否需要杀死空闲容器，为保证型容器腾出资源空间。
 */
public interface ResourceUtilizationTracker {

  /**
   * 获取当前节点上所有运行容器的总资源利用率。
   * @return 节点总资源利用率
   */
  ResourceUtilization getCurrentUtilization();

  /**
   * 将指定容器的资源占用计入节点资源利用率。
   * @param container 目标容器
   */
  void addContainerResources(Container container);

  /**
   * 从节点资源利用率中扣除指定容器的资源占用。
   * @param container 目标容器
   */
  void subtractContainerResource(Container container);

  /**
   * 检查节点当前是否有足够可用资源运行指定容器。
   * @param container 目标容器
   * @return 有足够可用资源返回true，否则返回false
   */
  boolean hasResourcesAvailable(Container container);

  /**
   * 检查节点当前是否有足够可用资源满足指定资源请求。
   * @param resource 请求的资源量
   * @return 有足够可用资源返回true，否则返回false
   */
  boolean hasResourcesAvailable(Resource resource);
}