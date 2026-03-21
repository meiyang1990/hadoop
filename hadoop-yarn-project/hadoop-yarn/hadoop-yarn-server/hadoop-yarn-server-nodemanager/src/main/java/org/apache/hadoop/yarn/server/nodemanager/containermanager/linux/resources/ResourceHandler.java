// 这个文件已经全部加上中文注释
/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements. See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership. The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License. You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperation;

import java.util.List;

/**
 * 资源处理器接口，为CPU、内存、网络、磁盘等各类资源子系统提供资源隔离和配额管控能力。
 * 是YARN NodeManager Linux容器资源管控的扩展点，支持不同资源类型的隔离实现。
 */

@InterfaceAudience.Private
@InterfaceStability.Unstable
public interface ResourceHandler {

  /**
   * 引导初始化资源子系统，在NodeManager启动阶段执行。
   *
   * @param configuration NodeManager配置对象
   * @return 需要提权执行的特权操作列表（可能为空）
   * @throws ResourceHandlerException 初始化失败时抛出
   */
  List<PrivilegedOperation> bootstrap(Configuration configuration)
      throws ResourceHandlerException;

  /**
   * 在容器启动前准备资源环境，完成资源分配和隔离配置。
   *
   * @param container 即将启动的容器对象
   * @return 需要提权执行的特权操作列表（可能为空），例如创建自定义cgroup、将容器PID添加到cgroup任务文件等
   * @throws ResourceHandlerException 准备资源失败时抛出
   */
  List<PrivilegedOperation> preStart(Container container)
      throws ResourceHandlerException;

  /**
   * 重新接管已经启动的容器的资源状态，用于NodeManager重启等场景恢复。
   *
   * @param containerId 需要重新接管的容器ID
   * @return 需要提权执行的特权操作列表（可能为空）
   * @throws ResourceHandlerException 重新接管失败时抛出
   */
  List<PrivilegedOperation> reacquireContainer(ContainerId containerId)
      throws ResourceHandlerException;

  /**
   * 更新已启动容器的资源配置，支持容器动态资源调整。
   *
   * @param container 需要更新资源配置的容器对象
   * @return 需要提权执行的特权操作列表（可能为空）
   * @throws ResourceHandlerException 更新资源配置失败时抛出
   */
  List<PrivilegedOperation> updateContainer(Container container)
      throws ResourceHandlerException;

  /**
   * 在容器完成后执行资源清理工作，回收容器占用的资源。
   *
   * @param containerId 已完成容器的ID
   * @return 需要提权执行的特权操作列表（可能为空）
   * @throws ResourceHandlerException 资源清理失败时抛出
   */
  List<PrivilegedOperation> postComplete(ContainerId containerId) throws
      ResourceHandlerException;

  /**
   * 销毁资源子系统环境，关闭ResourceHandler。此操作需谨慎使用，可能影响正在运行的容器。
   *
   * @return 需要提权执行的特权操作列表（可能为空）
   * @throws ResourceHandlerException 销毁失败时抛出
   */
  List<PrivilegedOperation> teardown() throws ResourceHandlerException;
}