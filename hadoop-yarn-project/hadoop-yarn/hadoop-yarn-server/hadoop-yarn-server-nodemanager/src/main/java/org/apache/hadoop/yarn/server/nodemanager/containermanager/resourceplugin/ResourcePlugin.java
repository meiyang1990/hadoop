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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin;

import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperationExecutor;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.CGroupsHandler;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.ResourceHandler;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.ResourceHandlerChain;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.DockerLinuxContainerRuntime;
import org.apache.hadoop.yarn.server.nodemanager.webapp.dao.NMResourceInfo;

/**
 * ResourcePlugin 是NodeManager的扩展资源插件接口，
 * 用于方便接入新资源类型，实现新资源的发现、管理和隔离能力。
 *
 * <p>
 * 插件主要包含两大核心部分：{@link ResourcePlugin#createResourceHandler(Context,
 * CGroupsHandler, PrivilegedOperationExecutor)} 创建资源隔离处理器
 * 和 {@link ResourcePlugin#getNodeResourceHandlerInstance()} 获取资源发现更新处理器，
 * 详见各方法的javadoc说明。
 * </p>
 */
public interface ResourcePlugin {
  /**
   * 初始化插件，在NodeManager启动阶段调用。
   * @param context NodeManager上下文对象
   * @throws YarnException 初始化发生错误时抛出
   */
  void initialize(Context context) throws YarnException;

  /**
   * 创建资源隔离处理器，如果该资源类型需要特殊的资源隔离逻辑，需要返回实现了ResourceHandler的实例，
   * 该处理器会被添加到ResourceHandlerChain中。如果不需要特殊隔离，返回null即可。
   *
   * @param nmContext NodeManager上下文对象
   * @param cGroupsHandler CGroups处理器
   * @param privilegedOperationExecutor 特权操作执行器
   * @return 资源隔离处理器实例，不需要则返回null
   */
  ResourceHandler createResourceHandler(Context nmContext,
      CGroupsHandler cGroupsHandler,
      PrivilegedOperationExecutor privilegedOperationExecutor);

  /**
   * 获取节点资源更新插件，如果该资源类型需要动态发现资源量，并上报给ResourceManager，
   * 需要返回NodeResourceUpdaterPlugin实例。比如在NodeManager注册时设置资源量、
   * 或在心跳中更新资源量，都可以通过实现该接口来修改注册/心跳请求的资源字段。
   *
   * 该方法会在每次节点状态更新或节点注册时调用，请不要每次调用都创建新实例。
   *
   * @return 节点资源更新插件实例，不需要资源发现则返回null
   */
  NodeResourceUpdaterPlugin getNodeResourceHandlerInstance();

  /**
   * 清理插件资源，在NodeManager停止时调用。
   * @throws YarnException 清理发生错误时抛出
   */
  void cleanup() throws YarnException;

  /**
   * 获取Docker命令扩展插件，DockerLinuxContainerRuntime在执行docker命令
   *（如run、stop、pull等）时会调用该方法获取插件实例。
   *
   * @return Docker命令扩展插件实例，如果不需要修改docker命令则返回null
   */
  DockerCommandPlugin getDockerCommandPluginInstance();

  /**
   * 获取当前插件的资源信息，用于Web UI展示。
   *
   * @return 资源信息对象，例如GPU设备信息实例
   * @throws YarnException 获取信息发生错误时抛出
   */
  NMResourceInfo getNMResourceInfo() throws YarnException;
}