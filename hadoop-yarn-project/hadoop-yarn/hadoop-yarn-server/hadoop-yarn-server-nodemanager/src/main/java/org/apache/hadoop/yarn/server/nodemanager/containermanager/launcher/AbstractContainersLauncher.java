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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.launcher;

import org.apache.hadoop.service.Service;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.server.nodemanager.ContainerExecutor;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.LocalDirsHandlerService;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.ContainerManagerImpl;


/**
 * 可插拔的容器启动器抽象接口，负责处理容器启动相关事件。
 * 是YARN NodeManager中容器启动模块的扩展点，支持不同实现方式。
 */
public interface AbstractContainersLauncher extends Service,
    EventHandler<ContainersLauncherEvent> {

  /**
   * 初始化容器启动器，注入NodeManager运行所需核心依赖。
   * @param context NodeManager上下文对象，包含集群和节点运行状态信息
   * @param dispatcher 事件分发器，用于分发容器相关事件
   * @param exec 容器执行器，负责实际启动和管理容器进程
   * @param dirsHandler 本地目录处理器，管理容器工作目录和磁盘空间
   * @param containerManager 容器管理器实例，负责容器生命周期管理
   */
  void init(Context context, Dispatcher dispatcher,
      ContainerExecutor exec, LocalDirsHandlerService dirsHandler,
      ContainerManagerImpl containerManager);

}