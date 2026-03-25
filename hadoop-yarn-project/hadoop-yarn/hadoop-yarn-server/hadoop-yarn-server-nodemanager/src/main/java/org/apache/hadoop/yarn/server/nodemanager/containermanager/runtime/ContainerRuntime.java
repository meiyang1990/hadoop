// 这个文件已经全部加上中文注释
/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.yarn.server.nodemanager.containermanager.runtime;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.protocol.datatransfer.IOStreamPair;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.executor.ContainerExecContext;

/**
 * 容器运行时抽象接口，定义了多种容器运行时实现的统一规范。
 * 支持原生进程树、Docker、Appc等不同底层容器运行实现，
 * 专注于提供底层操作系统级容器支持，应避免依赖高层NodeManager核心组件。
 */

@InterfaceAudience.Private
@InterfaceStability.Unstable
public interface ContainerRuntime {
  /**
   * 准备容器，完成启动前的初始化工作。
   *
   * @param ctx 容器运行时上下文，包含容器相关信息
   * @throws ContainerExecutionException 准备容器过程中发生错误时抛出
   */
  void prepareContainer(ContainerRuntimeContext ctx)
      throws ContainerExecutionException;

  /**
   * 启动容器。
   *
   * @param ctx 容器运行时上下文，包含容器相关信息
   * @throws ContainerExecutionException 启动容器过程中发生错误时抛出
   */
  void launchContainer(ContainerRuntimeContext ctx)
      throws ContainerExecutionException;

  /**
   * 重新启动容器。
   *
   * @param ctx 容器运行时上下文，包含容器相关信息
   * @throws ContainerExecutionException 重新启动容器过程中发生错误时抛出
   */
  void relaunchContainer(ContainerRuntimeContext ctx)
      throws ContainerExecutionException;

  /**
   * 向容器发送信号，支持终止请求、状态检查等操作。
   *
   * @param ctx 容器运行时上下文，包含容器相关信息
   * @throws ContainerExecutionException 发送信号过程中发生错误时抛出
   */
  void signalContainer(ContainerRuntimeContext ctx)
      throws ContainerExecutionException;

  /**
   * 清理容器资源，回收容器退出后的残留资源。
   *
   * @param ctx 容器运行时上下文，包含容器相关信息
   * @throws ContainerExecutionException 清理容器资源过程中发生错误时抛出
   */
  void reapContainer(ContainerRuntimeContext ctx)
      throws ContainerExecutionException;

  /**
   * 在容器内执行指定程序。
   *
   * @param ctx 容器执行上下文，包含执行参数
   * @return 容器执行的标准输入和标准输出流对
   * @throws ContainerExecutionException 在容器内执行程序发生错误时抛出
   */
  IOStreamPair execContainer(ContainerExecContext ctx)
      throws ContainerExecutionException;

  /**
   * 获取容器的IP地址和主机名信息。
   *
   * @param container 目标容器对象
   * @return 数组，第一个元素为IP，第二个元素为主机名
   * @throws ContainerExecutionException 获取IP和主机名过程中发生错误时抛出
   */
  String[] getIpAndHost(Container container) throws ContainerExecutionException;

  /**
   * 获取容器暴露的端口列表。
   * @param container 目标容器对象
   * @return 暴露端口列表字符串
   * @throws ContainerExecutionException 获取暴露端口过程中发生错误时抛出
   */
  String getExposedPorts(Container container)
      throws ContainerExecutionException;
}