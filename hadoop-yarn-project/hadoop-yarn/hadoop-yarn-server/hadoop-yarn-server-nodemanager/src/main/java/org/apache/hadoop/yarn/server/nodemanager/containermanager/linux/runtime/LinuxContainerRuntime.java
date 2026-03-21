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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.runtime.ContainerExecutionException;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.runtime.ContainerRuntime;

import java.io.IOException;
import java.util.Map;

/**
 * Linux平台容器运行时的公共接口，所有Linux平台特定的容器运行时实现都必须继承该接口。
 * 定义了Linux容器运行时需要实现的核心能力，为不同容器运行时实现提供统一抽象。
 */

@InterfaceAudience.Private
@InterfaceStability.Unstable
public interface LinuxContainerRuntime extends ContainerRuntime {
  /**
   * 初始化容器运行时，加载配置并完成准备工作。
   *
   * @param conf Hadoop配置对象
   * @param nmContext NodeManager上下文对象，包含节点运行时信息
   * @throws ContainerExecutionException 初始化失败时抛出异常
   */
  void initialize(Configuration conf, Context nmContext) throws ContainerExecutionException;

  /**
   * 根据环境变量判断当前容器是否请求使用本运行时。
   *
   * @param env 容器启动的环境变量
   * @return 如果请求本运行原则返回true，否则返回false
   */
  boolean isRuntimeRequested(Map<String, String> env);

  /**
   * 启动容器运行时服务，默认空实现。
   */
  default void start() {}

  /**
   * 停止容器运行时服务，默认空实现。
   */
  default void stop() {}

  /**
   * 获取容器需要的本地资源，默认直接从容器启动上下文获取。
   * @param container 目标容器对象
   * @return 容器需要的本地资源映射
   * @throws IOException 获取资源失败时抛出异常
   */
  default Map<String, LocalResource> getLocalResources(Container container)
      throws IOException {
    return container.getLaunchContext().getLocalResources();
  }
}