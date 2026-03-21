// 这个文件已经全部加上中文注释
/**
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements. See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership. The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License. You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package org.apache.hadoop.yarn.server.nodemanager.util;

import java.io.IOException;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.nodemanager.LinuxContainerExecutor;

/**
 * Linux容器执行器(LCE)资源处理器接口，已废弃。
 * 用于在Linux容器执行器启动/退出容器时管理容器资源限制，扩展资源控制能力。
 * 本接口已标记为过时，不再推荐新代码使用。
 */
@Deprecated
public interface LCEResourcesHandler extends Configurable {

  /**
   * 初始化资源处理器，关联到Linux容器执行器实例。
   * @param lce Linux容器执行器实例
   * @throws IOException 初始化失败时抛出IO异常
   */
  void init(LinuxContainerExecutor lce) throws IOException;

  /**
   * Called by the LinuxContainerExecutor before launching the executable
   * inside the container.
   * @param containerId the id of the container being launched
   * @param containerResource the node resources the container will be using
   */
  void preExecute(ContainerId containerId, Resource containerResource)
       throws IOException;

  /**
   * Called by the LinuxContainerExecutor after the executable inside the
   * container has exited (successfully or not).
   * @param containerId the id of the container which was launched
   */
  void postExecute(ContainerId containerId);
  
  /**
   * 获取容器资源限制参数，拼接为启动命令行选项。
   * @param containerId 目标容器ID
   * @return 资源限制命令行参数字符串
   */
  String getResourcesOption(ContainerId containerId);
}