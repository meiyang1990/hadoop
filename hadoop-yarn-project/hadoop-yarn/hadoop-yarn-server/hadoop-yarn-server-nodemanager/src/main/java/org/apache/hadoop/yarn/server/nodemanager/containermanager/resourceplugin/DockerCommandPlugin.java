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

import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.docker.DockerRunCommand;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.docker.DockerVolumeCommand;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.runtime.ContainerExecutionException;

/**
 * Docker命令扩展插件接口，允许不同资源插件（如GPU、FPGA等）
 * 在不修改Docker运行时核心逻辑的前提下，自定义修改Docker运行命令和管理数据卷。
 * 实现了核心逻辑与资源扩展逻辑的解耦，支持资源特性的可插拔扩展。
 */
public interface DockerCommandPlugin {
  /**
   * 更新Docker容器启动命令，注入当前资源插件需要的参数和配置。
   * @param dockerRunCommand 待修改的Docker run命令对象
   * @param container YARN NodeManager管理的容器实例
   * @throws ContainerExecutionException 当更新过程中发生错误时抛出
   */
  void updateDockerRunCommand(DockerRunCommand dockerRunCommand,
      Container container) throws ContainerExecutionException;

  /**
   * 获取创建Docker数据卷的命令，用于为容器准备所需资源相关的存储卷。
   * @param container YARN容器实例
   * @return 用于创建数据卷的Docker命令对象
   * @throws ContainerExecutionException 当生成命令过程中发生错误时抛出
   */
  DockerVolumeCommand getCreateDockerVolumeCommand(Container container)
      throws ContainerExecutionException;

  /**
   * 获取清理Docker容器相关数据卷的清理命令，在容器销毁后清理残留数据卷。
   * @param container YARN容器实例
   * @return 用于删除数据卷的Docker命令对象
   * @throws ContainerExecutionException 当生成命令过程中发生错误时抛出
   */
  DockerVolumeCommand getCleanupDockerVolumesCommand(Container container)
      throws ContainerExecutionException;

  // 未来可在此添加对其他类型Docker命令的扩展支持
}