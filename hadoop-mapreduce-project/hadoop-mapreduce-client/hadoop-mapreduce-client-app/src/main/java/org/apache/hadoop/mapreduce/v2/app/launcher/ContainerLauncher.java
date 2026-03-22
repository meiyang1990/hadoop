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

package org.apache.hadoop.mapreduce.v2.app.app.launcher;


import org.apache.hadoop.yarn.event.EventHandler;

/**
 * MapReduce任务容器启动器接口，定义了容器启动相关的事件处理契约
 * 负责处理ApplicationMaster中Map/Reduce任务容器的远程启动、清理和状态管理
 */
public interface ContainerLauncher 
    extends EventHandler<ContainerLauncherEvent> {

  /**
   * 容器启动器事件类型枚举，定义了所有支持的容器操作事件类型
   */
  enum EventType {
    /** 远程启动YARN容器事件 */
    CONTAINER_REMOTE_LAUNCH,
    /** 远程清理已完成/终止的YARN容器事件 */
    CONTAINER_REMOTE_CLEANUP,
    /** 容器完成通知事件，用于从已启动列表移除已完成容器，避免停止时重复清理 */
    CONTAINER_COMPLETED
  }

}