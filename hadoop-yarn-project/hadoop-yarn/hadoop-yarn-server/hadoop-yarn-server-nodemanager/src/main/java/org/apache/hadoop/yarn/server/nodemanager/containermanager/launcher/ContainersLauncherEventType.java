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

/**
 * 容器启动器事件类型枚举，定义了NodeManager容器启动器支持的所有事件类型
 * 用于在容器生命周期管理中传递不同类型的操作指令
 */
public enum ContainersLauncherEventType {
  /** 启动新容器 */
  LAUNCH_CONTAINER,
  /** 重新启动容器 */
  RELAUNCH_CONTAINER,
  /** 恢复容器（NM重启后恢复已有容器） */
  RECOVER_CONTAINER,
  /** 清理容器进程组 */
  CLEANUP_CONTAINER, // The process(grp) itself.
  /** 容器重新初始化时清理容器进程组 */
  CLEANUP_CONTAINER_FOR_REINIT, // The process(grp) itself.
  /** 向容器发送信号 */
  SIGNAL_CONTAINER,
  /** 暂停容器 */
  PAUSE_CONTAINER,
  /** 恢复暂停的容器 */
  RESUME_CONTAINER,
  /** 恢复已暂停的容器（NM重启场景） */
  RECOVER_PAUSED_CONTAINER

}