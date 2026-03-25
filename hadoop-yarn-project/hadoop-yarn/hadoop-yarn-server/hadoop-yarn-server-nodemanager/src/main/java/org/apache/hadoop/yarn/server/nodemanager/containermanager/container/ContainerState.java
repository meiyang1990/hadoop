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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.container;

/**
 * 容器状态机使用的容器状态枚举，定义了NodeManager中容器生命周期的所有可能状态。
 * 对枚举进行增删改后，请同步更新：
 * 1. ContainerImpl::getContainerSubState() 方法
 * 2. ContainerSubState 类文档
 * 3. yarn_protos.proto 协议文档
 */
public enum ContainerState {
  NEW, LOCALIZING, LOCALIZATION_FAILED, SCHEDULED, RUNNING, RELAUNCHING,
  REINITIALIZING, REINITIALIZING_AWAITING_KILL,
  EXITED_WITH_SUCCESS, EXITED_WITH_FAILURE, KILLING,
  CONTAINER_CLEANEDUP_AFTER_KILL, CONTAINER_RESOURCES_CLEANINGUP, DONE,
  PAUSING, PAUSED, RESUMING
}