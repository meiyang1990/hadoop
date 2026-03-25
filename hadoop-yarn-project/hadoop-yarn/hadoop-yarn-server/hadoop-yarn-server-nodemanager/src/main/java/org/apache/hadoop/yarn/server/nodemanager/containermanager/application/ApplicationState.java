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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.application;

/**
 * NodeManager上应用程序的生命周期状态枚举
 * 定义了应用在NM容器管理器中从创建到销毁的所有可能状态
 */
public enum ApplicationState {
  /** 应用刚创建，等待初始化 */
  NEW,
  /** 应用正在初始化中 */
  INITING,
  /** 应用初始化完成，正在运行 */
  RUNNING,
  /** 应用完成后等待所有容器清理完成 */
  FINISHING_CONTAINERS_WAIT,
  /** 容器清理完成，正在清理应用级资源 */
  APPLICATION_RESOURCES_CLEANINGUP,
  /** 应用所有资源清理完成，进入结束状态 */
  FINISHED 
}