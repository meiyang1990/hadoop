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

package org.apache.hadoop.yarn.server.resourcemanager.rmcontainer;

/**
 * RM容器状态枚举，定义YARN ResourceManager中容器生命周期的所有可能状态
 */
public enum RMContainerState {
  /** 新建状态，容器刚被创建 */
  NEW,
  /** 预留状态，资源已预留但未正式分配 */
  RESERVED,
  /** 已分配状态，资源已分配给容器 */
  ALLOCATED,
  /** 已获取状态，NodeManager已经获取到容器信息 */
  ACQUIRED,
  /** 运行中状态，容器正在NodeManager上运行 */
  RUNNING,
  /** 已完成状态，容器执行完成正常退出 */
  COMPLETED,
  /** 已过期状态，容器分配超时未使用 */
  EXPIRED,
  /** 已释放状态，容器资源已被释放 */
  RELEASED,
  /** 已杀死状态，容器被主动杀死终止 */
  KILLED
}