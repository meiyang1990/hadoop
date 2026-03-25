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

package org.apache.hadoop.yarn.server.nodemanager;

/**
 * NodeManager节点资源视图接口，定义了查询当前节点已分配资源和资源检查配置的方法。
 * 用于对外提供节点资源使用情况的只读视图。
 */
public interface ResourceView {

  /**
   * 获取已分配给容器的虚拟内存总量。
   * @return 虚拟内存总量，单位字节
   */
  long getVmemAllocatedForContainers();

  /**
   * 检查是否启用虚拟内存资源超限检查。
   * @return true表示启用检查，false表示不启用
   */
  boolean isVmemCheckEnabled();

  /**
   * 获取已分配给容器的物理内存总量。
   * @return 物理内存总量，单位字节
   */
  long getPmemAllocatedForContainers();

  /**
   * 检查是否启用物理内存资源超限检查。
   * @return true表示启用检查，false表示不启用
   */
  boolean isPmemCheckEnabled();

  /**
   * 获取已分配给容器的虚拟CPU核数总量。
   * @return 已分配虚拟CPU核数总量
   */
  long getVCoresAllocatedForContainers();
}