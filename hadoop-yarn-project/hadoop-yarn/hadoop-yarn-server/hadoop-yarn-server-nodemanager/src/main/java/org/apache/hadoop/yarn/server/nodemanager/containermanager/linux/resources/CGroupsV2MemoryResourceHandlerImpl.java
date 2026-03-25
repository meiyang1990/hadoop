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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources;

/**
 * cgroups v2 内存资源控制器处理器，用于在 Linux 系统上通过 cgroups v2 限制容器内存使用。
 * 相比YARN原生Java实现的内存监控，cgroups 限制更加可靠稳定，本类负责设置内存软硬限制，
 * 默认软限制为硬限制的90%。
 */
public class CGroupsV2MemoryResourceHandlerImpl extends AbstractCGroupsMemoryResourceHandler {

  /**
   * 构造函数，传入cgroups处理器实例。
   * @param cGroupsHandler cgroups处理器
   */
  CGroupsV2MemoryResourceHandlerImpl(CGroupsHandler cGroupsHandler) {
    super(cGroupsHandler);
  }

  /**
   * 更新容器内存硬限制，写入cgroup v2的memory.max接口。
   * @param cgroupId cgroup组ID
   * @param containerHardLimit 容器内存硬限制，单位MB
   * @throws ResourceHandlerException 更新失败时抛出异常
   */
  @Override
  protected void updateMemoryHardLimit(String cgroupId, long containerHardLimit)
      throws ResourceHandlerException {
    getCGroupsHandler().updateCGroupParam(MEMORY, cgroupId,
        CGroupsHandler.CGROUP_MEMORY_MAX, String.valueOf(containerHardLimit) + "M");
  }

  /**
   * 更新机会型容器内存软限制，使用默认的机会型容器软限制比例。
   * @param cgroupId cgroup组ID
   * @throws ResourceHandlerException 更新失败时抛出异常
   */
  @Override
  protected void updateOpportunisticMemoryLimits(String cgroupId) throws ResourceHandlerException {
    updateGuaranteedMemoryLimits(cgroupId, OPPORTUNISTIC_SOFT_LIMIT);
  }

  /**
   * 更新容器内存软限制，写入cgroup v2的memory.low接口。
   * memory.low 是cgroup v2 用于保护内存不被过度回收的接口，对应cgroups v1的软限制功能。
   * @param cgroupId cgroup组ID
   * @param containerSoftLimit 容器内存软限制，单位MB
   * @throws ResourceHandlerException 更新失败时抛出异常
   */
  @Override
  protected void updateGuaranteedMemoryLimits(String cgroupId, long containerSoftLimit)
      throws ResourceHandlerException {
    getCGroupsHandler().updateCGroupParam(MEMORY, cgroupId,
        CGroupsHandler.CGROUP_MEMORY_LOW, String.valueOf(containerSoftLimit) + "M");
  }
}