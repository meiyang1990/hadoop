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

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperation;

import java.util.List;

/**
 * cgroup内存控制器资源处理实现类，使用Linux CGroups对容器内存资源进行管控。
 * 相比YARN原生Java实现的物理内存监控，CGroups管控更精准可靠，负责设置内存软硬限制，
 * 默认软限制为硬限制的90%。
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class CGroupsMemoryResourceHandlerImpl extends AbstractCGroupsMemoryResourceHandler {

  // opportunistic容器的swappiness值，设为100允许全部交换
  private static final int OPPORTUNISTIC_SWAPPINESS = 100;
  // 保证型容器的内存swappiness值，从配置读取
  private int swappiness = 0;

  CGroupsMemoryResourceHandlerImpl(CGroupsHandler cGroupsHandler) {
    super(cGroupsHandler);
  }

  @Override
  public List<PrivilegedOperation> bootstrap(Configuration conf)
      throws ResourceHandlerException {
    super.bootstrap(conf);
    // 从配置读取swappiness值，使用默认值作为兜底
    swappiness = conf
        .getInt(YarnConfiguration.NM_MEMORY_RESOURCE_CGROUPS_SWAPPINESS,
            YarnConfiguration.DEFAULT_NM_MEMORY_RESOURCE_CGROUPS_SWAPPINESS);
    // 校验配置范围必须在0-100之间
    if (swappiness < 0 || swappiness > 100) {
      throw new ResourceHandlerException(
          "Illegal value '" + swappiness + "' for "
              + YarnConfiguration.NM_MEMORY_RESOURCE_CGROUPS_SWAPPINESS
              + ". Value must be between 0 and 100.");
    }
    return null;
  }

  @VisibleForTesting
  int getSwappiness() {
    return swappiness;
  }

  @Override
  protected void updateMemoryHardLimit(String cgroupId, long containerHardLimit)
      throws ResourceHandlerException {
    // 更新cgroup内存硬限制参数
    getCGroupsHandler().updateCGroupParam(MEMORY, cgroupId,
        CGroupsHandler.CGROUP_PARAM_MEMORY_HARD_LIMIT_BYTES,
        String.valueOf(containerHardLimit) + "M");
  }

  @Override
  protected void updateOpportunisticMemoryLimits(String cgroupId) throws ResourceHandlerException {
    // 设置opportunistic容器内存软限制
    getCGroupsHandler().updateCGroupParam(MEMORY, cgroupId,
        CGroupsHandler.CGROUP_PARAM_MEMORY_SOFT_LIMIT_BYTES,
        String.valueOf(OPPORTUNISTIC_SOFT_LIMIT) + "M");
    // 设置opportunistic容器swappiness为100，允许大量交换
    getCGroupsHandler().updateCGroupParam(MEMORY, cgroupId,
        CGroupsHandler.CGROUP_PARAM_MEMORY_SWAPPINESS,
        String.valueOf(OPPORTUNISTIC_SWAPPINESS));
  }

  @Override
  protected void updateGuaranteedMemoryLimits(String cgroupId, long containerSoftLimit)
      throws ResourceHandlerException {
    // 设置保证型容器内存软限制
    getCGroupsHandler().updateCGroupParam(MEMORY, cgroupId,
        CGroupsHandler.CGROUP_PARAM_MEMORY_SOFT_LIMIT_BYTES,
        String.valueOf(containerSoftLimit) + "M");
    // 设置保证型容器swappiness为配置值
    getCGroupsHandler().updateCGroupParam(MEMORY, cgroupId,
        CGroupsHandler.CGROUP_PARAM_MEMORY_SWAPPINESS,
        String.valueOf(swappiness));
  }
}