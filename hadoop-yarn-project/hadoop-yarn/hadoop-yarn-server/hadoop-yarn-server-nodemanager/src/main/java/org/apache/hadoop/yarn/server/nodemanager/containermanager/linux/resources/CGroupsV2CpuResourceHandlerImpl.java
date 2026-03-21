// 这个文件已经全部加上中文注释
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.classification.VisibleForTesting;

/**
 * 基于Linux cgroups v2实现的YARN容器CPU资源限制处理器
 * 支持三种控制模式：
 * 1. 限制所有YARN容器整体CPU使用上限，通过设置cpu.max实现
 * 2. 非严格模式：为每个容器设置cpu权重（cpu.weight），容器可使用YARN整体限额下未被占用的空闲CPU
 * 3. 严格模式：为每个容器单独设置CPU使用上限，通过cpu.max实现，容器最多只能使用分配给自己的CPU配额
 */
@InterfaceStability.Unstable
@InterfaceAudience.Private
public class CGroupsV2CpuResourceHandlerImpl extends AbstractCGroupsCpuResourceHandler {
  // cgroups v2 CPU控制器标识
  private static final CGroupsHandler.CGroupController CPU =
      CGroupsHandler.CGroupController.CPU;

  @VisibleForTesting
  // cgroup v2默认CPU权重
  static final int CPU_DEFAULT_WEIGHT = 100; // cgroup v2 default
  // 机会容器（空闲资源分配的容器）默认CPU权重
  static final int CPU_DEFAULT_WEIGHT_OPPORTUNISTIC = 1;
  // cgroup v2允许的最大CPU权重
  static final int CPU_MAX_WEIGHT = 10000;
  // 不限制CPU的cgroup v2标识值
  static final String NO_LIMIT = "max";


  /**
   * 构造函数，传入cgroups处理器实例
   * @param cGroupsHandler cgroups处理器
   */
  CGroupsV2CpuResourceHandlerImpl(CGroupsHandler cGroupsHandler) {
    super(cGroupsHandler);
  }

  @Override
  /**
   * 更新cgroup的CPU最大限额配置
   * @param cgroupId cgroup ID
   * @param max 最大CPU限额值，max表示无限制
   * @param period 配额周期（微秒），默认100000微秒
   * @throws ResourceHandlerException 更新失败时抛出异常
   */
  protected void updateCgroupMaxCpuLimit(String cgroupId, String max, String period)
      throws ResourceHandlerException {
    // 读取当前cgroup的cpu.max配置
    String currentCpuMax = cGroupsHandler.getCGroupParam(CPU, cgroupId,
        CGroupsHandler.CGROUP_CPU_MAX);

    if (currentCpuMax == null) {
      currentCpuMax = "";
    }

    // 拆分当前配置的max和period
    String[] currentCpuMaxArray = currentCpuMax.split(" ");
    // 未传入新max时，保留当前值
    String maxToSet = max != null ? max : currentCpuMaxArray[0];
    // 将-1转换为cgroup v2的无限制标识max
    maxToSet = maxToSet.equals("-1") ? NO_LIMIT : maxToSet;
    // 未传入新period时，保留当前值
    String periodToSet = period != null ? period : currentCpuMaxArray[1];
    // 更新cgroup的cpu.max配置
    cGroupsHandler
        .updateCGroupParam(CPU, cgroupId, CGroupsHandler.CGROUP_CPU_MAX,
            maxToSet + " " + periodToSet);
  }

  @Override
  /**
   * 获取机会容器的默认CPU权重
   * @return 机会容器CPU权重
   */
  protected int getOpportunisticCpuWeight() {
    return CPU_DEFAULT_WEIGHT_OPPORTUNISTIC;
  }

  /**
   * 根据容器vcore数计算对应的CPU权重
   * @param containerVCores 容器分配的vcore数
   * @return 计算得到的CPU权重，不超过最大允许值
   */
  protected int getCpuWeightByContainerVcores(int containerVCores) {
    return Math.min(containerVCores * CPU_DEFAULT_WEIGHT, CPU_MAX_WEIGHT);
  }

  @Override
  /**
   * 更新cgroup的CPU权重配置
   * @param cgroupId cgroup ID
   * @param weight 要设置的CPU权重
   * @throws ResourceHandlerException 更新失败时抛出异常
   */
  protected void updateCgroupCpuWeight(String cgroupId, int weight) throws ResourceHandlerException {
    cGroupsHandler.updateCGroupParam(CPU, cgroupId, CGroupsHandler.CGROUP_PARAM_WEIGHT,
            String.valueOf(weight));
  }

  @Override
  /**
   * 检查根cgroup是否已配置了CPU全局限额
   * @param cgroupPath 根cgroup路径（此处传空使用默认根路径）
   * @return 如果配置了非无限制的CPU限额返回true，否则返回false
   * @throws ResourceHandlerException 读取配置失败时抛出异常
   */
  public boolean cpuLimitExists(String cgroupPath) throws ResourceHandlerException {
    // 读取根cgroup的cpu.max配置
    String globalCpuMaxLimit = cGroupsHandler.getCGroupParam(CPU, "",
        CGroupsHandler.CGROUP_CPU_MAX);
    if (globalCpuMaxLimit == null) {
      return false;
    }
    // 拆分配置获取max部分
    String[] cpuMaxLimitArray = globalCpuMaxLimit.split(" ");

    // 判断是否不是无限制配置
    return !cpuMaxLimitArray[0].equals(NO_LIMIT);
  }
}