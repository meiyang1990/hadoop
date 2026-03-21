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

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.classification.VisibleForTesting;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * 使用Linux cgroups限制YARN容器CPU资源的实现类。
 * 支持三种控制方式：1. 限制所有YARN容器整体CPU使用；
 * 2. 通过cpu.shares设置单个容器相对CPU权重（空闲资源可抢占）；
 * 3. 通过cfs配额严格限制单个容器CPU使用上限。
 */
@InterfaceStability.Unstable
@InterfaceAudience.Private
public class CGroupsCpuResourceHandlerImpl extends AbstractCGroupsCpuResourceHandler {
  // cgroups CPU控制器
  private static final CGroupsHandler.CGroupController CPU =
      CGroupsHandler.CGroupController.CPU;

  @VisibleForTesting
  // Linux内核默认CPU权重
  static final int CPU_DEFAULT_WEIGHT = 1024; // set by kernel
  // 机会容器（可抢占容器）默认CPU权重
  static final int CPU_DEFAULT_WEIGHT_OPPORTUNISTIC = 2;


  /**
   * 构造函数，初始化CPU资源处理器
   * @param cGroupsHandler cgroups处理器实例
   */
  CGroupsCpuResourceHandlerImpl(CGroupsHandler cGroupsHandler) {
    super(cGroupsHandler);
  }

  @Override
  /**
   * 更新cgroup CPU最大配额限制
   * @param cgroupId 容器对应的cgroup ID
   * @param quota CPU配额值
   * @param period CPU周期值
   * @throws ResourceHandlerException 更新失败抛出异常
   */
  protected void updateCgroupMaxCpuLimit(String cgroupId, String quota, String period) throws ResourceHandlerException {
    if (period != null) {
      // 更新CPU周期参数
      cGroupsHandler
          .updateCGroupParam(CPU, cgroupId, CGroupsHandler.CGROUP_CPU_PERIOD_US, period);
    }
    if (quota != null) {
      // 更新CPU配额参数
      cGroupsHandler
          .updateCGroupParam(CPU, cgroupId, CGroupsHandler.CGROUP_CPU_QUOTA_US, quota);
    }
  }

  @Override
  /**
   * 获取机会容器CPU权重
   * @return 机会容器默认CPU权重
   */
  protected int getOpportunisticCpuWeight() {
    return CPU_DEFAULT_WEIGHT_OPPORTUNISTIC;
  }

  /**
   * 根据容器vcore数计算CPU权重
   * @param containerVCores 容器分配的vcore数
   * @return 计算得到的CPU权重
   */
  protected int getCpuWeightByContainerVcores(int containerVCores) {
    return containerVCores * CPU_DEFAULT_WEIGHT;
  }

  @Override
  /**
   * 更新cgroup CPU权重（cpu.shares）
   * @param cgroupId 容器对应的cgroup ID
   * @param weight CPU权重值
   * @throws ResourceHandlerException 更新失败抛出异常
   */
  protected void updateCgroupCpuWeight(String cgroupId, int weight) throws ResourceHandlerException {
    cGroupsHandler.updateCGroupParam(CPU, cgroupId, CGroupsHandler.CGROUP_CPU_SHARES,
            String.valueOf(weight));
  }

  @Override
  /**
   * 检查指定cgroup路径是否存在CPU限制配置
   * @param cgroupPath cgroup路径
   * @return 是否存在有效的CPU限制
   * @throws ResourceHandlerException 检查失败抛出异常
   */
  public boolean cpuLimitExists(String cgroupPath) throws ResourceHandlerException {
    try {
      return checkCgroupV1CPULimitExists(cgroupPath);
    } catch (IOException e) {
      throw new ResourceHandlerException("Failed to check CPU limit", e);
    }
  }

  /**
   * 检查cgroup v1中是否配置了有效的CPU配额限制
   * @param path cgroup路径
   * @return 如果存在且配额不是-1（无限制）返回true，否则返回false
   * @throws IOException 读取文件失败抛出异常
   */
  @InterfaceAudience.Private
  public static boolean checkCgroupV1CPULimitExists(String path) throws IOException {
    File quotaFile = new File(path,
        CPU.getName() + "." + CGroupsHandler.CGROUP_CPU_QUOTA_US);
    if (quotaFile.exists()) {
      // 读取配额文件内容
      String contents = FileUtils.readFileToString(quotaFile, StandardCharsets.UTF_8);
      // 配额为-1表示无限制，不等于-1表示存在有效限制
      return Integer.parseInt(contents.trim()) != -1;
    }
    // 配额文件不存在表示无限制
    return false;
  }
}