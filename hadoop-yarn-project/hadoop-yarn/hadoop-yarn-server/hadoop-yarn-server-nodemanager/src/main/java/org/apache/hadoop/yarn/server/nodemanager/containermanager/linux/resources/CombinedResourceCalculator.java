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

import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ProcfsBasedProcessTree;
import org.apache.hadoop.yarn.util.ResourceCalculatorProcessTree;

/**
 * 组合资源计算器，基于cgroups实现资源统计，同时保持与procfs虚拟内存统计的向后兼容。
 * 它会尝试多个资源计算器实现，返回第一个可用的统计结果，兼容不同cgroups版本和系统环境。
 */
public class CombinedResourceCalculator  extends ResourceCalculatorProcessTree {
  // 所有待尝试的资源计算器列表，按优先级排序
  private final List<ResourceCalculatorProcessTree> resourceCalculators;
  // 基于procfs的进程树计算器，用于获取虚拟内存和进程树转储
  private final ProcfsBasedProcessTree procfsBasedProcessTree;

  /**
   * 构造组合资源计算器，初始化各级资源计算器实现。
   * @param pid 目标进程ID
   */
  public CombinedResourceCalculator(String pid) {
    super(pid);
    this.procfsBasedProcessTree = new ProcfsBasedProcessTree(pid);
    this.resourceCalculators = Arrays.asList(
        new CGroupsV2ResourceCalculator(pid),
        new CGroupsResourceCalculator(pid),
        procfsBasedProcessTree
    );
  }

  @Override
  public void initialize() throws YarnException {
    // 初始化所有资源计算器
    for (ResourceCalculatorProcessTree calculator : resourceCalculators) {
      calculator.initialize();
    }
  }

  @Override
  public void updateProcessTree() {
    // 并行更新所有资源计算器的进程树信息
    resourceCalculators.stream().parallel()
        .forEach(ResourceCalculatorProcessTree::updateProcessTree);
  }

  @Override
  public String getProcessTreeDump() {
    // 使用procfs实现获取进程树转储信息
    return procfsBasedProcessTree.getProcessTreeDump();
  }

  @Override
  public boolean checkPidPgrpidForMatch() {
    // 使用procfs实现校验PID和PGID是否匹配
    return procfsBasedProcessTree.checkPidPgrpidForMatch();
  }

  @Override
  public long getVirtualMemorySize(int olderThanAge) {
    // 使用procfs实现获取虚拟内存大小，保持向后兼容
    return procfsBasedProcessTree.getVirtualMemorySize(olderThanAge);
  }

  @Override
  public long getRssMemorySize(int olderThanAge) {
    // 按优先级尝试不同计算器，返回第一个可用的RSS内存大小
    return resourceCalculators.stream()
        .map(calculator -> calculator.getRssMemorySize(olderThanAge))
        .filter(result -> UNAVAILABLE < result)
        .findAny().orElse((long) UNAVAILABLE);
  }

  @Override
  public long getCumulativeCpuTime() {
    // 按优先级尝试不同计算器，返回第一个可用的累计CPU时间
    return resourceCalculators.stream()
        .map(ResourceCalculatorProcessTree::getCumulativeCpuTime)
        .filter(result -> UNAVAILABLE < result)
        .findAny().orElse((long) UNAVAILABLE);
  }

  @Override
  public float getCpuUsagePercent() {
    // 按优先级尝试不同计算器，返回第一个可用的CPU使用率
    return resourceCalculators.stream()
        .map(ResourceCalculatorProcessTree::getCpuUsagePercent)
        .filter(result -> UNAVAILABLE < result)
        .findAny().orElse((float) UNAVAILABLE);
  }
}