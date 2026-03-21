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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.scheduler;

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceUtilization;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.monitor.ContainersMonitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 基于容器分配量计算资源利用率的实现类，将总分配资源直接作为节点资源利用率。
 * 继承自ResourceUtilizationTracker接口，负责跟踪节点上已分配容器的资源总和，
 * 并判断是否还有足够资源可分配给新容器。
 */
public class AllocationBasedResourceUtilizationTracker implements
    ResourceUtilizationTracker {

  private static final Logger LOG =
      LoggerFactory.getLogger(AllocationBasedResourceUtilizationTracker.class);

  // MB转字节左移位数（1MB = 2^20字节）
  private static final long LEFT_SHIFT_MB_IN_BYTES = 20;
  // 字节转MB右移位数（1MB = 2^20字节）
  private static final int RIGHT_SHIFT_BYTES_IN_MB = 20;

  // 累计所有已分配容器的资源利用率
  private ResourceUtilization containersAllocation;
  // 所属容器调度器
  private ContainerScheduler scheduler;

  /**
   * 构造函数，初始化基于分配量的资源利用率跟踪器。
   * @param scheduler 所属容器调度器
   */
  AllocationBasedResourceUtilizationTracker(ContainerScheduler scheduler) {
    this.containersAllocation = ResourceUtilization.newInstance(0, 0, 0.0f);
    this.scheduler = scheduler;
  }

  @Override
  public ResourceUtilization getCurrentUtilization() {
    return this.containersAllocation;
  }

  @Override
  public void addContainerResources(Container container) {
    ContainersMonitor.increaseResourceUtilization(
        getContainersMonitor(), this.containersAllocation,
        container.getResource());
  }

  @Override
  public void subtractContainerResource(Container container) {
    ContainersMonitor.decreaseResourceUtilization(
        getContainersMonitor(), this.containersAllocation,
        container.getResource());
  }

  @Override
  public boolean hasResourcesAvailable(Container container) {
    return hasResourcesAvailable(container.getResource());
  }

  /**
   * 通过位运算将MB单位内存转换为字节单位。
   * @param memMB MB单位的内存大小
   * @return 字节单位的内存大小
   */
  private static long convertMBToBytes(final long memMB) {
    return memMB << LEFT_SHIFT_MB_IN_BYTES;
  }

  /**
   * 通过位运算将字节单位内存转换为MB单位。
   * @param bytes 字节单位的内存大小
   * @return MB单位的内存大小
   */
  private static long convertBytesToMB(final long bytes) {
    return bytes >> RIGHT_SHIFT_BYTES_IN_MB;
  }

  @Override
  public boolean hasResourcesAvailable(Resource resource) {
    // 将容器请求物理内存转换为字节
    long pMemBytes = convertMBToBytes(resource.getMemorySize());
    // 根据虚拟内存比计算容器请求虚拟内存字节
    final long vmemBytes = (long)
        (getContainersMonitor().getVmemRatio() * pMemBytes);
    // 检查各类资源是否满足分配要求
    return hasResourcesAvailable(
        pMemBytes, vmemBytes, resource.getVirtualCores());
  }

  /**
   * 依次检查物理内存、虚拟内存、CPU三种资源是否有足够剩余可分配。
   * @param pMemBytes 请求物理内存（字节）
   * @param vMemBytes 请求虚拟内存（字节）
   * @param cpuVcores 请求CPU核心数
   * @return 所有资源都足够返回true，否则返回false
   */
  private boolean hasResourcesAvailable(long pMemBytes, long vMemBytes,
      int cpuVcores) {
    // 检查物理内存
    if (LOG.isDebugEnabled()) {
      LOG.debug("pMemCheck [current={} + asked={} > allowed={}]",
          this.containersAllocation.getPhysicalMemory(),
          convertBytesToMB(pMemBytes),
          convertBytesToMB(
              getContainersMonitor().getPmemAllocatedForContainers()));
    }
    // 已分配+请求超过允许分配最大值，物理内存不足
    if (this.containersAllocation.getPhysicalMemory() +
        convertBytesToMB(pMemBytes) > convertBytesToMB(
            getContainersMonitor().getPmemAllocatedForContainers())) {
      return false;
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("before vMemCheck" +
              "[isEnabled={}, current={} + asked={} > allowed={}]",
          getContainersMonitor().isVmemCheckEnabled(),
          this.containersAllocation.getVirtualMemory(),
          convertBytesToMB(vMemBytes),
          convertBytesToMB(
              getContainersMonitor().getVmemAllocatedForContainers()));
    }
    // 检查虚拟内存
    if (getContainersMonitor().isVmemCheckEnabled() &&
        this.containersAllocation.getVirtualMemory() +
            convertBytesToMB(vMemBytes) >
            convertBytesToMB(getContainersMonitor()
                .getVmemAllocatedForContainers())) {
      // 已分配+请求超过允许分配最大值，虚拟内存不足
      return false;
    }

    LOG.debug("before cpuCheck [asked={} > allowed={}]",
        this.containersAllocation.getCPU(),
        getContainersMonitor().getVCoresAllocatedForContainers());
    // 检查CPU
    if (this.containersAllocation.getCPU() + cpuVcores >
        getContainersMonitor().getVCoresAllocatedForContainers()) {
      // 已分配+请求超过允许分配最大值，CPU不足
      return false;
    }
    // 所有资源都满足要求
    return true;
  }

  /**
   * 获取容器监控对象，从所属容器调度器中获取。
   * @return 容器监控对象
   */
  public ContainersMonitor getContainersMonitor() {
    return this.scheduler.getContainersMonitor();
  }
}