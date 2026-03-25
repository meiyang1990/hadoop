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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.gpu;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableList;
import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableMap;
import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableSet;
import org.apache.hadoop.util.Sets;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.exceptions.ResourceNotFoundException;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.ResourceHandlerException;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.gpu.AssignedGpuDevice;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.gpu.GpuDevice;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static org.apache.hadoop.yarn.api.records.ResourceInformation.GPU_URI;

/**
 * GPU资源分配器，根据容器请求分配节点上的GPU设备。
 * 管理节点GPU设备的分配、回收和恢复，支持等待正在释放的GPU资源。
 */
public class GpuResourceAllocator {
  final static Logger LOG = LoggerFactory.
      getLogger(GpuResourceAllocator.class);

  private static final int WAIT_MS_PER_LOOP = 1000;

  // 当前节点允许使用的GPU设备集合
  private Set<GpuDevice> allowedGpuDevices = new TreeSet<>();
  // 已分配GPU设备映射：GPU设备 -> 分配给的容器ID
  private Map<GpuDevice, ContainerId> usedDevices = new TreeMap<>();
  // NodeManager上下文，用于获取容器信息和状态存储
  private Context nmContext;
  // 等待可用GPU资源的最大等待时间
  private final int waitPeriodForResource;

  /**
   * 构造GPU资源分配器。
   * @param ctx NodeManager上下文
   */
  public GpuResourceAllocator(Context ctx) {
    this.nmContext = ctx;
    // 如果GPU都被占用，等待正在释放的GPU，最大等待120秒
    this.waitPeriodForResource = 120 * WAIT_MS_PER_LOOP;
  }

  @VisibleForTesting
  GpuResourceAllocator(Context ctx, int waitPeriodForResource) {
    this.nmContext = ctx;
    this.waitPeriodForResource = waitPeriodForResource;
  }

  /**
   * 存储GPU分配结果，包含允许容器使用和禁止容器使用的GPU设备。
   * 禁止列表用于cgroups设备模块做黑名单处理。
   */
  static class GpuAllocation {
    private Set<GpuDevice> allowed = Collections.emptySet();
    private Set<GpuDevice> denied = Collections.emptySet();

    GpuAllocation(Set<GpuDevice> allowed, Set<GpuDevice> denied) {
      if (allowed != null) {
        this.allowed = ImmutableSet.copyOf(allowed);
      }
      if (denied != null) {
        this.denied = ImmutableSet.copyOf(denied);
      }
    }

    public Set<GpuDevice> getAllowedGPUs() {
      return allowed;
    }

    public Set<GpuDevice> getDeniedGPUs() {
      return denied;
    }
  }

  /**
   * 添加一个GPU设备到允许使用列表。
   * @param gpuDevice GPU设备对象
   */
  public synchronized void addGpu(GpuDevice gpuDevice) {
    allowedGpuDevices.add(gpuDevice);
  }

  @VisibleForTesting
  public synchronized int getAvailableGpus() {
    return allowedGpuDevices.size() - usedDevices.size();
  }

  /**
   * 从NM状态存储恢复已分配给容器的GPU设备，用于NodeManager重启后恢复。
   * @param containerId 容器ID
   * @throws ResourceHandlerException 恢复失败时抛出异常
   */
  public synchronized void recoverAssignedGpus(ContainerId containerId)
      throws ResourceHandlerException {
    Container c = nmContext.getContainers().get(containerId);
    if (c == null) {
      throw new ResourceHandlerException(
          "Cannot find container with id=" + containerId +
              ", this should not occur under normal circumstances!");
    }

    LOG.info("Starting recovery of GpuDevice for {}.", containerId);
    // 遍历状态存储中已分配给该容器的GPU资源
    for (Serializable gpuDeviceSerializable : c.getResourceMappings()
        .getAssignedResources(GPU_URI)) {
      if (!(gpuDeviceSerializable instanceof GpuDevice)) {
        throw new ResourceHandlerException(
            "Trying to recover device id, however it"
                + " is not an instance of " + GpuDevice.class.getName()
                + ", this should not occur under normal circumstances!");
      }

      GpuDevice gpuDevice = (GpuDevice) gpuDeviceSerializable;

      // 检查GPU是否在允许列表中
      if (!allowedGpuDevices.contains(gpuDevice)) {
        throw new ResourceHandlerException(
            "Try to recover device = " + gpuDevice
                + " however it is not in the allowed device list:" +
                StringUtils.join(",", allowedGpuDevices));
      }

      // 检查GPU是否已被其他容器占用
      if (usedDevices.containsKey(gpuDevice)) {
        throw new ResourceHandlerException(
            "Try to recover device id = " + gpuDevice
                + " however it is already assigned to container=" + usedDevices
                .get(gpuDevice) + ", please double check what happened.");
      }

      // 标记GPU为已分配给当前容器
      usedDevices.put(gpuDevice, containerId);
      LOG.info("ContainerId {} is assigned to GpuDevice {} on recovery.",
          containerId, gpuDevice);
    }
    LOG.info("Finished recovery of GpuDevice for {}.", containerId);
  }

  /**
   * 从资源请求中解析出需要的GPU数量。
   * @param requestedResource 容器请求的资源
   * @return 请求的GPU数量
   */
  public static int getRequestedGpus(Resource requestedResource) {
    try {
      return Long.valueOf(requestedResource.getResourceValue(
          GPU_URI)).intValue();
    } catch (ResourceNotFoundException e) {
      return 0;
    }
  }

  /**
   * 为指定容器分配GPU资源，等待GPU释放超时后抛出异常。
   * @param container 需要分配GPU的容器
   * @return GPU分配结果
   * @throws ResourceHandlerException 分配失败或等待超时抛出异常
   */
  public GpuAllocation assignGpus(Container container)
      throws ResourceHandlerException {
    GpuAllocation allocation = internalAssignGpus(container);

    // 如果当前没有足够可用GPU，等待正在释放的GPU，最多等待指定时长
    int timeWaiting = 0;
    while (allocation == null) {
      if (timeWaiting >= waitPeriodForResource) {
        break;
      }

      // 每秒重试一次，等待GPU被释放
      try {
        LOG.info("Container : " + container.getContainerId()
            + " is waiting for free GPU devices.");
        Thread.sleep(WAIT_MS_PER_LOOP);
        timeWaiting += WAIT_MS_PER_LOOP;
        allocation = internalAssignGpus(container);
      } catch (InterruptedException e) {
        // 中断后退出等待，继续处理
        Thread.currentThread().interrupt();
        LOG.warn("Interrupted while waiting for available GPU");
        break;
      }
    }

    if(allocation == null) {
      String message = "Could not get valid GPU device for container '" +
          container.getContainerId()
          + "' as some other containers might not releasing GPUs.";
      LOG.warn(message);
      throw new ResourceHandlerException(message);
    }
    return allocation;
  }

  /**
   * 内部GPU分配逻辑，尝试为容器分配请求数量的GPU。
   * 如果当前可用不足但有正在释放的GPU，返回null让外层等待。
   * @param container 需要分配GPU的容器
   * @return 分配结果，返回null表示需要等待GPU释放
   * @throws ResourceHandlerException 资源不足或存储失败抛出异常
   */
  private synchronized GpuAllocation internalAssignGpus(Container container)
      throws ResourceHandlerException {
    Resource requestedResource = container.getResource();
    ContainerId containerId = container.getContainerId();
    int numRequestedGpuDevices = getRequestedGpus(requestedResource);

    // 容器请求了GPU才进行分配
    if (numRequestedGpuDevices > 0) {
      if (LOG.isDebugEnabled()) {
        LOG.debug(String.format("Trying to assign %d GPUs to container: %s" +
            ", #AvailableGPUs=%d, #ReleasingGPUs=%d",
            numRequestedGpuDevices, containerId,
            getAvailableGpus(), getReleasingGpus()));
      }
      if (numRequestedGpuDevices > getAvailableGpus()) {
        // 如果总可用（当前可用+正在释放）足够，返回null让外层等待释放完成
        if (numRequestedGpuDevices <= getReleasingGpus() + getAvailableGpus()) {
          return null;
        }
      }

      // 总可用也不足，直接抛出异常
      if (numRequestedGpuDevices > getAvailableGpus()) {
        throw new ResourceHandlerException(
            "Failed to find enough GPUs, requestor=" + containerId +
                ", #RequestedGPUs=" + numRequestedGpuDevices +
                ", #AvailableGPUs=" + getAvailableGpus());
      }

      // 遍历分配空闲GPU
      Set<GpuDevice> assignedGpus = new TreeSet<>();
      for (GpuDevice gpu : allowedGpuDevices) {
        if (!usedDevices.containsKey(gpu)) {
          usedDevices.put(gpu, containerId);
          assignedGpus.add(gpu);
          if (assignedGpus.size() == numRequestedGpuDevices) {
            break;
          }
        }
      }

      // 分配完成后持久化到NM状态存储
      if (!assignedGpus.isEmpty()) {
        try {
          nmContext.getNMStateStore().storeAssignedResources(container, GPU_URI,
              new ArrayList<>(assignedGpus));
        } catch (IOException e) {
          // 存储失败，回滚分配
          unassignGpus(containerId);
          throw new ResourceHandlerException(e);
        }
      }

      // 允许列表为分配给容器的GPU，禁止列表为剩余所有GPU，用于cgroups黑名单
      return new GpuAllocation(assignedGpus,
          Sets.differenceInTreeSets(allowedGpuDevices, assignedGpus));
    }
    // 容器未请求GPU，所有GPU都禁止访问
    return new GpuAllocation(null, allowedGpuDevices);
  }

  /**
   * 获取当前正在被处于终态容器占用，即将释放的GPU数量。
   * @return 正在释放的GPU数量
   */
  private synchronized long getReleasingGpus() {
    long releasingGpus = 0;
    // 遍历所有已分配GPU的容器
    for (ContainerId containerId : ImmutableSet.copyOf(usedDevices.values())) {
      Container container;
      if ((container = nmContext.getContainers().get(containerId)) != null) {
        // 统计已处于终态容器占用的GPU
        if (container.isContainerInFinalStates()) {
          releasingGpus = releasingGpus + container.getResource()
              .getResourceInformation(ResourceInformation.GPU_URI).getValue();
        }
      }
    }
    return releasingGpus;
  }

  /**
   * 回收容器分配的所有GPU设备。
   * @param containerId 容器ID
   */
  public synchronized void unassignGpus(ContainerId containerId) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Trying to unassign GPU device from container " + containerId);
    }
    // 移除该容器分配的所有GPU
    usedDevices.entrySet().removeIf(entry ->
        entry.getValue().equals(containerId));
  }

  @VisibleForTesting
  public synchronized Map<GpuDevice, ContainerId> getDeviceAllocationMapping() {
    return ImmutableMap.copyOf(usedDevices);
  }

  /**
   * 获取当前节点允许使用的所有GPU列表。
   * @return 允许使用的GPU列表
   */
  public synchronized List<GpuDevice> getAllowedGpus() {
    return ImmutableList.copyOf(allowedGpuDevices);
  }

  /**
   * 获取当前所有已分配的GPU设备信息。
   * @return 已分配GPU设备列表，包含分配给的容器信息
   */
  public synchronized List<AssignedGpuDevice> getAssignedGpus() {
    return usedDevices.entrySet().stream()
        .map(e -> {
          final GpuDevice gpu = e.getKey();
          ContainerId containerId = e.getValue();
          return new AssignedGpuDevice(gpu.getIndex(), gpu.getMinorNumber(),
              containerId);
        }).collect(Collectors.toList());
  }

  @Override
  public String toString() {
    return GpuResourceAllocator.class.getName();
  }
}