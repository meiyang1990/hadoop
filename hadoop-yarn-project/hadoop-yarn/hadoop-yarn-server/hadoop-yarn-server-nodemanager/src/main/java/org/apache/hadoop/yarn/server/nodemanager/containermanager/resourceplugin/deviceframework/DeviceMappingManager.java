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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.deviceframework;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableMap;
import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableSet;
import org.apache.hadoop.util.Sets;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.exceptions.ResourceNotFoundException;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.api.deviceplugin.Device;
import org.apache.hadoop.yarn.server.nodemanager.api.deviceplugin.DevicePluginScheduler;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.ResourceHandlerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 设备资源分配管理器，负责管理节点上各类硬件设备的分配与回收，
 * 支持默认调度和厂商自定义调度器，维护设备使用状态，支持节点重启恢复。
 * 是YARN设备框架的核心组件，负责所有设备资源的簿记和调度。
 * */
public class DeviceMappingManager {
  static final Logger LOG = LoggerFactory.
      getLogger(DeviceMappingManager.class);

  private Context nmContext;
  private static final int WAIT_MS_PER_LOOP = 1000;

  /** 保存各设备类型对应的厂商自定义调度器 */
  private Map<String, DevicePluginScheduler> devicePluginSchedulers =
      new ConcurrentHashMap<>();

  /**
   * 保存节点上所有可用设备
   * key: 设备资源名称，如 "yarn.io/gpu"
   * value: 排序的设备集合
   * */
  private Map<String, Set<Device>> allAllowedDevices =
      new ConcurrentHashMap<>();

  /**
   * 保存已分配使用的设备
   * key: 设备资源名称
   * value: (设备 -> 分配容器ID) 有序映射
   * */
  private Map<String, Map<Device, ContainerId>> allUsedDevices =
      new ConcurrentHashMap<>();

  /**
   * 构造函数，初始化设备管理器
   * @param context NodeManager上下文对象
   */
  public DeviceMappingManager(Context context) {
    nmContext = context;
  }

  @VisibleForTesting
  public Map<String, Set<Device>> getAllAllowedDevices() {
    return allAllowedDevices;
  }

  @VisibleForTesting
  public Map<String, Map<Device, ContainerId>> getAllUsedDevices() {
    return allUsedDevices;
  }

  @VisibleForTesting
  public Map<String, DevicePluginScheduler> getDevicePluginSchedulers() {
    return devicePluginSchedulers;
  }

  @VisibleForTesting
  /** 获取指定容器分配到的所有指定类型设备 */
  public Set<Device> getAllocatedDevices(String resourceName,
      ContainerId cId) {
    Set<Device> assigned = new TreeSet<>();
    Map<Device, ContainerId> assignedMap =
        this.getAllUsedDevices().get(resourceName);
    for (Map.Entry<Device, ContainerId> entry : assignedMap.entrySet()) {
      if (entry.getValue().equals(cId)) {
        assigned.add(entry.getKey());
      }
    }
    return assigned;
  }

  /**
   * 添加新的设备类型和对应设备集合
   * @param resourceName 设备资源名称
   * @param deviceSet 设备集合
   */
  public synchronized void addDeviceSet(String resourceName,
      Set<Device> deviceSet) {
    LOG.info("Adding new resource: " + "type:"
        + resourceName + "," + deviceSet);
    allAllowedDevices.put(resourceName, new TreeSet<>(deviceSet));
    allUsedDevices.put(resourceName, new TreeMap<>());
  }

  /**
   * 为容器分配指定类型的设备，超时等待设备释放
   * @param resourceName 设备资源名称
   * @param container 待分配容器
   * @return 设备分配结果
   * @throws ResourceHandlerException 分配失败时抛出异常
   */
  public DeviceAllocation assignDevices(String resourceName,
      Container container)
      throws ResourceHandlerException {
    DeviceAllocation allocation = internalAssignDevices(resourceName,
        container);
    // 如果当前无可用设备，最多等待120秒，等待正在释放的设备
    final int timeoutMsecs = 120 * WAIT_MS_PER_LOOP;
    int timeWaiting = 0;
    // 循环等待设备释放
    while (allocation == null) {
      if (timeWaiting >= timeoutMsecs) {
        break;
      }

      try {
        LOG.info("Container : " + container.getContainerId()
            + " is waiting for free " + resourceName + " devices.");
        Thread.sleep(WAIT_MS_PER_LOOP);
        timeWaiting += WAIT_MS_PER_LOOP;
        // 重新尝试分配
        allocation = internalAssignDevices(resourceName, container);
      } catch (InterruptedException e) {
        // 中断后直接退出等待
        break;
      }
    }

    // 超时仍未分配到设备，抛出异常
    if (allocation == null) {
      String message = "Could not get valid " + resourceName
          + " device for container '" + container.getContainerId()
          + "' as some other containers might not releasing them.";
      LOG.warn(message);
      throw new ResourceHandlerException(message);
    }
    return allocation;
  }

  /**
   * 内部设备分配逻辑，检查资源可用性并执行调度
   * @param resourceName 设备资源名称
   * @param container 待分配容器
   * @return 分配结果，返回null表示需要等待正在释放的设备
   * @throws ResourceHandlerException 资源不足时抛出异常
   */
  private synchronized DeviceAllocation internalAssignDevices(
      String resourceName, Container container)
      throws ResourceHandlerException {
    Resource requestedResource = container.getResource();
    ContainerId containerId = container.getContainerId();
    int requestedDeviceCount = getRequestedDeviceCount(resourceName,
        requestedResource);
    LOG.debug("Try allocating {} {}", requestedDeviceCount, resourceName);
    // 容器请求设备数量大于0时才分配
    if (requestedDeviceCount > 0) {
      if (requestedDeviceCount > getAvailableDevices(resourceName)) {
        // 如果请求数量不超过(可用设备 + 正在释放设备)，返回null等待释放
        if (requestedDeviceCount <= getReleasingDevices(resourceName)
            + getAvailableDevices(resourceName)) {
          return null;
        }
      }

      int availableDeviceCount = getAvailableDevices(resourceName);
      if (requestedDeviceCount > availableDeviceCount) {
        throw new ResourceHandlerException("Failed to find enough "
            + resourceName
            + ", requestor=" + containerId
            + ", #Requested=" + requestedDeviceCount + ", #available="
            + availableDeviceCount);
      }

      Set<Device> assignedDevices = new TreeSet<>();
      Map<Device, ContainerId> usedDevices = allUsedDevices.get(resourceName);
      Set<Device> allowedDevices = allAllowedDevices.get(resourceName);
      DevicePluginScheduler dps = devicePluginSchedulers.get(resourceName);
      // 选择调度器并执行分配
      pickAndDoSchedule(allowedDevices, usedDevices, assignedDevices,
          container, requestedDeviceCount, resourceName, dps);

      // 分配成功后持久化分配信息到NM状态存储
      if (!assignedDevices.isEmpty()) {
        try {
          nmContext.getNMStateStore().storeAssignedResources(container,
              resourceName,
              new ArrayList<>(assignedDevices));
        } catch (IOException e) {
          // 持久化失败，清理已分配设备后抛出异常
          cleanupAssignedDevices(resourceName, containerId);
          throw new ResourceHandlerException(e);
        }
      }

      // 计算未分配设备集合，返回分配结果
      return new DeviceAllocation(resourceName, assignedDevices,
          Sets.differenceInTreeSets(allowedDevices, assignedDevices));
    }
    // 容器未请求设备，返回空分配结果
    return new DeviceAllocation(resourceName, null,
        allAllowedDevices.get(resourceName));
  }

  /**
   * 从NM状态存储恢复已分配设备信息，用于节点重启后恢复
   * @param resourceName 设备资源名称
   * @param containerId 容器ID
   * @throws ResourceHandlerException 恢复失败抛出异常
   */
  public synchronized void recoverAssignedDevices(String resourceName,
      ContainerId containerId)
      throws ResourceHandlerException {
    Container c = nmContext.getContainers().get(containerId);
    Map<Device, ContainerId> usedDevices = allUsedDevices.get(resourceName);
    Set<Device> allowedDevices = allAllowedDevices.get(resourceName);
    if (null == c) {
      throw new ResourceHandlerException(
          "This shouldn't happen, cannot find container with id="
              + containerId);
    }

    // 遍历持久化的分配信息恢复状态
    for (Serializable deviceSerializable : c.getResourceMappings()
        .getAssignedResources(resourceName)) {
      if (!(deviceSerializable instanceof Device)) {
        throw new ResourceHandlerException(
            "Trying to recover device id, however it"
                + " is not Device instance, this shouldn't happen");
      }

      Device device = (Device) deviceSerializable;

      // 检查设备是否在允许列表中
      if (!allowedDevices.contains(device)) {
        throw new ResourceHandlerException(
            "Try to recover device = " + device
                + " however it is not in allowed device list:" + StringUtils
                .join(",", allowedDevices));
      }

      // 检查设备是否已被占用
      if (usedDevices.containsKey(device)) {
        throw new ResourceHandlerException(
            "Try to recover device id = " + device
                + " however it is already assigned to container="
                + usedDevices.get(device)
                + ", please double check what happened.");
      }

      // 标记设备为已使用
      usedDevices.put(device, containerId);
    }
  }

  /**
   * 回收容器占用的所有指定类型设备
   * @param resourceName 设备资源名称
   * @param containerId 容器ID
   */
  public synchronized void cleanupAssignedDevices(String resourceName,
      ContainerId containerId) {
    Iterator<Map.Entry<Device, ContainerId>> iter =
        allUsedDevices.get(resourceName).entrySet().iterator();
    Map.Entry<Device, ContainerId> entry;
    // 遍历移除该容器分配的所有设备
    while (iter.hasNext()) {
      entry = iter.next();
      if (entry.getValue().equals(containerId)) {
        LOG.debug("Recycle devices: {}, type: {} from {}", entry.getKey(),
            resourceName, containerId);
        iter.remove();
      }
    }
  }

  /**
   * 从请求资源中获取请求的设备数量
   * @param resName 设备资源名称
   * @param requestedResource 请求资源对象
   * @return 请求设备数量，未找到该资源返回0
   */
  public static int getRequestedDeviceCount(String resName,
      Resource requestedResource) {
    try {
      return Long.valueOf(requestedResource.getResourceValue(
          resName)).intValue();
    } catch (ResourceNotFoundException e) {
      return 0;
    }
  }

  /** 获取指定类型当前可用设备数量 */
  public int getAvailableDevices(String resourceName) {
    return allAllowedDevices.get(resourceName).size()
        - allUsedDevices.get(resourceName).size();
  }

  /** 获取正在释放（容器已进入终态）的设备数量 */
  private long getReleasingDevices(String resourceName) {
    long releasingDevices = 0;
    Map<Device, ContainerId> used = allUsedDevices.get(resourceName);
    // 遍历所有已分配设备，统计终态容器占用的设备
    for (ContainerId containerId : ImmutableSet.copyOf(used.values())) {
      Container container = nmContext.getContainers().get(containerId);
      if (container != null) {
        if (container.isContainerInFinalStates()) {
          releasingDevices = releasingDevices + container.getResource()
              .getResourceInformation(resourceName).getValue();
        }
      }
    }
    return releasingDevices;
  }

  /**
   * 根据是否有自定义调度器选择分配逻辑，执行设备分配
   * @param allowed 允许分配的设备集合
   * @param used 已使用设备映射
   * @param assigned 保存分配结果的集合
   * @param c 待分配容器
   * @param count 请求设备数量
   * @param resourceName 设备资源名称
   * @param dps 厂商自定义调度器，可为null
   * @throws ResourceHandlerException 分配失败抛出异常
   * */
  private void pickAndDoSchedule(Set<Device> allowed,
      Map<Device, ContainerId> used, Set<Device> assigned,
      Container c, int count, String resourceName,
      DevicePluginScheduler dps)
      throws ResourceHandlerException {
    ContainerId containerId = c.getContainerId();
    Map<String, String> env = c.getLaunchContext().getEnvironment();
    if (null == dps) {
      LOG.debug("Customized device plugin scheduler is preferred "
          + "but not implemented, use default logic");
      // 无自定义调度器，使用默认调度逻辑
      defaultScheduleAction(allowed, used,
          assigned, containerId, count);
    } else {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Customized device plugin implemented,"
            + "use customized logic");
        LOG.debug("Try to schedule " + count
            + "(" + resourceName + ") using " + dps.getClass());
      }
      // 使用厂商自定义调度器分配，传入不可修改的可用设备集合
      Set<Device> dpsAllocated = dps.allocateDevices(
          Sets.differenceInTreeSets(allowed, used.keySet()),
          count,
          ImmutableMap.copyOf(env));
      // 检查分配数量是否符合请求
      if (dpsAllocated.size() != count) {
        throw new ResourceHandlerException(dps.getClass()
            + " should allocate " + count
            + " of " + resourceName + ", but actual: "
            + assigned.size());
      }
      // 将自定义调度结果复制到结果集合
      assigned.addAll(dpsAllocated);
      // 更新已使用设备映射
      for (Device device : assigned) {
        used.put(device, containerId);
      }
    }
  }

  /**
   * 默认调度逻辑：按设备ID顺序分配空闲设备
   * @param allowed 允许分配的设备集合
   * @param used 已使用设备映射
   * @param assigned 保存分配结果的集合
   * @param containerId 容器ID
   * @param count 请求设备数量
   */
  private void defaultScheduleAction(Set<Device> allowed,
      Map<Device, ContainerId> used, Set<Device> assigned,
      ContainerId containerId, int count) {
    LOG.debug("Using default scheduler. Allowed:" + allowed
        + ",Used:" + used + ", containerId:" + containerId);
    // 顺序遍历，遇到空闲设备直接分配，直到满足请求数量
    for (Device device : allowed) {
      if (!used.containsKey(device)) {
        used.put(device, containerId);
        assigned.add(device);
        if (assigned.size() == count) {
          return;
        }
      }
    }
  }

  /**
   * 设备分配结果封装类，保存允许注入容器和禁止注入容器的设备列表
   * 用于后续cgroups等隔离机制配置设备访问权限
   */
  static class DeviceAllocation {
    private String resourceName;

    private Set<Device> allowed = Collections.emptySet;

    private Set<Device> denied = Collections.emptySet;

    /**
     * 构造分配结果
     * @param resName 设备资源名称
     * @param a 分配给容器的允许访问设备集合
     * @param d 禁止容器访问的设备集合
     */
    DeviceAllocation(String resName, Set<Device> a,
        Set<Device> d) {
      this.resourceName = resName;
      if (a != null) {
        this.allowed = ImmutableSet.copyOf(a);
      }
      if (d != null) {