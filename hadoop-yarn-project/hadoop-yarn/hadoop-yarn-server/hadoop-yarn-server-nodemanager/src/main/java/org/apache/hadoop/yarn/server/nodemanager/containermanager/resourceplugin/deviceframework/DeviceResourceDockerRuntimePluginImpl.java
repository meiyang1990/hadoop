// 这个文件已经全部加上中文注释
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.deviceframework;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.server.nodemanager.api.deviceplugin.Device;
import org.apache.hadoop.yarn.server.nodemanager.api.deviceplugin.DevicePlugin;
import org.apache.hadoop.yarn.server.nodemanager.api.deviceplugin.DeviceRuntimeSpec;
import org.apache.hadoop.yarn.server.nodemanager.api.deviceplugin.MountDeviceSpec;
import org.apache.hadoop.yarn.server.nodemanager.api.deviceplugin.MountVolumeSpec;
import org.apache.hadoop.yarn.server.nodemanager.api.deviceplugin.VolumeSpec;
import org.apache.hadoop.yarn.server.nodemanager.api.deviceplugin.YarnRuntimeType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.docker.DockerRunCommand;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.docker.DockerVolumeCommand;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.DockerCommandPlugin;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.runtime.ContainerExecutionException;
import org.apache.hadoop.yarn.util.LRUCacheHashMap;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * 设备框架与Docker容器启动流程的桥接实现，连接DevicePlugin和Docker容器启动钩子
 * 当启动Docker容器时，DockerLinuxContainerRuntime会调用此类方法，从DevicePlugin获取所需配置信息
 * */
public class DeviceResourceDockerRuntimePluginImpl
    implements DockerCommandPlugin {

  final static Logger LOG = LoggerFactory.getLogger(
      DeviceResourceDockerRuntimePluginImpl.class);

  private String resourceName;
  private DevicePlugin devicePlugin;
  private DevicePluginAdapter devicePluginAdapter;

  private int maxCacheSize = 100;
  // LRU缓存，防止清理卷命令未调用时发生内存泄漏
  private Map<ContainerId, Set<Device>> cachedAllocation =
      Collections.synchronizedMap(new LRUCacheHashMap(maxCacheSize, true));

  private Map<ContainerId, DeviceRuntimeSpec> cachedSpec =
      Collections.synchronizedMap(new LRUCacheHashMap<>(maxCacheSize, true));

  /**
   * 构造函数，初始化Docker运行时插件
   * @param resourceName 资源名称
   * @param devicePlugin 对应的设备插件实例
   * @param devicePluginAdapter 设备插件适配器
   */
  public DeviceResourceDockerRuntimePluginImpl(String resourceName,
      DevicePlugin devicePlugin, DevicePluginAdapter devicePluginAdapter) {
    this.resourceName = resourceName;
    this.devicePlugin = devicePlugin;
    this.devicePluginAdapter = devicePluginAdapter;
  }

  @Override
  public void updateDockerRunCommand(DockerRunCommand dockerRunCommand,
      Container container) throws ContainerExecutionException {
    String containerId = container.getContainerId().toString();
    LOG.debug("Try to update docker run command for: {}", containerId);
    // 检查容器是否请求了当前类型的设备，无请求则直接返回
    if(!requestedDevice(resourceName, container)) {
      return;
    }
    // 获取设备插件生成的运行时规范
    DeviceRuntimeSpec deviceRuntimeSpec = getRuntimeSpec(container);
    if (deviceRuntimeSpec == null) {
      LOG.warn("The device plugin: "
          + devicePlugin.getClass().getCanonicalName()
          + " returns null device runtime spec value for container: "
          + containerId);
      return;
    }
    // 添加容器运行时配置
    dockerRunCommand.addRuntime(deviceRuntimeSpec.getContainerRuntime());
    LOG.debug("Handle docker container runtime type: {} for container: {}",
        deviceRuntimeSpec.getContainerRuntime(), containerId);
    // 处理设备挂载配置
    Set<MountDeviceSpec> deviceMounts = deviceRuntimeSpec.getDeviceMounts();
    LOG.debug("Handle device mounts: {} for container: {}", deviceMounts,
        containerId);
    for (MountDeviceSpec mountDeviceSpec : deviceMounts) {
      dockerRunCommand.addDevice(
          mountDeviceSpec.getDevicePathInHost(),
          mountDeviceSpec.getDevicePathInContainer());
    }
    // 处理数据卷挂载配置
    Set<MountVolumeSpec> mountVolumeSpecs = deviceRuntimeSpec.getVolumeMounts();
    LOG.debug("Handle volume mounts: {} for container: {}", mountVolumeSpecs,
        containerId);
    for (MountVolumeSpec mountVolumeSpec : mountVolumeSpecs) {
      if (mountVolumeSpec.getReadOnly()) {
        dockerRunCommand.addReadOnlyMountLocation(
            mountVolumeSpec.getHostPath(),
            mountVolumeSpec.getMountPath());
      } else {
        dockerRunCommand.addReadWriteMountLocation(
            mountVolumeSpec.getHostPath(),
            mountVolumeSpec.getMountPath());
      }
    }
    // 添加环境变量配置
    dockerRunCommand.addEnv(deviceRuntimeSpec.getEnvs());
    LOG.debug("Handle envs: {} for container: {}",
        deviceRuntimeSpec.getEnvs(), containerId);
  }

  @Override
  public DockerVolumeCommand getCreateDockerVolumeCommand(Container container)
      throws ContainerExecutionException {
    // 检查容器是否请求了当前类型的设备，无请求则直接返回
    if(!requestedDevice(resourceName, container)) {
      return null;
    }
    // 获取设备插件生成的运行时规范
    DeviceRuntimeSpec deviceRuntimeSpec = getRuntimeSpec(container);
    if (deviceRuntimeSpec == null) {
      return null;
    }
    // 遍历卷声明，查找需要创建的Docker卷
    Set<VolumeSpec> volumeClaims = deviceRuntimeSpec.getVolumeSpecs();
    for (VolumeSpec volumeSec: volumeClaims) {
      if (volumeSec.getVolumeOperation().equals(VolumeSpec.CREATE)) {
        // 构造Docker卷创建命令
        DockerVolumeCommand command = new DockerVolumeCommand(
            DockerVolumeCommand.VOLUME_CREATE_SUB_COMMAND);
        command.setDriverName(volumeSec.getVolumeDriver());
        command.setVolumeName(volumeSec.getVolumeName());
        LOG.debug("Get volume create request from plugin:{} for container: {}",
            volumeClaims, container.getContainerId());
        return command;
      }
    }
    return null;
  }

  @Override
  public DockerVolumeCommand getCleanupDockerVolumesCommand(Container container)
      throws ContainerExecutionException {
    // 检查容器是否请求了当前类型的设备，无请求则直接返回
    if(!requestedDevice(resourceName, container)) {
      return null;
    }
    // 获取已分配给容器的设备
    Set<Device> allocated = getAllocatedDevices(container);
    // 通知设备插件设备已释放，执行清理逻辑
    try {
      devicePlugin.onDevicesReleased(allocated);
    } catch (Exception e) {
      LOG.warn("Exception thrown in onDeviceReleased of "
          + devicePlugin.getClass() + "for container: "
          + container.getContainerId().toString(), e);
    }
    // 清除缓存信息
    ContainerId containerId = container.getContainerId();
    cachedAllocation.remove(containerId);
    cachedSpec.remove(containerId);
    return null;
  }

  /**
   * 检查容器是否请求了指定类型的设备
   * @param resName 资源名称
   * @param container 容器实例
   * @return 是否请求了该类型设备
   */
  protected boolean requestedDevice(String resName, Container container) {
    return DeviceMappingManager.
        getRequestedDeviceCount(resName, container.getResource()) > 0;
  }

  private Set<Device> getAllocatedDevices(Container container) {
    // 获取已分配设备集合
    Set<Device> allocated;
    ContainerId containerId = container.getContainerId();
    // 先查询缓存
    allocated = cachedAllocation.get(containerId);
    if (allocated != null) {
      return allocated;
    }
    // 缓存未命中，从设备映射管理器获取
    allocated = devicePluginAdapter
        .getDeviceMappingManager()
        .getAllocatedDevices(resourceName, containerId);
    LOG.debug("Get allocation from deviceMappingManager: {}, {} for"
        + " container: {}", allocated, resourceName, containerId);
    // 写入缓存
    cachedAllocation.put(containerId, allocated);
    return allocated;
  }

  /**
   * 获取容器的设备运行时规范，从缓存或设备插件获取
   * @param container 容器实例
   * @return 设备运行时规范
   */
  public synchronized DeviceRuntimeSpec getRuntimeSpec(Container container) {
    ContainerId containerId = container.getContainerId();
    // 先查询缓存
    DeviceRuntimeSpec deviceRuntimeSpec = cachedSpec.get(containerId);
    if (deviceRuntimeSpec == null) {
      // 缓存未命中，获取已分配设备
      Set<Device> allocated = getAllocatedDevices(container);
      if (allocated == null || allocated.size() == 0) {
        LOG.error("Cannot get allocation for container:" + containerId);
        return null;
      }
      try {
        // 调用设备插件生成Docker运行时的配置规范
        deviceRuntimeSpec = devicePlugin.onDevicesAllocated(allocated,
            YarnRuntimeType.RUNTIME_DOCKER);
      } catch (Exception e) {
        LOG.error("Exception thrown in onDeviceAllocated of "
            + devicePlugin.getClass() + " for container: " + containerId, e);
      }
      if (deviceRuntimeSpec == null) {
        LOG.error("Null DeviceRuntimeSpec value got from "
            + devicePlugin.getClass() + " for container: "
            + containerId + ", please check plugin logic");
        return null;
      }
      // 写入缓存
      cachedSpec.put(containerId, deviceRuntimeSpec);
    }
    return deviceRuntimeSpec;
  }

}