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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.api.deviceplugin.Device;
import org.apache.hadoop.yarn.server.nodemanager.api.deviceplugin.DevicePlugin;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperationExecutor;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.CGroupsHandler;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.ResourceHandler;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.DockerCommandPlugin;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.NodeResourceUpdaterPlugin;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.ResourcePlugin;
import org.apache.hadoop.yarn.server.nodemanager.webapp.dao.NMDeviceResourceInfo;
import org.apache.hadoop.yarn.server.nodemanager.webapp.dao.NMResourceInfo;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * YARN NodeManager 设备框架适配器，将第三方厂商实现的设备插件适配为YARN标准ResourcePlugin接口
 * 解耦厂商设备插件与YARN核心设备框架，实现模块化扩展能力
 * 支持GPU、FPGA等专用硬件设备的资源管理与调度
 * 
 * */
public class DevicePluginAdapter implements ResourcePlugin {
  private final static Logger LOG = LoggerFactory.
      getLogger(DevicePluginAdapter.class);

  /** 管理的资源名称（如gpu、fpga等） */
  private final String resourceName;

  /** 厂商提供的设备插件实例 */
  private final DevicePlugin devicePlugin;
  /** 设备映射管理器，管理设备分配与使用状态 */
  private DeviceMappingManager deviceMappingManager;

  /** 设备资源处理器，处理容器资源分配与释放 */
  private DeviceResourceHandlerImpl deviceResourceHandler;
  /** 节点资源更新器，同步设备资源信息到NodeManager */
  private DeviceResourceUpdaterImpl deviceResourceUpdater;
  /** Docker运行时插件，处理Docker容器的设备挂载配置 */
  private DeviceResourceDockerRuntimePluginImpl deviceDockerCommandPlugin;


  @VisibleForTesting
  /** 供单元测试注入设备资源处理器实例 */
  public void setDeviceResourceHandler(
      DeviceResourceHandlerImpl deviceResourceHandler) {
    this.deviceResourceHandler = deviceResourceHandler;
  }

  /**
   * 构造设备插件适配器
   * @param name 资源名称
   * @param dp 厂商设备插件实例
   * @param dmm 设备映射管理器
   */
  public DevicePluginAdapter(String name, DevicePlugin dp,
      DeviceMappingManager dmm) {
    deviceMappingManager = dmm;
    resourceName = name;
    devicePlugin = dp;
  }

  /** 获取当前适配器的设备映射管理器 */
  public DeviceMappingManager getDeviceMappingManager() {
    return deviceMappingManager;
  }


  /** 获取厂商设备插件实例 */
  public DevicePlugin getDevicePlugin() {
    return devicePlugin;
  }

  @Override
  /** 初始化适配器，创建Docker插件和资源更新器实例 */
  public void initialize(Context context) throws YarnException {
    deviceDockerCommandPlugin = new DeviceResourceDockerRuntimePluginImpl(
        resourceName,
        devicePlugin, this);
    deviceResourceUpdater = new DeviceResourceUpdaterImpl(
        resourceName, devicePlugin);
    LOG.info(resourceName + " plugin adapter initialized");
    return;
  }

  @Override
  /** 创建设备资源处理器实例，绑定cgroups和特权操作执行器 */
  public ResourceHandler createResourceHandler(Context nmContext,
      CGroupsHandler cGroupsHandler,
      PrivilegedOperationExecutor privilegedOperationExecutor) {
    this.deviceResourceHandler = new DeviceResourceHandlerImpl(resourceName,
        this, deviceMappingManager,
        cGroupsHandler, privilegedOperationExecutor, nmContext);
    return deviceResourceHandler;
  }

  @Override
  /** 获取节点资源更新器实例 */
  public NodeResourceUpdaterPlugin getNodeResourceHandlerInstance() {
    return deviceResourceUpdater;
  }

  @Override
  /** 清理资源，当前无需要清理的资源 */
  public void cleanup() {

  }

  @Override
  /** 获取Docker命令插件实例，用于处理Docker容器设备配置 */
  public DockerCommandPlugin getDockerCommandPluginInstance() {
    return deviceDockerCommandPlugin;
  }

  @Override
  /** 构造WebUI所需的设备资源信息，包含可用设备和已分配设备 */
  public NMResourceInfo getNMResourceInfo() throws YarnException {
    // 获取当前资源类型所有可用设备列表
    List<Device> allowed = new ArrayList<>(
        deviceMappingManager.getAllAllowedDevices().get(resourceName));
    List<AssignedDevice> assigned = new ArrayList<>();
    // 获取当前资源类型所有已分配设备映射
    Map<Device, ContainerId> assignedMap =
        deviceMappingManager.getAllUsedDevices().get(resourceName);
    // 将设备-容器映射转换为UI需要的结构
    for (Map.Entry<Device, ContainerId> entry : assignedMap.entrySet()) {
      assigned.add(new AssignedDevice(entry.getValue(),
          entry.getKey()));
    }
    // 返回设备资源信息对象供WebUI展示
    return new NMDeviceResourceInfo(allowed, assigned);
  }

  /** 获取设备资源处理器实例 */
  public DeviceResourceHandlerImpl getDeviceResourceHandler() {
    return deviceResourceHandler;
  }

  @Override
  public String toString() {
    return DevicePluginAdapter.class.getName();
  }
}