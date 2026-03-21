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

import org.apache.hadoop.classification.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.api.deviceplugin.Device;
import org.apache.hadoop.yarn.server.nodemanager.api.deviceplugin.DevicePlugin;
import org.apache.hadoop.yarn.server.nodemanager.api.deviceplugin.DeviceRuntimeSpec;
import org.apache.hadoop.yarn.server.nodemanager.api.deviceplugin.YarnRuntimeType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperation;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperationException;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperationExecutor;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.CGroupsHandler;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.ResourceHandler;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.ResourceHandlerException;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.OCIContainerRuntime;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

/**
 * 设备资源处理实现类，挂载到容器生命周期，管理节点设备资源的分配与回收
 * 在{@code bootstrap}阶段从设备插件获取可用设备列表
 * 在{@code preStart}阶段为容器分配对应设备
 * 在{@code reacquireContainer}阶段恢复设备分配状态
 * 在{@code postComplete}阶段回收容器占用的设备
 * */
public class DeviceResourceHandlerImpl implements ResourceHandler {

  static final Logger LOG = LoggerFactory.
      getLogger(DeviceResourceHandlerImpl.class);

  private final String resourceName;
  private final DevicePlugin devicePlugin;
  private final DeviceMappingManager deviceMappingManager;
  private final CGroupsHandler cGroupsHandler;
  private final PrivilegedOperationExecutor privilegedOperationExecutor;
  private final DevicePluginAdapter devicePluginAdapter;
  private final Context nmContext;
  private ShellWrapper shellWrapper;

  // 容器执行器命令行参数，用于添加设备隔离配置
  public static final String EXCLUDED_DEVICES_CLI_OPTION = "--excluded_devices";
  public static final String ALLOWED_DEVICES_CLI_OPTION = "--allowed_devices";
  public static final String CONTAINER_ID_CLI_OPTION = "--container_id";

  /**
   * 构造函数，初始化设备资源处理器
   * @param resName 资源名称
   * @param devPluginAdapter 设备插件适配器
   * @param devMappingManager 设备映射管理器
   * @param cgHandler cgroups处理器
   * @param operation 特权操作执行器
   * @param ctx NodeManager上下文
   */
  public DeviceResourceHandlerImpl(String resName,
      DevicePluginAdapter devPluginAdapter,
      DeviceMappingManager devMappingManager,
      CGroupsHandler cgHandler,
      PrivilegedOperationExecutor operation,
      Context ctx) {
    this.devicePluginAdapter = devPluginAdapter;
    this.resourceName = resName;
    this.devicePlugin = devPluginAdapter.getDevicePlugin();
    this.cGroupsHandler = cgHandler;
    this.privilegedOperationExecutor = operation;
    this.deviceMappingManager = devMappingManager;
    this.nmContext = ctx;
    this.shellWrapper = new ShellWrapper();
  }

  @VisibleForTesting
  /**
   * 测试用构造函数，支持注入自定义ShellWrapper
   * @param resName 资源名称
   * @param devPluginAdapter 设备插件适配器
   * @param devMappingManager 设备映射管理器
   * @param cgHandler cgroups处理器
   * @param operation 特权操作执行器
   * @param ctx NodeManager上下文
   * @param shell 自定义Shell包装器
   */
  public DeviceResourceHandlerImpl(String resName,
      DevicePluginAdapter devPluginAdapter,
      DeviceMappingManager devMappingManager,
      CGroupsHandler cgHandler,
      PrivilegedOperationExecutor operation,
      Context ctx, ShellWrapper shell) {
    this.devicePluginAdapter = devPluginAdapter;
    this.resourceName = resName;
    this.devicePlugin = devPluginAdapter.getDevicePlugin();
    this.cGroupsHandler = cgHandler;
    this.privilegedOperationExecutor = operation;
    this.deviceMappingManager = devMappingManager;
    this.nmContext = ctx;
    this.shellWrapper = shell;
  }

  @Override
  /**
   * 资源处理器初始化，从设备插件获取设备并初始化cgroups
   * @param configuration NodeManager配置
   * @return 需要执行的特权操作列表
   * @throws ResourceHandlerException 初始化失败抛出异常
   */
  public List<PrivilegedOperation> bootstrap(Configuration configuration)
      throws ResourceHandlerException {
    Set<Device> availableDevices = null;
    try {
      // 从设备插件获取节点上所有可用设备
      availableDevices = devicePlugin.getDevices();
    } catch (Exception e) {
      throw new ResourceHandlerException("Exception thrown from"
          + " plugin's \"getDevices\"" + e.getMessage());
    }
    /**
     * We won't fail the NM if plugin returns invalid value here.
     * */
    if (availableDevices == null) {
      LOG.error("Bootstrap " + resourceName
          + " failed. Null value got from plugin's getDevices method");
      return null;
    }
    // 将获取到的设备添加到映射管理器
    deviceMappingManager.addDeviceSet(resourceName, availableDevices);
    // 初始化devices cgroup控制器
    this.cGroupsHandler.initializeCGroupController(
        CGroupsHandler.CGroupController.DEVICES);
    return null;
  }

  @Override
  /**
   * 容器启动前准备，为容器分配设备并完成cgroups隔离配置
   * @param container 待启动容器
   * @return 需要执行的特权操作列表
   * @throws ResourceHandlerException 分配失败抛出异常
   */
  public synchronized List<PrivilegedOperation> preStart(Container container)
      throws ResourceHandlerException {
    String containerIdStr = container.getContainerId().toString();
    // 为容器分配指定数量的设备
    DeviceMappingManager.DeviceAllocation allocation =
        deviceMappingManager.assignDevices(resourceName, container);
    LOG.debug("Allocated to {}: {}", containerIdStr, allocation);
    DeviceRuntimeSpec spec;
    try {
      // 通知设备插件设备已分配，获取运行时配置
      spec = devicePlugin.onDevicesAllocated(
          allocation.getAllowed(), YarnRuntimeType.RUNTIME_DEFAULT);
    } catch (Exception e) {
      throw new ResourceHandlerException("Exception thrown from"
          + " plugin's \"onDeviceAllocated\"" + e.getMessage());
    }

    // 目前不支持非Docker容器使用自定义运行时配置
    if (spec != null) {
      LOG.warn("Runtime spec in non-Docker container is not supported yet!");
    }
    // 为当前容器创建设备cgroup
    cGroupsHandler.createCGroup(CGroupsHandler.CGroupController.DEVICES,
        containerIdStr);
    // 非OCI兼容容器，使用cgroups实现设备隔离
    if (!OCIContainerRuntime.isOCICompliantContainerRequested(
        nmContext.getConf(),
        container.getLaunchContext().getEnvironment())) {
      // 执行设备隔离配置
      tryIsolateDevices(allocation, containerIdStr);
      List<PrivilegedOperation> ret = new ArrayList<>();
      // 添加将容器PID加入设备cgroup的操作
      ret.add(new PrivilegedOperation(
          PrivilegedOperation.OperationType.ADD_PID_TO_CGROUP,
          PrivilegedOperation.CGROUP_ARG_PREFIX + cGroupsHandler
              .getPathForCGroupTasks(CGroupsHandler.CGroupController.DEVICES,
                  containerIdStr)));

      return ret;
    }
    return null;
  }

  /**
   * 使用容器执行器配置容器cgroup设备隔离规则，仅当存在设备编号时执行隔离
   * @param allocation 设备分配结果
   * @param containerIdStr 容器ID字符串
   * @throws ResourceHandlerException 配置失败抛出异常
   * */
  private void tryIsolateDevices(
      DeviceMappingManager.DeviceAllocation allocation,
      String containerIdStr) throws ResourceHandlerException {
    try {
      // 创建设备隔离特权操作，传入容器ID
      PrivilegedOperation privilegedOperation = new PrivilegedOperation(
          PrivilegedOperation.OperationType.DEVICE,
          Arrays.asList(CONTAINER_ID_CLI_OPTION, containerIdStr));
      boolean needNativeDeviceOperation = false;
      int majorNumber;
      int minorNumber;
      List<String> devNumbers = new ArrayList<>();
      // 处理禁止访问的设备
      if (!allocation.getDenied().isEmpty()) {
        DeviceType devType;
        for (Device deniedDevice : allocation.getDenied()) {
          majorNumber = deniedDevice.getMajorNumber();
          minorNumber = deniedDevice.getMinorNumber();
          // 获取设备类型
          devType = getDeviceType(deniedDevice);
          if (devType != null) {
            devNumbers.add(devType.getName() + "-" + majorNumber + ":"
                + minorNumber + "-rwm");
          }
        }
        // 如果有可禁止的设备，添加禁止参数
        if (devNumbers.size() != 0) {
          privilegedOperation.appendArgs(
              Arrays.asList(EXCLUDED_DEVICES_CLI_OPTION,
                  StringUtils.join(",", devNumbers)));
          needNativeDeviceOperation = true;
        }
      }

      // 处理允许访问的设备
      if (!allocation.getAllowed().isEmpty()) {
        devNumbers.clear();
        for (Device allowedDevice : allocation.getAllowed()) {
          majorNumber = allowedDevice.getMajorNumber();
          minorNumber = allowedDevice.getMinorNumber();
          // 只处理编号合法的设备
          if (majorNumber != -1 && minorNumber != -1) {
            devNumbers.add(majorNumber + ":" + minorNumber);
          }
        }
        // 如果有可允许的设备，添加允许参数
        if (devNumbers.size() > 0) {
          privilegedOperation.appendArgs(
              Arrays.asList(ALLOWED_DEVICES_CLI_OPTION,
                  StringUtils.join(",", devNumbers)));
          needNativeDeviceOperation = true;
        }
      }
      // 如果需要设备隔离操作，调用容器执行器执行
      if (needNativeDeviceOperation) {
        privilegedOperationExecutor.executePrivilegedOperation(
            privilegedOperation, true);
      }
    } catch (PrivilegedOperationException e) {
      // 操作失败，清理已创建的cgroup
      cGroupsHandler.deleteCGroup(CGroupsHandler.CGroupController.DEVICES,
          containerIdStr);
      LOG.warn("Could not update cgroup for container", e);
      throw new ResourceHandlerException(e);
    }
  }

  @Override
  /**
   * 恢复容器重启后已分配设备的状态
   * @param containerId 容器ID
   * @return 需要执行的特权操作列表
   * @throws ResourceHandlerException 恢复失败抛出异常
   */
  public synchronized List<PrivilegedOperation> reacquireContainer(
      ContainerId containerId) throws ResourceHandlerException {
    deviceMappingManager.recoverAssignedDevices(resourceName, containerId);
    return null;
  }

  @Override
  public List<PrivilegedOperation> updateContainer(Container container)
      throws ResourceHandlerException {
    return null;
  }

  @Override
  /**
   * 容器完成后清理，回收设备并删除对应cgroup
   * @param containerId 容器ID
   * @return 需要执行的特权操作列表
   * @throws ResourceHandlerException 清理失败抛出异常
   */
  public synchronized List<PrivilegedOperation> postComplete(
      ContainerId containerId) throws ResourceHandlerException {
    deviceMappingManager.cleanupAssignedDevices(resourceName, containerId);
    cGroupsHandler.deleteCGroup(CGroupsHandler.CGroupController.DEVICES,
        containerId.toString());
    return null;
  }

  @Override
  public List<PrivilegedOperation> teardown()
      throws ResourceHandlerException {
    return null;
  }

  @Override
  public String toString() {
    return DeviceResourceHandlerImpl.class.getName() + "{" +
        "resourceName='" + resourceName + '\'' +
        ", devicePlugin=" + devicePlugin +
        ", devicePluginAdapter=" + devicePluginAdapter +
        '}';
  }

  /**
   * 根据设备信息获取设备类型（块设备/字符设备）
   * @param device 设备对象
   * @return 设备类型，无法确定返回null
   */
  public DeviceType getDeviceType(Device device) {
    String devName = device.getDevPath();
    if (devName.isEmpty()) {
      LOG.warn("Empty device path provided, try to get device type from " +
          "major:minor device number");
      int major = device.getMajorNumber();
      int minor = device.getMinorNumber();
      if (major == -1 && minor == -1) {
        LOG.warn("Non device number provided, cannot decide the device type");
        return null;
      }
      // 设备编号判断设备类型
      return getDeviceTypeFromDeviceNumber(device.getMajorNumber(),
          device.getMinorNumber());
    }
    DeviceType deviceType;
    try {
      LOG.debug("Try to get device type from device path: {}", devName);
      // 通过stat命令获取设备文件类型
      String output = shellWrapper.getDeviceFileType(devName);
      LOG.debug("stat output:{}", output);
      // stat输出首字符c表示字符设备，b表示块设备
      deviceType = output.startsWith("c") ? DeviceType.CHAR : DeviceType.BLOCK;
    } catch (IOException e) {
      String msg =
          "Failed to get device type from stat " + devName;
      LOG.warn(msg);
      return null;
    }
    return deviceType;
  }

  /**
   * 根据设备主从编号判断设备类型，通过检查/sys/dev/block下是否存在对应目录判断
   * 如果目录存在则为块设备，否则默认为字符设备（NVIDIA GPU不存在该目录，按字符设备处理）
   * @param major 设备主编号
   * @param minor 设备从编号
   * @return 设备类型
   */
  public DeviceType getDeviceTypeFromDeviceNumber(int major, int minor) {
    if (shellWrapper.existFile("/sys/dev/block/"
        + major + ":" + minor)) {
      return DeviceType.BLOCK;
    }
    return DeviceType.CHAR;
  }

  /**
   * Linux设备类型枚举，用于配置cgroups设备规则
   * "b" 代表块设备
   * "c" 代表字符设备
   * */
  private enum DeviceType {
    BLOCK("b"),
    CHAR("c");

    private final String name;

    DeviceType(String n) {
      this.name = n;
    }

    public String getName() {
      return name;
    }
  }

}