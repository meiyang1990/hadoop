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

/**
 * FPGA资源处理器实现类，负责NodeManager节点上FPGA设备的生命周期管理，包括分配、隔离、配置和回收。
 * 属于YARN NodeManager容器管理模块，支持为容器分配指定数量和类型的FPGA设备，并通过cgroups进行设备访问隔离。
 */
package org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.fpga;

import static org.apache.hadoop.yarn.api.records.ResourceInformation.FPGA_URI;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperation;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperationException;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperationExecutor;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.CGroupsHandler;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.ResourceHandler;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.ResourceHandlerException;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.fpga.FpgaDevice;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.fpga.AbstractFpgaVendorPlugin;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.fpga.FpgaDiscoverer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.VisibleForTesting;

@InterfaceStability.Unstable
@InterfaceAudience.Private
public class FpgaResourceHandlerImpl implements ResourceHandler {
  private static final Logger LOG =
      LoggerFactory.getLogger(FpgaResourceHandlerImpl.class);

  // 容器环境变量中请求FPGA比特流ID的键名
  private final String REQUEST_FPGA_IP_ID_KEY = "REQUESTED_FPGA_IP_ID";

  // FPGA厂商插件，处理厂商特定逻辑
  private final AbstractFpgaVendorPlugin vendorPlugin;

  // FPGA资源分配器，管理节点上FPGA设备的分配与回收
  private final FpgaResourceAllocator allocator;

  // cgroups处理器，用于实现FPGA设备访问隔离
  private final CGroupsHandler cGroupsHandler;

  // FPGA设备发现器，负责探测节点上可用的FPGA设备
  private final FpgaDiscoverer fpgaDiscoverer;

  public static final String EXCLUDED_FPGAS_CLI_OPTION = "--excluded_fpgas";
  public static final String CONTAINER_ID_CLI_OPTION = "--container_id";
  // 特权操作执行器，执行需要root权限的操作
  private PrivilegedOperationExecutor privilegedOperationExecutor;

  @VisibleForTesting
  public FpgaResourceHandlerImpl(Context nmContext,
      CGroupsHandler cGroupsHandler,
      PrivilegedOperationExecutor privilegedOperationExecutor,
      AbstractFpgaVendorPlugin plugin,
      FpgaDiscoverer fpgaDiscoverer) {
    this.allocator = new FpgaResourceAllocator(nmContext);
    this.vendorPlugin = plugin;
    this.fpgaDiscoverer = fpgaDiscoverer;
    this.cGroupsHandler = cGroupsHandler;
    this.privilegedOperationExecutor = privilegedOperationExecutor;
  }

  @VisibleForTesting
  FpgaResourceAllocator getFpgaAllocator() {
    return allocator;
  }

  /**
   * 从容器环境变量中获取用户请求的FPGA比特流ID。
   * @param container 目标容器
   * @return 请求的比特流ID，不存在则返回null
   */
  public String getRequestedIPID(Container container) {
    return container.getLaunchContext().getEnvironment().
        get(REQUEST_FPGA_IP_ID_KEY);
  }

  @Override
  /**
   * 启动FPGA资源处理器，初始化插件、发现设备并准备cgroup控制器。
   * @param configuration YARN配置
   * @return 特权操作列表，本实现返回null
   * @throws ResourceHandlerException 初始化失败时抛出异常
   */
  public List<PrivilegedOperation> bootstrap(Configuration configuration)
      throws ResourceHandlerException {
    // 厂商插件已经由FpgaDiscoverer完成初始化，此处仅检查结果
    if (!vendorPlugin.initPlugin(configuration)) {
      throw new ResourceHandlerException("FPGA plugin initialization failed");
    }
    LOG.info("FPGA Plugin bootstrap success.");
    // 从配置或厂商工具发现节点上可用的FPGA设备
    List<FpgaDevice> fpgaDeviceList = fpgaDiscoverer.discover();
    // 将发现的设备加入分配器管理
    allocator.addFpgaDevices(vendorPlugin.getFpgaType(), fpgaDeviceList);
    // 初始化devices cgroup控制器，用于后续设备访问隔离
    this.cGroupsHandler.initializeCGroupController(
        CGroupsHandler.CGroupController.DEVICES);
    return null;
  }

  @Override
  /**
   * 容器启动前为容器分配FPGA设备，配置cgroup隔离并烧录请求的比特流。
   * @param container 待启动容器
   * @return 需要执行的特权操作列表
   * @throws ResourceHandlerException 分配或配置失败时抛出异常
   */
  public List<PrivilegedOperation> preStart(Container container)
      throws ResourceHandlerException {
    // 1. 获取请求的FPGA类型和数量，选择对应厂商插件
    // 2. 调用分配器获取FPGA分配结果
    // 3. 如需要，下载比特流并烧录到分配的设备中
    List<PrivilegedOperation> ret = new ArrayList<>();
    String containerIdStr = container.getContainerId().toString();
    Resource requestedResource = container.getResource();

    // 为容器创建设备cgroup
    cGroupsHandler.createCGroup(CGroupsHandler.CGroupController.DEVICES,
      containerIdStr);

    long deviceCount = requestedResource.getResourceValue(FPGA_URI);
    LOG.info(containerIdStr + " requested " + deviceCount + " Intel FPGA(s)");
    String ipFilePath = null;
    try {

      // 即使请求0个FPGA也要执行分配逻辑，需要禁止容器访问所有FPGA设备
      final String requestedIPID = getRequestedIPID(container);
      String localizedIPIDHash = null;
      ipFilePath = vendorPlugin.retrieveIPfilePath(
          requestedIPID, container.getWorkDir(),
          container.getResourceSet().getLocalizedResources());
      // 计算下载后的比特流文件SHA256哈希，用于判断是否需要重新烧录
      if (ipFilePath != null) {
        try (FileInputStream fis = new FileInputStream(ipFilePath)) {
          localizedIPIDHash = DigestUtils.sha256Hex(fis);
        } catch (IOException e) {
          throw new ResourceHandlerException("Could not calculate SHA-256", e);
        }
      }

      // 从分配器获取FPGA分配结果，包含允许和禁止容器访问的设备
      FpgaResourceAllocator.FpgaAllocation allocation = allocator.assignFpga(
          vendorPlugin.getFpgaType(), deviceCount,
          container, localizedIPIDHash);
      LOG.info("FpgaAllocation:" + allocation);

      // 构造FPGA设备隔离特权操作
      PrivilegedOperation privilegedOperation =
          new PrivilegedOperation(PrivilegedOperation.OperationType.FPGA,
          Arrays.asList(CONTAINER_ID_CLI_OPTION, containerIdStr));
      // 将禁止访问的设备加入操作参数
      if (!allocation.getDenied().isEmpty()) {
        List<Integer> denied = new ArrayList<>();
        allocation.getDenied().forEach(device -> denied.add(device.getMinor()));
        privilegedOperation.appendArgs(Arrays.asList(EXCLUDED_FPGAS_CLI_OPTION,
            StringUtils.join(",", denied)));
      }
      // 执行特权操作，更新cgroup设备访问规则
      privilegedOperationExecutor.executePrivilegedOperation(
          privilegedOperation, true);

      // 如果容器请求了FPGA设备，进行比特流烧录
      if (deviceCount > 0) {
        /**
         * 当前仅支持所有分配的设备烧录同一份比特流。如果用户未设置环境变量，
         * YARN不进行烧录，假设应用程序自行处理设备配置。
         * YARN提前下载并烧录比特流可以为容器提供更快的启动路径，应用程序启动后即可直接使用。
         * 示例：REQUESTED_FPGA_IP_ID = "matrix_mul" 会让所有分配的设备烧录矩阵乘法比特流
         * 未来可能支持 "matrix_mul:1,gzip:2" 格式为不同设备烧录不同比特流
         *
         * */
        ipFilePath = vendorPlugin.retrieveIPfilePath(
            getRequestedIPID(container),
            container.getWorkDir(),
            container.getResourceSet().getLocalizedResources());
        if (ipFilePath == null) {
          LOG.warn("FPGA plugin failed to downloaded IP, please check the" +
              " value of environment viable: " + REQUEST_FPGA_IP_ID_KEY +
              " if you want YARN to program the device");
        } else {
          LOG.info("IP file path:" + ipFilePath);
          List<FpgaDevice> allowed = allocation.getAllowed();
          String majorMinorNumber;
          // 遍历所有分配给容器的FPGA设备
          for (int i = 0; i < allowed.size(); i++) {
            FpgaDevice device = allowed.get(i);
            majorMinorNumber = device.getMajor() + ":" + device.getMinor();
            String currentHash = allowed.get(i).getAocxHash();
            // 比特流已经烧录过且哈希一致，跳过重新烧录
            if (currentHash != null &&
                currentHash.equalsIgnoreCase(localizedIPIDHash)) {
              LOG.info("IP already in device \""
                  + allowed.get(i).getAliasDevName() + "," +
                  majorMinorNumber + "\", skip reprogramming");
              continue;
            }
            // 调用厂商插件烧录比特流到设备
            if (vendorPlugin.configureIP(ipFilePath, device)) {
              // 更新分配器中设备信息，记录当前烧录的比特流信息
              allocator.updateFpga(containerIdStr, allowed.get(i),
                  requestedIPID, localizedIPIDHash);
              //TODO: update the node constraint label
            }
          }
        }
      }
    } catch (ResourceHandlerException re) {
      // 分配失败，清理已分配资源并删除cgroup
      allocator.cleanupAssignFpgas(containerIdStr);
      cGroupsHandler.deleteCGroup(CGroupsHandler.CGroupController.DEVICES,
          containerIdStr);
      throw re;
    } catch (PrivilegedOperationException e) {
      // 特权操作执行失败，清理资源后抛出异常
      allocator.cleanupAssignFpgas(containerIdStr);
      cGroupsHandler.deleteCGroup(CGroupsHandler.CGroupController.DEVICES,
          containerIdStr);
      LOG.warn("Could not update cgroup for container", e);
      throw new ResourceHandlerException(e);
    }
    // 添加将容器PID加入cgroup的隔离操作
    ret.add(new PrivilegedOperation(
        PrivilegedOperation.OperationType.ADD_PID_TO_CGROUP,
        PrivilegedOperation.CGROUP_ARG_PREFIX
        + cGroupsHandler.getPathForCGroupTasks(
        CGroupsHandler.CGroupController.DEVICES, containerIdStr)));
    return ret;
  }

  @Override
  /**
   * 容器恢复时重新获取已分配的FPGA资源。
   * @param containerId 容器ID
   * @return 特权操作列表，本实现返回null
   * @throws ResourceHandlerException 恢复失败时抛出异常
   */
  public List<PrivilegedOperation> reacquireContainer(ContainerId containerId)
      throws ResourceHandlerException {
    allocator.recoverAssignedFpgas(containerId);
    return null;
  }

  @Override
  /**
   * 更新容器资源分配，当前FPGA资源不支持动态更新。
   * @param container 目标容器
   * @return 特权操作列表，本实现返回null
   * @throws ResourceHandlerException 永远不抛出
   */
  public List<PrivilegedOperation> updateContainer(Container container)
      throws ResourceHandlerException {
    return null;
  }

  @Override
  /**
   * 容器完成后回收FPGA资源，删除容器对应的设备cgroup。
   * @param containerId 已完成容器ID
   * @return 特权操作列表，本实现返回null
   * @throws ResourceHandlerException 回收失败时抛出异常
   */
  public List<PrivilegedOperation> postComplete(ContainerId containerId)
      throws ResourceHandlerException {
    allocator.cleanupAssignFpgas(containerId.toString());
    cGroupsHandler.deleteCGroup(CGroupsHandler.CGroupController.DEVICES,
        containerId.toString());
    return null;
  }

  @Override
  /**
   * 处理器关闭前清理资源。
   * @return 特权操作列表，本实现返回null
   * @throws ResourceHandlerException 永远不抛出
   */
  public List<PrivilegedOperation> teardown() throws ResourceHandlerException {
    return null;
  }

  @Override
  public String toString() {
    return FpgaResourceHandlerImpl.class.getName() + "{" +
        "vendorPlugin=" + vendorPlugin +
        ", allocator=" + allocator +
        '}';
  }
}