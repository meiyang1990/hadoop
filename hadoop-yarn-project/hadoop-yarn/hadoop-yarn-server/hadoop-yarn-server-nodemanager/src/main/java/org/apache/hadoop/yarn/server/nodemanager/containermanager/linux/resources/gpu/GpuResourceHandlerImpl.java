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

import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.ResourcesExceptionUtil.throwIfNecessary;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperation;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperationException;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperationExecutor;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.CGroupsHandler;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.ResourceHandler;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.ResourceHandlerException;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.OCIContainerRuntime;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.gpu.GpuDevice;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.gpu.GpuDiscoverer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * GPU资源处理器实现，负责在NodeManager节点上管理GPU资源的分配、隔离和回收
 * 基于cgroups的devices控制器实现GPU设备隔离，支持容器级GPU资源调度
 */
public class GpuResourceHandlerImpl implements ResourceHandler {
  final static Logger LOG = LoggerFactory
      .getLogger(GpuResourceHandlerImpl.class);

  // 容器执行器命令行参数：需要排除的GPU设备选项
  public static final String EXCLUDED_GPUS_CLI_OPTION = "--excluded_gpus";
  // 容器执行器命令行参数：容器ID选项
  public static final String CONTAINER_ID_CLI_OPTION = "--container_id";

  private final Context nmContext;
  private final GpuResourceAllocator gpuAllocator;
  private final CGroupsHandler cGroupsHandler;
  private final PrivilegedOperationExecutor privilegedOperationExecutor;
  private final GpuDiscoverer gpuDiscoverer;

  /**
   * 构造GPU资源处理器
   * @param nmContext NodeManager上下文
   * @param cGroupsHandler cgroups处理器
   * @param privilegedOperationExecutor 特权操作执行器
   * @param gpuDiscoverer GPU设备发现器
   */
  public GpuResourceHandlerImpl(Context nmContext,
      CGroupsHandler cGroupsHandler,
      PrivilegedOperationExecutor privilegedOperationExecutor,
      GpuDiscoverer gpuDiscoverer) {
    this.nmContext = nmContext;
    this.cGroupsHandler = cGroupsHandler;
    this.privilegedOperationExecutor = privilegedOperationExecutor;
    this.gpuAllocator = new GpuResourceAllocator(nmContext);
    this.gpuDiscoverer = gpuDiscoverer;
  }

  @Override
  /**
   * 启动GPU资源处理器，完成GPU设备发现和初始化
   * @param configuration 配置
   * @return 特权操作列表
   * @throws ResourceHandlerException 初始化失败抛出异常
   */
  public List<PrivilegedOperation> bootstrap(Configuration configuration)
      throws ResourceHandlerException {
    List<GpuDevice> usableGpus;
    try {
      // 获取Yarn可使用的GPU设备列表
      usableGpus = gpuDiscoverer.getGpusUsableByYarn();
      if (usableGpus == null || usableGpus.isEmpty()) {
        String message = "GPU is enabled on the NodeManager, but couldn't find "
            + "any usable GPU devices, please double check configuration!";
        LOG.error(message);
        throwIfNecessary(new ResourceHandlerException(message),
            configuration);
      }
    } catch (YarnException e) {
      LOG.error("Exception when trying to get usable GPU device", e);
      throw new ResourceHandlerException(e);
    }

    // 将可使用GPU添加到分配器中
    for (GpuDevice gpu : usableGpus) {
      gpuAllocator.addGpu(gpu);
    }

    // 初始化devices cgroup控制器
    this.cGroupsHandler.initializeCGroupController(
        CGroupsHandler.CGroupController.DEVICES);

    return null;
  }

  @Override
  /**
   * 容器启动前预处理，分配GPU资源并设置cgroups隔离
   * @param container 待启动容器
   * @return 需要执行的特权操作列表
   * @throws ResourceHandlerException 处理失败抛出异常
   */
  public synchronized List<PrivilegedOperation> preStart(Container container)
      throws ResourceHandlerException {
    String containerIdStr = container.getContainerId().toString();

    // 为容器分配请求的GPU资源
    GpuResourceAllocator.GpuAllocation allocation = gpuAllocator.assignGpus(
        container);

    // 为容器创建设备cgroup
    cGroupsHandler.createCGroup(CGroupsHandler.CGroupController.DEVICES,
        containerIdStr);
    // 非OCI标准容器才需要YARN自行设置cgroup，OCI容器由运行时处理设备规则
    if (!OCIContainerRuntime.isOCICompliantContainerRequested(
        nmContext.getConf(),
        container.getLaunchContext().getEnvironment())) {
      try {
        // 创建GPU隔离特权操作，调用容器执行器完成配置
        PrivilegedOperation privilegedOperation = new PrivilegedOperation(
            PrivilegedOperation.OperationType.GPU,
            Arrays.asList(CONTAINER_ID_CLI_OPTION, containerIdStr));
        // 添加需要拒绝访问的GPU设备列表
        if (!allocation.getDeniedGPUs().isEmpty()) {
          List<Integer> minorNumbers = new ArrayList<>();
          for (GpuDevice deniedGpu : allocation.getDeniedGPUs()) {
            minorNumbers.add(deniedGpu.getMinorNumber());
          }
          privilegedOperation.appendArgs(Arrays.asList(EXCLUDED_GPUS_CLI_OPTION,
              StringUtils.join(",", minorNumbers)));
        }

        // 执行特权操作完成GPU隔离配置
        privilegedOperationExecutor.executePrivilegedOperation(
            privilegedOperation, true);
      } catch (PrivilegedOperationException e) {
        // 操作失败清理已创建的cgroup
        cGroupsHandler.deleteCGroup(CGroupsHandler.CGroupController.DEVICES,
            containerIdStr);
        LOG.warn("Could not update cgroup for container", e);
        throw new ResourceHandlerException(e);
      }

      // 返回将容器PID添加到cgroup的操作
      List<PrivilegedOperation> ret = new ArrayList<>();
      ret.add(new PrivilegedOperation(
          PrivilegedOperation.OperationType.ADD_PID_TO_CGROUP,
          PrivilegedOperation.CGROUP_ARG_PREFIX + cGroupsHandler
              .getPathForCGroupTasks(CGroupsHandler.CGroupController.DEVICES,
                  containerIdStr)));

      return ret;
    }
    // OCI容器不需要额外操作，返回空
    return null;
  }

  /**
   * 获取GPU资源分配器实例
   * @return GPU资源分配器
   */
  public GpuResourceAllocator getGpuAllocator() {
    return gpuAllocator;
  }

  @Override
  public List<PrivilegedOperation> reacquireContainer(ContainerId containerId)
      throws ResourceHandlerException {
    // 恢复已分配GPU资源，用于容器恢复场景
    gpuAllocator.recoverAssignedGpus(containerId);
    return null;
  }

  @Override
  public List<PrivilegedOperation> updateContainer(Container container)
      throws ResourceHandlerException {
    return null;
  }

  @Override
  /**
   * 容器完成后清理GPU资源和cgroup
   * @param containerId 已完成容器ID
   * @return 特权操作列表
   * @throws ResourceHandlerException 清理失败抛出异常
   */
  public synchronized List<PrivilegedOperation> postComplete(
      ContainerId containerId) throws ResourceHandlerException {
    // 回收分配给容器的GPU资源
    gpuAllocator.unassignGpus(containerId);
    // 删除容器的devices cgroup
    cGroupsHandler.deleteCGroup(CGroupsHandler.CGroupController.DEVICES,
        containerId.toString());
    return null;
  }

  @Override
  public List<PrivilegedOperation> teardown() throws ResourceHandlerException {
    return null;
  }

  @Override
  public String toString() {
    return GpuResourceHandlerImpl.class.getName() + "{" +
        "gpuAllocator=" + gpuAllocator +
        '}';
  }
}