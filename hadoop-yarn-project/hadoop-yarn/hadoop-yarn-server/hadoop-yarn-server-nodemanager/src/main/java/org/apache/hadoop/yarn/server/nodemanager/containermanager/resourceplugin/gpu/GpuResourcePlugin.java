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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.gpu;

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.nodemanager.ContainerExecutor;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.DefaultContainerExecutor;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperationExecutor;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.CGroupsHandler;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.ResourceHandler;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.gpu.GpuResourceAllocator;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.gpu.GpuResourceHandlerImpl;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.DockerCommandPlugin;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.NodeResourceUpdaterPlugin;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.ResourcePlugin;
import org.apache.hadoop.yarn.server.nodemanager.webapp.dao.NMResourceInfo;
import org.apache.hadoop.yarn.server.nodemanager.webapp.dao.gpu.GpuDeviceInformation;
import org.apache.hadoop.yarn.server.nodemanager.webapp.dao.gpu.NMGpuResourceInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * YARN NodeManager GPU资源插件实现，负责管理节点上GPU资源的发现、分配和调度
 * 支持容器对GPU资源的独占隔离，提供Docker集成支持和Web监控信息
 */
public class GpuResourcePlugin implements ResourcePlugin {

  private static final Logger LOG =
      LoggerFactory.getLogger(GpuResourcePlugin.class);

  // 节点GPU资源更新处理器，负责自动发现GPU并更新节点资源信息
  private final GpuNodeResourceUpdateHandler resourceDiscoverHandler;
  // GPU设备发现器，负责扫描节点上的可用GPU设备
  private final GpuDiscoverer gpuDiscoverer;
  // 允许GPU检测失败的最大连续次数
  public static final int MAX_REPEATED_ERROR_ALLOWED = 10;

  // 上次成功检测后累计的连续失败次数
  private int numOfErrorExecutionSinceLastSucceed = 0;

  // GPU资源处理器，负责容器GPU资源的分配和隔离
  private GpuResourceHandlerImpl gpuResourceHandler = null;
  // Docker容器GPU命令插件，处理Docker容器启动时的GPU参数注入
  private DockerCommandPlugin dockerCommandPlugin = null;

  /**
   * 构造GPU资源插件
   * @param resourceDiscoverHandler GPU资源发现处理器
   * @param gpuDiscoverer GPU设备发现器
   */
  public GpuResourcePlugin(GpuNodeResourceUpdateHandler resourceDiscoverHandler,
      GpuDiscoverer gpuDiscoverer) {
    this.resourceDiscoverHandler = resourceDiscoverHandler;
    this.gpuDiscoverer = gpuDiscoverer;
  }

  @Override
  public void initialize(Context context) throws YarnException {
    // 验证容器执行器配置是否符合要求
    validateExecutorConfig(context.getConf());
    // 初始化GPU发现器，传入Nvidia工具路径帮助类
    this.gpuDiscoverer.initialize(context.getConf(),
        new NvidiaBinaryHelper());
    // 创建Docker GPU命令插件实例
    this.dockerCommandPlugin =
        GpuDockerCommandPluginFactory.createGpuDockerCommandPlugin(
            context.getConf());
  }

  private void validateExecutorConfig(Configuration conf) {
    // 获取配置的容器执行器类
    Class<? extends ContainerExecutor> executorClass = conf.getClass(
        YarnConfiguration.NM_CONTAINER_EXECUTOR, DefaultContainerExecutor.class,
        ContainerExecutor.class);

    // 使用默认执行器时，GPU隔离不安全，打印警告
    if (executorClass.equals(DefaultContainerExecutor.class)) {
      LOG.warn("Using GPU plugin with disabled LinuxContainerExecutor" +
          " is considered to be unsafe.");
    }
  }

  @Override
  public ResourceHandler createResourceHandler(
      Context context, CGroupsHandler cGroupsHandler,
      PrivilegedOperationExecutor privilegedOperationExecutor) {
    // 单例模式创建GPU资源处理器
    if (gpuResourceHandler == null) {
      gpuResourceHandler = new GpuResourceHandlerImpl(context, cGroupsHandler,
          privilegedOperationExecutor, gpuDiscoverer);
    }

    return gpuResourceHandler;
  }

  @Override
  public NodeResourceUpdaterPlugin getNodeResourceHandlerInstance() {
    // 返回GPU资源更新插件实例，用于更新节点可分配GPU资源
    return resourceDiscoverHandler;
  }

  @Override
  public void cleanup() throws YarnException {
    // Do nothing.
  }

  /**
   * 获取Docker命令插件实例，用于Docker容器GPU参数处理
   * @return Docker GPU命令插件实例
   */
  public DockerCommandPlugin getDockerCommandPluginInstance() {
    return dockerCommandPlugin;
  }

  @Override
  public synchronized NMResourceInfo getNMResourceInfo() throws YarnException {
    // GPU设备信息对象
    final GpuDeviceInformation gpuDeviceInformation;

    // 如果启用了自动发现，获取GPU设备信息
    if (gpuDiscoverer.isAutoDiscoveryEnabled()) {
      //At this point the gpu plugin is already enabled
      // 检查GPU资源处理器是否已正确初始化
      checkGpuResourceHandler();

      // 检查连续错误次数，超过阈值则直接抛出异常
      checkErrorCount();
      try{
        // 执行GPU设备信息获取
        gpuDeviceInformation = gpuDiscoverer.getGpuDeviceInformation();
        // 获取成功，重置错误计数
        numOfErrorExecutionSinceLastSucceed = 0;
      } catch (YarnException e) {
        // 获取失败，错误计数递增后抛出异常
        LOG.error(e.getMessage(), e);
        numOfErrorExecutionSinceLastSucceed++;
        throw e;
      }
    } else {
      // 未启用自动发现，设备信息为空
      gpuDeviceInformation = null;
    }
    // 从资源处理器获取分配器信息
    GpuResourceAllocator gpuResourceAllocator =
        gpuResourceHandler.getGpuAllocator();
    // 获取节点所有允许分配的GPU设备
    List<GpuDevice> totalGpus = gpuResourceAllocator.getAllowedGpus();
    // 获取已分配给容器的GPU设备
    List<AssignedGpuDevice> assignedGpuDevices =
        gpuResourceAllocator.getAssignedGpus();
    // 封装为WebUI可用的GPU资源信息对象返回
    return new NMGpuResourceInfo(gpuDeviceInformation, totalGpus,
        assignedGpuDevices);
  }

  private void checkGpuResourceHandler() throws YarnException {
    // 检查GPU资源处理器是否已初始化
    if(gpuResourceHandler == null) {
      // 未初始化说明未正确配置LinuxContainerExecutor，抛出异常提示用户
      String errorMsg =
          "Linux Container Executor is not configured for the NodeManager. "
              + "To fully enable GPU feature on the node also set "
              + YarnConfiguration.NM_CONTAINER_EXECUTOR + " properly.";
      LOG.warn(errorMsg);
      throw new YarnException(errorMsg);
    }
  }

  private void checkErrorCount() throws YarnException {
    // 检查连续错误次数是否达到允许的最大值
    if (numOfErrorExecutionSinceLastSucceed == MAX_REPEATED_ERROR_ALLOWED) {
      // 达到最大失败次数，抛出异常停止后续检测
      String msg =
          "Failed to execute GPU device information detection script for "
              + MAX_REPEATED_ERROR_ALLOWED
              + " times, skip following executions.";
      LOG.error(msg);
      throw new YarnException(msg);
    }
  }

  @Override
  public String toString() {
    return GpuResourcePlugin.class.getName();
  }
}