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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.gpu;

import org.apache.hadoop.classification.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ResourceMappings;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.gpu.GpuResourceAllocator;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.docker.DockerRunCommand;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.docker.DockerVolumeCommand;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.DockerCommandPlugin;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.runtime.ContainerExecutionException;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * 基于nvidia-docker v2实现的GPU Docker命令插件，负责为Docker容器配置GPU资源
 */
public class NvidiaDockerV2CommandPlugin implements DockerCommandPlugin {
  final static Logger LOG = LoggerFactory.
      getLogger(NvidiaDockerV2CommandPlugin.class);

  // nvidia-docker runtime名称
  private String nvidiaRuntime = "nvidia";
  // 可见GPU设备环境变量名
  private String nvidiaVisibleDevices = "NVIDIA_VISIBLE_DEVICES";

  public NvidiaDockerV2CommandPlugin() {}

  /**
   * 获取容器被分配的GPU设备列表
   * @param container 目标容器
   * @return 分配给容器的GPU设备集合，无分配则返回空集合
   */
  private Set<GpuDevice> getAssignedGpus(Container container) {
    ResourceMappings resourceMappings = container.getResourceMappings();

    // Copy of assigned Resources
    Set<GpuDevice> assignedResources = null;
    if (resourceMappings != null) {
      assignedResources = new HashSet<>();
      // 遍历所有已分配的GPU资源，转换为GpuDevice对象
      for (Serializable s : resourceMappings.getAssignedResources(
          ResourceInformation.GPU_URI)) {
        assignedResources.add((GpuDevice) s);
      }
    }
    if (assignedResources == null || assignedResources.isEmpty()) {
      // When no GPU resource assigned, don't need to update docker command.
      return Collections.emptySet();
    }
    return assignedResources;
  }

  /**
   * 检查容器是否申请了GPU资源
   * @param container 目标容器
   * @return true表示申请了GPU，false表示未申请
   */
  @VisibleForTesting
  protected boolean requestsGpu(Container container) {
    return GpuResourceAllocator.getRequestedGpus(container.getResource()) > 0;
  }

  @Override
  /**
   * 更新Docker运行命令，添加nvidia-docker v2所需的runtime和环境变量配置
   * @param dockerRunCommand Docker运行命令对象
   * @param container 目标容器
   * @throws ContainerExecutionException 容器执行异常
   */
  public synchronized void updateDockerRunCommand(
      DockerRunCommand dockerRunCommand, Container container)
      throws ContainerExecutionException {
    // 容器未申请GPU，直接返回
    if (!requestsGpu(container)) {
      return;
    }
    // 获取已分配的GPU设备
    Set<GpuDevice> assignedResources = getAssignedGpus(container);
    if (assignedResources == null || assignedResources.isEmpty()) {
      return;
    }
    // 准备环境变量映射
    Map<String, String> environment = new HashMap<>();
    String gpuIndexList = "";
    // 拼接所有已分配GPU的索引字符串
    for (GpuDevice gpuDevice : assignedResources) {
      gpuIndexList = gpuIndexList + gpuDevice.getIndex() + ",";
      LOG.info("nvidia docker2 assigned gpu index: " + gpuDevice.getIndex());
    }
    // 添加nvidia runtime参数
    dockerRunCommand.addRuntime(nvidiaRuntime);
    // 去掉末尾多余的逗号，设置NVIDIA_VISIBLE_DEVICES环境变量
    environment.put(nvidiaVisibleDevices,
            gpuIndexList.substring(0, gpuIndexList.length() - 1));
    // 将环境变量添加到Docker运行命令中
    dockerRunCommand.addEnv(environment);
  }

  @Override
  public DockerVolumeCommand getCreateDockerVolumeCommand(Container container)
      throws ContainerExecutionException {
    // nvidia-docker2不需要额外创建数据卷
    return null;
  }

  @Override
  public DockerVolumeCommand getCleanupDockerVolumesCommand(Container container)
      throws ContainerExecutionException {
    // 不需要清理数据卷
    return null;
  }
}