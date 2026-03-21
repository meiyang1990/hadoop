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
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ResourceMappings;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.gpu.GpuResourceAllocator;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.docker.DockerRunCommand;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.docker.DockerVolumeCommand;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.DockerCommandPlugin;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.runtime.ContainerExecutionException;

import java.io.IOException;
import java.io.Serializable;
import java.io.StringWriter;
import java.net.URL;
import java.net.URLConnection;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.docker.DockerVolumeCommand.VOLUME_NAME_PATTERN;

/**
 * 基于nvidia-docker v1实现的GPU Docker命令插件，用于在Docker容器中配置GPU资源
 * 实现了DockerCommandPlugin接口，为GPU容器生成正确的Docker运行命令参数
 */
public class NvidiaDockerV1CommandPlugin implements DockerCommandPlugin {
  final static Logger LOG = LoggerFactory.
      getLogger(NvidiaDockerV1CommandPlugin.class);

  private Configuration conf;
  private Map<String, Set<String>> additionalCommands = null;
  private String volumeDriver = "local";

  // 已知命令选项
  private String DEVICE_OPTION = "--device";
  private String VOLUME_DRIVER_OPTION = "--volume-driver";
  private String MOUNT_RO_OPTION = "--volume";

  /**
   * 构造函数，传入Yarn配置
   * @param conf Yarn配置对象
   */
  public NvidiaDockerV1CommandPlugin(Configuration conf) {
    this.conf = conf;
  }

  /**
   * 从key=value格式字符串中提取value部分
   * @param input 输入字符串
   * @return 提取出的value
   * @throws IllegalArgumentException 当未找到=分隔符时抛出
   */
  private String getValue(String input) throws IllegalArgumentException {
    int index = input.indexOf('=');
    if (index < 0) {
      throw new IllegalArgumentException(
          "Failed to locate '=' from input=" + input);
    }
    return input.substring(index + 1);
  }

  /**
   * 添加命令选项到内部存储
   * @param key 选项名称
   * @param value 选项值
   */
  private void addToCommand(String key, String value) {
    if (additionalCommands == null) {
      additionalCommands = new HashMap<>();
    }
    if (!additionalCommands.containsKey(key)) {
      additionalCommands.put(key, new HashSet<>());
    }
    additionalCommands.get(key).add(value);
  }

  /**
   * 初始化插件，从nvidia-docker v1插件服务端获取所需的Docker命令参数
   * @throws ContainerExecutionException 初始化失败时抛出
   */
  private void init() throws ContainerExecutionException {
    // 从配置中获取nvidia-docker插件端点地址
    String endpoint = conf.get(
        YarnConfiguration.NVIDIA_DOCKER_PLUGIN_V1_ENDPOINT,
        YarnConfiguration.DEFAULT_NVIDIA_DOCKER_PLUGIN_V1_ENDPOINT);
    if (null == endpoint || endpoint.isEmpty()) {
      LOG.info(YarnConfiguration.NVIDIA_DOCKER_PLUGIN_V1_ENDPOINT
          + " set to empty, skip init ..");
      return;
    }
    String cliOptions;
    try {
      // 连接插件服务端获取CLI选项
      URL url = new URL(endpoint);
      URLConnection uc = url.openConnection();
      uc.setRequestProperty("X-Requested-With", "Curl");

      StringWriter writer = new StringWriter();
      IOUtils.copy(uc.getInputStream(), writer, StandardCharsets.UTF_8);
      cliOptions = writer.toString();

      LOG.info("Additional docker CLI options from plugin to run GPU "
          + "containers:" + cliOptions);

      // 解析CLI选项，示例格式：
      // --device=/dev/nvidiactl --device=/dev/nvidia-uvm --device=/dev/nvidia0
      // --volume-driver=nvidia-docker
      // --volume=nvidia_driver_352.68:/usr/local/nvidia:ro

      for (String str : cliOptions.split(" ")) {
        str = str.trim();
        if (str.startsWith(DEVICE_OPTION)) {
          addToCommand(DEVICE_OPTION, getValue(str));
        } else if (str.startsWith(VOLUME_DRIVER_OPTION)) {
          volumeDriver = getValue(str);
          LOG.debug("Found volume-driver:{}", volumeDriver);
        } else if (str.startsWith(MOUNT_RO_OPTION)) {
          String mount = getValue(str);
          if (!mount.endsWith(":ro")) {
            throw new IllegalArgumentException(
                "Should not have mount other than ro, command=" + str);
          }
          addToCommand(MOUNT_RO_OPTION,
              mount.substring(0, mount.lastIndexOf(':')));
        } else{
          throw new IllegalArgumentException("Unsupported option:" + str);
        }
      }
    } catch (RuntimeException e) {
      LOG.warn(
          "RuntimeException of " + this.getClass().getSimpleName() + " init:",
          e);
      throw new ContainerExecutionException(e);
    } catch (IOException e) {
      LOG.warn("IOException of " + this.getClass().getSimpleName() + " init:",
          e);
      throw new ContainerExecutionException(e);
    }
  }

  /**
   * 从设备路径中提取GPU索引号
   * @param device 设备路径，如/dev/nvidia0
   * @return GPU索引号，提取失败返回-1
   */
  private int getGpuIndexFromDeviceName(String device) {
    final String NVIDIA = "nvidia";
    int idx = device.lastIndexOf(NVIDIA);
    if (idx < 0) {
      return -1;
    }
    // 获取nvidia后的字符串部分
    String str = device.substring(idx + NVIDIA.length());
    for (int i = 0; i < str.length(); i++) {
      if (!Character.isDigit(str.charAt(i))) {
        return -1;
      }
    }
    return Integer.parseInt(str);
  }

  /**
   * 获取当前容器被分配的GPU设备集合
   * @param container YARN容器对象
   * @return 分配给容器的GPU设备集合，无分配则返回空集合
   */
  private Set<GpuDevice> getAssignedGpus(Container container) {
    ResourceMappings resourceMappings = container.getResourceMappings();

    // 已分配资源副本
    Set<GpuDevice> assignedResources = null;
    if (resourceMappings != null) {
      assignedResources = new HashSet<>();
      for (Serializable s : resourceMappings.getAssignedResources(
          ResourceInformation.GPU_URI)) {
        assignedResources.add((GpuDevice) s);
      }
    }

    if (assignedResources == null || assignedResources.isEmpty()) {
      // 没有分配GPU资源，不需要修改Docker命令
      return Collections.emptySet();
    }

    return assignedResources;
  }

  /**
   * 检查容器是否请求了GPU资源
   * @param container YARN容器对象
   * @return true表示容器请求了GPU，false否则
   */
  @VisibleForTesting
  protected boolean requestsGpu(Container container) {
    return GpuResourceAllocator.getRequestedGpus(container.getResource()) > 0;
  }

  /**
   * 当容器请求GPU时执行懒初始化
   * @param container YARN容器对象
   * @return true表示请求了GPU且初始化完成，false否则
   * @throws ContainerExecutionException 初始化失败时抛出
   */
  private boolean initializeWhenGpuRequested(Container container)
      throws ContainerExecutionException {
    if (!requestsGpu(container)) {
      return false;
    }

    // 对gpu-docker插件执行懒初始化
    if (additionalCommands == null) {
      init();
    }

    return true;
  }

  @Override
  /**
   * 更新Docker运行命令，添加GPU相关配置参数
   * @param dockerRunCommand Docker运行命令对象
   * @param container YARN容器对象
   * @throws ContainerExecutionException 处理失败时抛出
   */
  public synchronized void updateDockerRunCommand(
      DockerRunCommand dockerRunCommand, Container container)
      throws ContainerExecutionException {
    if (!initializeWhenGpuRequested(container)) {
      return;
    }

    Set<GpuDevice> assignedResources = getAssignedGpus(container);
    if (assignedResources == null || assignedResources.isEmpty()) {
      return;
    }

    // 将GPU配置写入Docker运行命令
    for (Map.Entry<String, Set<String>> option : additionalCommands
        .entrySet()) {
      String key = option.getKey();
      Set<String> values = option.getValue();
      if (key.equals(DEVICE_OPTION)) {
        int foundGpuDevices = 0;
        for (String deviceName : values) {
          // 对于GPU卡设备，格式为/dev/nvidia[n]
          // 提取索引号[n]
          Integer gpuIdx = getGpuIndexFromDeviceName(deviceName);
          if (gpuIdx >= 0) {
            // 使用分配的GPU列表过滤nvidia-docker-plugin返回的设备
            for (GpuDevice gpuDevice : assignedResources) {
              if (gpuDevice.getIndex() == gpuIdx) {
                foundGpuDevices++;
                dockerRunCommand.addDevice(deviceName, deviceName);
              }
            }
          } else{
            // GPU索引号小于0说明是控制设备（如/dev/nvidiactl），直接添加
            dockerRunCommand.addDevice(deviceName, deviceName);
          }
        }

        // 未找到所有已分配的GPU设备，抛出异常
        if (foundGpuDevices < assignedResources.size()) {
          throw new ContainerExecutionException(
              "Cannot get all assigned Gpu devices from docker plugin output");
        }
      } else if (key.equals(MOUNT_RO_OPTION)) {
        for (String value : values) {
          int idx = value.indexOf(':');
          String source = value.substring(0, idx);
          String target = value.substring(idx + 1);
          dockerRunCommand.addReadOnlyMountLocation(source, target, true);
        }
      } else{
        throw new ContainerExecutionException("Unsupported option:" + key);
      }
    }
  }

  @Override
  /**
   * 获取创建GPU相关Docker数据卷的命令
   * @param container YARN容器对象
   * @return 创建卷命令，不需要创建则返回null
   * @throws ContainerExecutionException 处理失败时抛出
   */
  public DockerVolumeCommand getCreateDockerVolumeCommand(Container container)
      throws ContainerExecutionException {
    if (!initializeWhenGpuRequested(container)) {
      return null;
    }

    String newVolumeName = null;

    // 从挂载配置中提取卷名
    Set<String> mounts = additionalCommands.get(MOUNT_RO_OPTION);
    for (String mount : mounts) {
      int idx = mount.indexOf(':');
      if (idx >= 0) {
        String mountSource = mount.substring(0, idx);
        if (VOLUME_NAME_PATTERN.matcher(mountSource).matches()) {
          // 匹配到有效的命名卷
          newVolumeName = mountSource;
          LOG.debug("Found volume name for GPU:{}", newVolumeName);
          break;
        } else{
          LOG.debug("Failed to match {} to named-volume regex pattern",
              mountSource);
        }
      }
    }

    if (newVolumeName != null) {
      DockerVolumeCommand command = new DockerVolumeCommand(
          DockerVolumeCommand.VOLUME_CREATE_SUB_COMMAND);
      command.setDriverName(volumeDriver);
      command.setVolumeName(newVolumeName);
      return command;
    }

    return null;
  }

  @Override
  /**
   * 获取清理Docker数据卷的命令
   * @param container YARN容器对象
   * @return 清理命令，本实现不需要清理，返回null
   * @throws ContainerExecutionException 处理失败时抛出
   */
  public DockerVolumeCommand getCleanupDockerVolumesCommand(Container container)
      throws ContainerExecutionException {
    // 不需要清理
    return null;
  }
}