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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.DockerCommandPlugin;

/**
 * GPU Docker命令插件工厂，负责根据配置创建对应版本的GPU Docker插件实例
 * 用于在Docker容器环境中给YARN容器分配GPU资源时处理设备挂载
 */
public class GpuDockerCommandPluginFactory {
  /**
   * 根据配置创建对应版本的GPU Docker命令插件实例
   * @param conf YARN配置对象
   * @return 对应版本的GPU Docker命令插件实例
   * @throws YarnException 当配置的插件实现版本不支持时抛出异常
   */
  public static DockerCommandPlugin createGpuDockerCommandPlugin(
      Configuration conf) throws YarnException {
    // 从配置中读取GPU Docker插件实现类名称
    String impl = conf.get(YarnConfiguration.NM_GPU_DOCKER_PLUGIN_IMPL,
        YarnConfiguration.DEFAULT_NM_GPU_DOCKER_PLUGIN_IMPL);
    // 如果是nvidia-docker v1版本，创建对应插件实例
    if (impl.equals(YarnConfiguration.NVIDIA_DOCKER_V1)) {
      return new NvidiaDockerV1CommandPlugin(conf);
    }
    // 如果是nvidia-docker v2版本，创建对应插件实例
    if (impl.equals(YarnConfiguration.NVIDIA_DOCKER_V2)) {
      return new NvidiaDockerV2CommandPlugin();
    }

    // 配置的实现版本未知，抛出异常
    throw new YarnException(
        "Unkown implementation name for Gpu docker plugin, impl=" + impl);
  }
}