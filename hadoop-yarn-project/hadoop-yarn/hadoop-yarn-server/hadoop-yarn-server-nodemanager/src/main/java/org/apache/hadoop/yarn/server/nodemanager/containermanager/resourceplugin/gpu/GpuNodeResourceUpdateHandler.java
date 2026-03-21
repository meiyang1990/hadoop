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

import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.ResourcesExceptionUtil.throwIfNecessary;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.NodeResourceUpdaterPlugin;
import org.apache.hadoop.yarn.server.nodemanager.webapp.dao.gpu.PerGpuDeviceInformation;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.hadoop.yarn.api.records.ResourceInformation.GPU_URI;

/**
 * GPU节点资源更新处理器，负责在NodeManager节点发现并更新GPU资源，同时提供GPU使用率监控能力
 * 继承NodeResourceUpdaterPlugin，作为YARN节点资源更新插件实现
 */
public class GpuNodeResourceUpdateHandler extends NodeResourceUpdaterPlugin {
  private static final Logger LOG =
      LoggerFactory.getLogger(GpuNodeResourceUpdateHandler.class);
  // GPU发现器实例，负责探测当前节点GPU设备信息
  private final GpuDiscoverer gpuDiscoverer;
  // YARN配置对象
  private Configuration conf;

  /**
   * 构造函数，初始化GPU发现器和配置对象
   * @param gpuDiscoverer GPU发现器实例
   * @param conf YARN配置对象
   */
  public GpuNodeResourceUpdateHandler(GpuDiscoverer gpuDiscoverer,
      Configuration conf) {
    this.gpuDiscoverer = gpuDiscoverer;
    this.conf = conf;
  }

  @Override
  /**
   * 更新NodeManager节点上可分配的GPU资源信息
   * @param res 节点资源对象，用于更新GPU资源数量
   * @throws YarnException 当GPU启用但未找到可用GPU时抛出异常（根据配置决定是否抛出）
   */
  public void updateConfiguredResource(Resource res) throws YarnException {
    LOG.info("Initializing configured GPU resources for the NodeManager.");

    // 获取YARN可用的GPU设备列表
    List<GpuDevice> usableGpus = gpuDiscoverer.getGpusUsableByYarn();
    if (usableGpus == null || usableGpus.isEmpty()) {
      String message = "GPU is enabled, " +
          "but could not find any usable GPUs on the NodeManager!";
      LOG.error(message);
      // 根据配置决定是否抛出异常，允许配置忽略找不到GPU的情况
      throwIfNecessary(new YarnException(message), conf);
      return;
    }

    // 统计可用GPU数量
    long nUsableGpus = usableGpus.size();

    // 获取节点已配置的资源类型
    Map<String, ResourceInformation> configuredResourceTypes =
        ResourceUtils.getResourceTypes();
    // 检查GPU资源类型是否已配置，未配置则打印警告提示用户
    if (!configuredResourceTypes.containsKey(GPU_URI)) {
      LOG.warn("Found " + nUsableGpus + " usable GPUs, however "
          + GPU_URI
          + " resource-type is not configured inside"
          + " resource-types.xml, please configure it to enable GPU feature or"
          + " remove " + GPU_URI + " from "
          + YarnConfiguration.NM_RESOURCE_PLUGINS);
    }

    // 更新节点资源中GPU的可用数量
    res.setResourceValue(GPU_URI, nUsableGpus);
  }

  /**
   *
   * @return 当前节点平均GPU使用率（占总GPU容量的比例，0~1）
   *
   * For example:
   * Node with total 4 GPUs
   * Physical used 2.4 GPUs
   * Will return 2.4/4 = 0.6f
   *
   * @throws Exception when any error happens
   */
  public float getAvgNodeGpuUtilization() throws Exception{
    // 获取当前节点所有GPU的详细信息
    List<PerGpuDeviceInformation> gpuList =
        gpuDiscoverer.getGpuDeviceInformation().getGpus();
    Float avgGpuUtilization = 0F;
    if (gpuList != null &&
        gpuList.size() != 0) {
      // 总使用率除以GPU数量得到平均使用率
      avgGpuUtilization = getTotalNodeGpuUtilization() / gpuList.size();
    }
    return avgGpuUtilization;
  }

  /**
   *
   * @return 当前节点所有GPU的总物理使用率（所有单卡使用率之和，0~N，N为GPU总数）
   *
   * For example:
   * Node with total 4 GPUs
   * Physical used 2.4 GPUs
   * Will return 2.4f
   *
   * @throws Exception when any error happens
   */
  public float getTotalNodeGpuUtilization() throws Exception{
    // 获取当前节点所有GPU的详细信息
    List<PerGpuDeviceInformation> gpuList =
        gpuDiscoverer.getGpuDeviceInformation().getGpus();
    // 流式累加所有GPU的整体使用率
    Float totalGpuUtilization = gpuList
        .stream()
        .map(g -> g.getGpuUtilizations().getOverallGpuUtilization())
        .collect(Collectors.summingDouble(Float::floatValue))
        .floatValue();
    return totalGpuUtilization;
  }
}