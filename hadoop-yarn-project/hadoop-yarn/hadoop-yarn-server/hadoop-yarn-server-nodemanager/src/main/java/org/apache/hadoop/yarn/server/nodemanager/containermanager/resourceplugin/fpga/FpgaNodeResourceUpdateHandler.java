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


package org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.fpga;

import static org.apache.hadoop.yarn.api.records.ResourceInformation.FPGA_URI;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.NodeResourceUpdaterPlugin;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * FPGA资源更新处理器，实现NodeManager节点资源更新插件，负责发现并上报当前节点可用的FPGA资源
 */
public class FpgaNodeResourceUpdateHandler extends NodeResourceUpdaterPlugin {
  private final FpgaDiscoverer fpgaDiscoverer;

  private static final Logger LOG = LoggerFactory.getLogger(
      FpgaNodeResourceUpdateHandler.class);

  /**
   * 构造FPGA资源更新处理器，注入FPGA发现器实例
   * @param fpgaDiscoverer FPGA设备发现器
   */
  public FpgaNodeResourceUpdateHandler(FpgaDiscoverer fpgaDiscoverer) {
    this.fpgaDiscoverer = fpgaDiscoverer;
  }

  @Override
  /**
   * 更新当前节点配置的FPGA资源总量
   * @param res 节点资源对象，用于更新FPGA资源数量
   * @throws YarnException 配置错误时抛出异常
   */
  public void updateConfiguredResource(Resource res) throws YarnException {
    LOG.info("Initializing configured FPGA resources for the NodeManager.");
    // 获取当前节点所有FPGA设备信息
    List<FpgaDevice> list = fpgaDiscoverer.getCurrentFpgaInfo();
    // 收集所有可用FPGA设备的次设备号
    List<Integer> minors = new LinkedList<>();
    for (FpgaDevice device : list) {
      minors.add(device.getMinor());
    }
    // 未发现可用FPGA，直接返回
    if (minors.isEmpty()) {
      LOG.info("Didn't find any usable FPGAs on the NodeManager.");
      return;
    }
    // 统计可用FPGA数量
    long count = minors.size();

    // 获取已配置的资源类型集合
    Map<String, ResourceInformation> configuredResourceTypes =
        ResourceUtils.getResourceTypes();
    // 检查FPGA资源类型是否已配置
    if (!configuredResourceTypes.containsKey(FPGA_URI)) {
      throw new YarnException("Wrong configurations, found " + count +
          " usable FPGAs, however " + FPGA_URI
          + " resource-type is not configured inside"
          + " resource-types.xml, please configure it to enable FPGA feature or"
          + " remove " + FPGA_URI + " from "
          + YarnConfiguration.NM_RESOURCE_PLUGINS);
    }

    // 更新节点资源中FPGA资源的总数量
    res.setResourceValue(FPGA_URI, count);
  }
}