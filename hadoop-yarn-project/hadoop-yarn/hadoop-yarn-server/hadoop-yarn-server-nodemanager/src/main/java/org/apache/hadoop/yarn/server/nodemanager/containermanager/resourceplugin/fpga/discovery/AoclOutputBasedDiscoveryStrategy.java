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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.fpga.discovery;

import java.util.List;

import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.ResourceHandlerException;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.fpga.AbstractFpgaVendorPlugin;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.fpga.FpgaDevice;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.fpga.FpgaDiscoverer;

/**
 * 基于Altera OpenCL (AOCL) SDK输出的FPGA设备发现策略
 * 通过调用AOCL命令行工具获取当前节点上可用的FPGA设备列表
 */
public class AoclOutputBasedDiscoveryStrategy
    implements FPGADiscoveryStrategy {

  private final AbstractFpgaVendorPlugin plugin;

  /**
   * 构造基于AOCL输出的发现策略实例
   * @param fpgaPlugin FPGA厂商插件实例
   */
  public AoclOutputBasedDiscoveryStrategy(AbstractFpgaVendorPlugin fpgaPlugin) {
    this.plugin = fpgaPlugin;
  }

  @Override
  public List<FpgaDevice> discover() throws ResourceHandlerException {
    // 通过厂商插件执行设备发现，设置最大执行超时时间
    List<FpgaDevice> list =
        plugin.discover(FpgaDiscoverer.MAX_EXECUTION_TIMEOUT_MS);
    // 如果未发现任何FPGA设备，抛出异常终止发现流程
    if (list.isEmpty()) {
      throw new ResourceHandlerException("No FPGA devices detected!");
    }

    return list;
  }
}