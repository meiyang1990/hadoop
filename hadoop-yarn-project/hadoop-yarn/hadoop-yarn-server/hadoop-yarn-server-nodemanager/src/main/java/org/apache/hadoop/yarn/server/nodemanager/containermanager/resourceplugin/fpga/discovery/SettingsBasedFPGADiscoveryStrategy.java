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
import org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.fpga.FpgaDevice;

/**
 * 基于静态配置的FPGA设备发现策略，从用户配置字符串解析FPGA设备信息
 * 配置字符串必须为单行且满足特定格式要求
 *
 * 格式详情参见DeviceSpecParser
 * 本策略用于用户提前静态指定节点上可用FPGA设备，不需要自动探测
 */
public class SettingsBasedFPGADiscoveryStrategy
    implements FPGADiscoveryStrategy {

  private final String type;
  private final String availableDevices;

  /**
   * 构造基于静态配置的FPGA发现策略
   * @param fpgaType FPGA设备类型
   * @param devices 设备配置字符串
   */
  public SettingsBasedFPGADiscoveryStrategy(
      String fpgaType, String devices) {
    this.type = fpgaType;
    this.availableDevices = devices;
  }

  @Override
  public List<FpgaDevice> discover() throws ResourceHandlerException {
    // 从配置字符串解析FPGA设备列表
    List<FpgaDevice> list =
        DeviceSpecParser.getDevicesFromString(type, availableDevices);
    // 配置为空时抛出异常
    if (list.isEmpty()) {
      throw new ResourceHandlerException("No FPGA devices were specified");
    }
    return list;
  }
}