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
import java.util.Optional;
import java.util.function.Function;

import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.ResourceHandlerException;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.fpga.FpgaDevice;

/**
 * 基于外部脚本的FPGA设备发现策略，通过调用用户指定的外部脚本获取当前节点上可用的FPGA设备信息
 * 外部脚本需要按照约定格式返回设备信息，具体解析规则见DeviceSpecParser
 */
public class ScriptBasedFPGADiscoveryStrategy
    implements FPGADiscoveryStrategy {

  // 脚本执行函数，输入脚本路径，输出脚本执行结果
  private final Function<String, Optional<String>> scriptRunner;
  // 发现脚本的路径
  private final String discoveryScript;
  // FPGA设备厂商类型
  private final String type;

  /**
   * 构造基于外部脚本的FPGA设备发现策略实例
   * @param fpgaType FPGA设备厂商类型
   * @param scriptRunner 脚本执行函数
   * @param propValue 发现脚本路径配置值
   */
  public ScriptBasedFPGADiscoveryStrategy(
      String fpgaType,
      Function<String, Optional<String>> scriptRunner,
      String propValue) {
    this.scriptRunner = scriptRunner;
    this.discoveryScript = propValue;
    this.type = fpgaType;
  }

  /**
   * 执行FPGA设备发现，调用外部脚本获取并解析设备信息
   * @return 当前节点可用FPGA设备列表
   * @throws ResourceHandlerException 脚本执行失败或未发现有效设备时抛出异常
   */
  @Override
  public List<FpgaDevice> discover() throws ResourceHandlerException {
    // 执行发现脚本获取输出
    Optional<String> scriptOutput =
        scriptRunner.apply(discoveryScript);
    if (scriptOutput.isPresent()) {
      // 解析脚本输出，提取FPGA设备信息
      List<FpgaDevice> list =
          DeviceSpecParser.getDevicesFromString(type, scriptOutput.get());
      if (list.isEmpty()) {
        throw new ResourceHandlerException("No FPGA devices were specified");
      }
      return list;
    } else {
      throw new ResourceHandlerException("Unable to run external script");
    }
  }
}