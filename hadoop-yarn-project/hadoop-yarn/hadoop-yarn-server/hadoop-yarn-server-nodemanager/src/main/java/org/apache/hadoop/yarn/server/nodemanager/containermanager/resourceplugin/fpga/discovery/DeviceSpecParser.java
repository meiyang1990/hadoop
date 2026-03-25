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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.ResourceHandlerException;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.fpga.FpgaDevice;

/**
 * FPGA设备规格解析工具类，用于解析用户配置的FPGA设备描述字符串，提取设备信息。
 * 多个设备用逗号分隔，单个设备格式为：设备名称/主设备号:次设备号
 * 
 * 示例: "acl0/243:0,acl1/243:1"
 */
public final class DeviceSpecParser {
  // FPGA设备规格正则表达式，匹配 设备名/主设备号:次设备号 格式
  private static final String DEVICE_SPEC_REGEX =
      "(\\w+[0-31])(\\/)(\\d+)(\\:)(\\d+)";

  // 预编译正则表达式模式
  private static final Pattern DEVICE_PATTERN =
      Pattern.compile(DEVICE_SPEC_REGEX);

  private DeviceSpecParser() {
    // 工具类不允许实例化
  }

  /**
   * 从设备描述字符串解析出所有FPGA设备信息。
   * @param type 设备类型标识
   * @param devices 设备描述字符串，多设备逗号分隔
   * @return 解析后的FPGA设备列表
   * @throws ResourceHandlerException 解析失败时抛出异常
   */
  static List<FpgaDevice> getDevicesFromString(String type, String devices)
      throws ResourceHandlerException {
    if (devices.trim().isEmpty()) {
      return Collections.emptyList();
    }

    // 按逗号分割多个设备
    String[] deviceList = devices.split(",");

    List<FpgaDevice> fpgaDevices = new ArrayList<>();

    // 遍历解析每个设备规格
    for (final String deviceSpec : deviceList) {
      Matcher matcher = DEVICE_PATTERN.matcher(deviceSpec);
      if (matcher.matches()) {
        try {
          // 提取设备名称
          String devName = matcher.group(1);
          // 提取并转换主设备号
          int major = Integer.parseInt(matcher.group(3));
          // 提取并转换次设备号
          int minor = Integer.parseInt(matcher.group(5));
          // 创建FPGA设备对象并加入列表
          fpgaDevices.add(new FpgaDevice(type,
              major,
              minor,
              devName));
        } catch (NumberFormatException e) {
          throw new ResourceHandlerException(
              "Cannot parse major/minor number: " + deviceSpec);
        }
      } else {
        throw new ResourceHandlerException(
            "Illegal device specification string: " + deviceSpec);
      }
    }

    return fpgaDevices;
  }
}