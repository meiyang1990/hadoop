// 这个文件已经全部加上中文注释
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.gpu;

import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;

/**
 * GPU设备配置异常，当允许使用的GPU设备配置为空或格式非法时抛出该异常
 */
public final class GpuDeviceSpecificationException extends YarnException {
  // GPU设备合法格式提示信息
  private static final String VALID_FORMAT_MESSAGE = "The valid format " +
      "should be: index:minor_number";

  private GpuDeviceSpecificationException(String message) {
    super(message);
  }

  private GpuDeviceSpecificationException(String message, Exception cause) {
    super(message, cause);
  }

  /**
   * 创建GPU配置为空的异常实例
   * @return 异常实例
   */
  public static GpuDeviceSpecificationException createWithEmptyValueSpecified() {
    return new GpuDeviceSpecificationException(
        YarnConfiguration.NM_GPU_ALLOWED_DEVICES +
        " is set to an empty value! Please specify " +
        YarnConfiguration.AUTOMATICALLY_DISCOVER_GPU_DEVICES +
        " to enable auto-discovery or " +
        "please enter the GPU device IDs manually! " +
            VALID_FORMAT_MESSAGE);
  }

  /**
   * 创建GPU配置格式非法的异常实例（包含原始异常）
   * @param device 非法的设备配置字符串
   * @param configValue 完整的配置值
   * @param cause 原始异常
   * @return 异常实例
   */
  public static GpuDeviceSpecificationException createWithWrongValueSpecified(
      String device, String configValue, Exception cause) {
    final String message = createIllegalFormatMessage(device, configValue);
    return new GpuDeviceSpecificationException(message, cause);
  }

  /**
   * 创建GPU配置格式非法的异常实例
   * @param device 非法的设备配置字符串
   * @param configValue 完整的配置值
   * @return 异常实例
   */
  public static GpuDeviceSpecificationException createWithWrongValueSpecified(
      String device, String configValue) {
    final String message = createIllegalFormatMessage(device, configValue);
    return new GpuDeviceSpecificationException(message);
  }

  /**
   * 创建GPU设备配置重复定义的异常实例
   * @param device 重复定义的设备
   * @param configValue 完整的配置值
   * @return 异常实例
   */
  public static GpuDeviceSpecificationException createWithDuplicateValueSpecified(
      String device, String configValue) {
    final String message = createDuplicateFormatMessage(device, configValue);
    return new GpuDeviceSpecificationException(message);
  }

  /**
   * 生成格式非法的错误信息
   * @param device 非法的设备配置
   * @param configValue 完整配置值
   * @return 格式化后的错误信息
   */
  private static String createIllegalFormatMessage(String device,
      String configValue) {
    return String.format("Illegal format of individual GPU device: %s, " +
            "the whole config value was: '%s'! " + VALID_FORMAT_MESSAGE,
        device, configValue);
  }

  /**
   * 生成重复定义的错误信息
   * @param device 重复的设备配置
   * @param configValue 完整配置值
   * @return 格式化后的错误信息
   */
  private static String createDuplicateFormatMessage(String device,
      String configValue) {
    return String.format("GPU device %s" +
            " has a duplicate definition! " +
            "Please double-check the configuration " +
            YarnConfiguration.NM_GPU_ALLOWED_DEVICES +
            "! Current value of the configuration is: %s",
        device, configValue);
  }
}