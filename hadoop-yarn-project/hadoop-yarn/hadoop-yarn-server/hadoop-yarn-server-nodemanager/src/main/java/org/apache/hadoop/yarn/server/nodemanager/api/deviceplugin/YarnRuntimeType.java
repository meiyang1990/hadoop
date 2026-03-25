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

package org.apache.hadoop.yarn.server.nodemanager.api.deviceplugin;

/**
 * 设备插件使用的YARN容器运行时类型枚举
 * 该枚举会传入{@code onDevicesAllocated}方法，设备插件可根据当前实际使用的运行时类型
 * 生成对应{@link DeviceRuntimeSpec}，指导YARN完成设备在对应容器运行时下的配置
 * */
public enum YarnRuntimeType {

  /** 默认容器运行时 */
  RUNTIME_DEFAULT("default"),
  /** Docker容器运行时 */
  RUNTIME_DOCKER("docker");

  private final String name;

  YarnRuntimeType(String n) {
    this.name = n;
  }

  /**
   * 获取运行时名称字符串
   * @return 运行时名称
   */
  public String getName() {
    return name;
  }
}