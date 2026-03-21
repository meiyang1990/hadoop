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

package org.apache.hadoop.yarn.server.nodemanager.api.deviceplugin;

import java.util.Set;

/**
 * YARN NodeManager 设备插件SPI接口，第三方硬件厂商必须实现该接口来对接自定义设备。
 * 用于扩展YARN对GPU、FPGA等特殊计算设备的支持，实现设备的发现、分配和生命周期管理。
 * */
public interface DevicePlugin {
  /**
   * 设备插件向NodeManager注册时调用，是注册流程的第一个方法。
   * @return 设备注册请求信息，包含设备类型等基本信息
   * @throws Exception 注册过程中发生异常
   * */
  DeviceRegisterRequest getRegisterRequestInfo()
      throws Exception;

  /**
   * NodeManager更新节点可用资源时调用，获取当前节点上该类型设备的全部信息。
   * @return 当前节点上所有设备的集合，推荐使用TreeSet保证有序性
   * @throws Exception 获取设备信息过程中发生异常
   * */
  Set<Device> getDevices() throws Exception;

  /**
   * 容器启动前，当设备已分配给容器后调用，用于准备设备运行环境。
   * 插件可以自行完成准备工作，也可以通过DeviceRuntimeSpec定义，交由YARN框架完成。
   * 例如可以定义数据卷规格，让框架在容器启动前自动创建设备数据卷。
   *
   * @param allocatedDevices 已分配给容器的设备集合
   * @param yarnRuntime YARN将使用的容器运行时类型，
   *        可选值为{@link DeviceRuntimeSpec}中定义的RUNTIME_DEFAULT（原生容器）
   *        或RUNTIME_DOCKER（Docker容器）
   * @return 设备运行时规格描述，包含环境变量、数据卷、挂载点等配置
   * @throws Exception 设备分配处理过程中发生异常
   * */
  DeviceRuntimeSpec onDevicesAllocated(Set<Device> allocatedDevices,
      YarnRuntimeType yarnRuntime) throws Exception;

  /**
   * 容器运行结束，设备释放完成后调用，用于插件执行清理工作。
   * @param releasedDevices 已释放的设备集合
   * @throws Exception 设备释放处理过程中发生异常
   * */
  void onDevicesReleased(Set<Device> releasedDevices)
      throws Exception;
}