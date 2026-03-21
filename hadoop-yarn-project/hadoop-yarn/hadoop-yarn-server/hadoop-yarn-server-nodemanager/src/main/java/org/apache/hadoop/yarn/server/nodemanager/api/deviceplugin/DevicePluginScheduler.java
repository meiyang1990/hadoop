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

import java.util.Map;
import java.util.Set;

/**
 * 设备插件自定义调度器接口，当需要自定义设备分配逻辑时可实现该接口
 * 若未实现该接口，则默认由YARN设备框架完成设备调度分配
 * */
public interface DevicePluginScheduler {
  /**
   * 设备分配调度钩子，在YARN设备框架分配设备时调用
   * 框架已负责设备台账管理和故障恢复，因此该接口无需维护状态，仅需根据传入的可用设备完成调度决策
   * 框架可能多次调用该方法，可通过容器环境变量传入的调度参数做出更贴合业务的分配决策
   * 例如GPU调度可通过环境变量指定不同调度策略
   * @param availableDevices 可用于分配的候选设备集合
   * @param count 需要分配的设备数量
   * @param env 待分配容器的环境变量，可携带自定义调度参数
   * @return 完成分配的设备集合
   * */
  Set<Device> allocateDevices(Set<Device> availableDevices, int count,
      Map<String, String> env);
}