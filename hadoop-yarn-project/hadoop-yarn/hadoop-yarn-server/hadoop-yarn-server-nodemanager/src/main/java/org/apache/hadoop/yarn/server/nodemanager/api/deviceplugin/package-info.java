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

/**
 * NodeManager 设备插件API包，定义了外部自定义设备插件与YARN NodeManager交互的核心接口。
 * 允许第三方扩展支持特殊计算设备（如GPU、FPGA、NPU等）的资源分配与管理，实现自定义设备的调度集成。
 * 核心接口包括{@link org.apache.hadoop.yarn.server.nodemanager.api.deviceplugin.DevicePlugin}
 * 定义了设备插件需要实现的生命周期与能力，NodeManager通过该接口与设备插件交互。
 */
package org.apache.hadoop.yarn.server.nodemanager.api.deviceplugin;