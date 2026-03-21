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
 * FPGA 资源插件包，提供YARN NodeManager上的FPGA加速器资源管理能力
 * <p>
 * 核心功能：
 * <ul>
 *   <li>发现当前节点上的可用FPGA设备</li>
 *   <li>为容器分配和释放FPGA资源</li>
 *   <li>支持容器运行时对FPGA设备的隔离与配置</li>
 *   <li>对接不同厂商FPGA平台的设备发现与编程下载逻辑</li>
 * </ul>
 * 该模块属于YARN NodeManager的自定义资源插件扩展，允许YARN调度和管理集群中的FPGA异构计算资源，
 * 满足AI、深度学习、信号处理等对FPGA加速有需求的应用场景。
 */
package org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.fpga;