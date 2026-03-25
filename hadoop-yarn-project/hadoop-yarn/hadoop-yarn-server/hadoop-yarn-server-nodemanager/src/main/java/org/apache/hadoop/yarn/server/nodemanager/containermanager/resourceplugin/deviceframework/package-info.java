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
 * 设备框架资源插件包，提供YARN NodeManager上通用的异构设备资源管理抽象框架。
 * 核心职责是统一GPU、FPGA等各类硬件设备的资源分配、隔离调度逻辑，
 * 允许第三方快速扩展自定义设备资源插件，无需重复实现基础设备管理流程。
 * 该框架集成在NodeManager容器管理器的资源插件体系中，负责节点本地设备发现、分配和回收。
 */
package org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.deviceframework;