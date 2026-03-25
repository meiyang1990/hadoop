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
 * NodeManager容器管理模块的资源插件包，提供可扩展的资源管理框架。
 * 核心职责是支持自定义资源类型（如GPU、FPGA、FPGA等异构计算资源）的管理，
 * 允许第三方开发者通过插件机制扩展YARN可调度的资源类型，无需修改核心代码。
 * 该包定义了资源插件的核心接口，负责在NodeManager节点上完成资源发现、
 * 容器资源分配和释放等生命周期操作，对接YARN资源调度系统。
 */
package org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin;