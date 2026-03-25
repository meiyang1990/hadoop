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

/**
 * NVIDIA GPU资源插件包，提供YARN NodeManager对NVIDIA GPU资源的发现、隔离和分配能力
 * <p>
 * 该包实现了YARN自定义资源插件接口，用于支持GPU调度，负责在NodeManager节点上：
 * 1. 发现节点上可用的NVIDIA GPU设备信息
 * 2. 分配GPU资源给运行的容器
 * 3. 实现GPU设备的环境隔离（设置可见性）
 * 4. 容器释放后回收GPU资源
 */
package org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.com.nvidia;