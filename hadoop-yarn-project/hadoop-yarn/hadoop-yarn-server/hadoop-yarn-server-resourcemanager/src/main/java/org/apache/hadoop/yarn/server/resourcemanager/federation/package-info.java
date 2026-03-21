// 这个文件已经全部加上中文注释
/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

/**
 * YARN ResourceManager 联邦模式核心实现包。
 * 提供YARN集群联邦能力，支持将多个独立YARN集群聚合为一个大逻辑集群，
 * 实现集群水平扩展，支撑更大规模的应用调度与资源管理。
 * 核心功能包括跨集群应用路由、状态同步、资源视图聚合等。
 */
package org.apache.hadoop.yarn.server.resourcemanager.federation;