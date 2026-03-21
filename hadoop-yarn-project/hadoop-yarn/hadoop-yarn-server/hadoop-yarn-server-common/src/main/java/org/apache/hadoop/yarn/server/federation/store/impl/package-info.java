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
 * YARN 联邦状态存储的具体实现包，提供了多种基于不同存储后端的联邦元数据存储实现。
 * 包含基于ZooKeeper、内存、文件等不同存储方案，用于存储YARN联邦集群的子集群信息、
 * 路由信息、应用placement信息等核心元数据。
 */
package org.apache.hadoop.yarn.server.federation.store.impl;