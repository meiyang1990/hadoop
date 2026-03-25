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
 * YARN 联邦状态存储根包，提供了联邦集群元数据存储的核心抽象接口和定义。
 * 负责存储和管理联邦集群中各子集群的注册信息、路由信息等核心元数据，
 * 支持不同的存储实现（如基于ZooKeeper、基于HDFS等）供联邦路由器使用。
 */
package org.apache.hadoop.yarn.server.federation.store;