// 这个文件已经全部加上中文注释
/*
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
 * YARN联邦路由策略管理器包
 * 该包提供了YARN联邦环境中不同路由策略管理器的实现，
 * 负责加载、维护和管理应用提交到子集群的路由策略，
 * 支持联邦集群中作业路由的可扩展策略管理。
 */
/** Various implementation of FederationPolicyManager. **/
package org.apache.hadoop.yarn.server.federation.policies.manager;