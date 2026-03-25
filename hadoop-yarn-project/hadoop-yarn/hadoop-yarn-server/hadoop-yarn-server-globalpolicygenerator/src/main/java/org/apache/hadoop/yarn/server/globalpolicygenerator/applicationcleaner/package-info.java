// 这个文件已经全部加上中文注释
/**
 *  Licensed to the Apache Software Foundation (ASF) under one
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
 * 全局策略生成器的应用清理器模块，提供联邦YARN集群中过期应用的清理策略生成与执行能力。
 * 该包下包含实现跨集群全局应用生命周期清理的核心类，负责清理联邦环境中已经完成/超时的应用，
 * 维持联邦集群的元数据一致性和存储利用率。
 */
package org.apache.hadoop.yarn.server.globalpolicygenerator.applicationcleaner;