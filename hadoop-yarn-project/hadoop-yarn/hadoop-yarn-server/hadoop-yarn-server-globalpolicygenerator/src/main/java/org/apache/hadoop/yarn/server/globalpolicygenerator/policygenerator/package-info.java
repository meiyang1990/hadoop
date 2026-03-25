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
 * YARN全局策略生成器（GPG）的核心策略生成包。
 * 包内包含策略生成相关核心类，负责基于集群当前状态生成、更新全局调度策略，
 * 为YARN联邦集群提供统一的全局资源调度策略支持。
 */
package org.apache.hadoop.yarn.server.globalpolicygenerator.policygenerator;