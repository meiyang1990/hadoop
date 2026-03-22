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
 * HDFS NameNode启动进度跟踪包，负责对NameNode启动过程进行分段建模和进度跟踪。
 * <p>
 * 核心设计将NameNode启动过程划分为多个粗粒度{@link Phase}阶段，每个阶段进一步拆分为多个细粒度{@link Step}步骤：
 * <ul>
 *   <li>阶段：粗粒度划分，提前定义，如加载fsimage、编辑日志回放等</li>
 *   <li>步骤：细粒度划分，运行时确定，如加载特定位置的指定fsimage文件</li>
 * </ul>
 * 核心组件职责：
 * <ul>
 *   <li>{@link StartupProgress}：线程安全的数据结构，存储启动过程的状态信息和计数器，供NameNode各模块更新进度</li>
 *   <li>{@link StartupProgressView}：提供不可变的一致性启动进度快照，用于向用户展示进度信息</li>
 *   <li>{@link StartupProgressMetrics}：通过JMX暴露启动进度指标，集成到Hadoop监控系统</li>
 * </ul>
 * 该模块为NameNode启动过程提供可观测能力，方便用户和运维人员了解启动进度，排查启动超时等问题。
 */
@InterfaceAudience.Private
package org.apache.hadoop.hdfs.server.namenode.startupprogress;

import org.apache.hadoop.classification.InterfaceAudience;