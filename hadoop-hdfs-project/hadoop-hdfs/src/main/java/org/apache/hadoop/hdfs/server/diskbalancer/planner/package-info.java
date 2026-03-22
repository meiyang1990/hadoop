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
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * HDFS磁盘均衡器的规划器包，负责生成磁盘间数据均衡的移动计划。
 *
 * 核心职责：接收DataNode上一组磁盘的使用情况，根据设定的均衡阈值，
 * 迭代生成一系列数据移动步骤，最终让这组磁盘的数据分布达到均衡状态。
 *
 * 规划器核心工作流程：
 * <ol>
 * <li>检查当前磁盘集合是否需要均衡，如果已经满足均衡条件则停止规划</li>
 * <li>基于当前磁盘使用率分布，生成单步数据移动计划（确定源磁盘、目标磁盘和移动数据量）</li>
 * <li>将该步骤添加到整体计划中</li>
 * <li>更新磁盘集合的状态，模拟执行该步骤后的磁盘使用率变化</li>
 * <li>重复上述过程，直到达到均衡条件或无法继续优化</li>
 * </ol>
 * 本包是HDFS磁盘均衡器的核心规划逻辑模块，最终生成的计划会被均衡执行器执行，完成实际数据移动。
 */
package org.apache.hadoop.hdfs.server.diskbalancer.planner;