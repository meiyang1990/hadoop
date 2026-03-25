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
 * HDFS磁盘均衡器顶层包，提供DataNode节点磁盘间数据均衡能力。
 *
 * 磁盘均衡器的核心工作流程：
 *
 * 1) 按存储介质类型分组，计算每组卷中每个卷应承载的平均数据量。例如计算一台节点上所有SSD卷每个卷应分布的数据量。
 *
 * 2) 根据计算出的平均值，将数据从超出平均使用率的卷移动到低于平均使用率的卷，实现空间均衡。
 *
 * 3) 磁盘均衡器可在线运行，不影响DataNode节点正常对外提供服务。
 *
 */
package org.apache.hadoop.hdfs.server.diskbalancer;