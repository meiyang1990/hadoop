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
 * HDFS磁盘均衡器的数据模型包，定义了磁盘均衡器运行所需的集群拓扑与磁盘信息结构。
 * 包中数据可从NameNode直接获取，也可从用户提供的JSON模型文件读取。
 * <p>
 * 数据模型层级结构概述：
 * <ul>
 * <li>DiskBalancerCluster：整个集群信息，包含所有DataNode列表</li>
 * <li>DiskBalancerDataNodes：单DataNode信息，包含该节点上所有卷集合</li>
 * <li>DiskBalancerVolumeSets：卷分组集合，包含多个卷</li>
 * <li>DiskBalancerVolumes：DataNode上实际物理磁盘卷的抽象，存储卷的使用信息</li>
 * </ul>
 * 该包为磁盘均衡算法提供统一的集群磁盘数据视图，是均衡计算和移动规划的基础。
 */
package org.apache.hadoop.hdfs.server.diskbalancer.datamodel;