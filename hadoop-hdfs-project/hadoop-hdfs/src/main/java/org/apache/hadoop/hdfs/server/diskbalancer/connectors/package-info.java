// 这个文件已经全部加上中文注释
/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

/**
 * 磁盘均衡器连接器包，提供多种数据源连接器实现，用于从不同来源读取Hadoop集群拓扑信息，
 * 为磁盘均衡计算提供集群数据，支持对接真实集群、JSON文件和内存测试数据源三种场景。
 * <p/>
 * 当前包包含三种核心连接器实现：
 * <ul>
 *   <li>DBNameNodeConnector：对接原生NameNode，连接真实运行的Hadoop集群获取集群信息</li>
 *   <li>JsonNodeConnector：从本地JSON文件读取集群拓扑信息，可通过磁盘均衡工具从真实集群导出，也可手动构造，用于离线分析和测试</li>
 *   <li>NullConnector：纯内存实现的连接器，主要用于测试场景，可动态创建数据节点接入，供磁盘均衡器从内存数据源读取数据</li>
 * </ul>
 */
package org.apache.hadoop.hdfs.server.diskbalancer.connectors;