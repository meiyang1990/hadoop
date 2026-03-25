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

package org.apache.hadoop.hdfs.server.diskbalancer.connectors;

import org.apache.hadoop.hdfs.server.diskbalancer.datamodel.DiskBalancerDataNode;

import java.util.List;

/**
 * 文件：HDFS磁盘均衡器集群连接接口
 * 职责：抽象HDFS集群的访问逻辑，隐藏不同连接方式的实现细节，为磁盘均衡器提供统一的集群数据获取入口
 * 设计目的：支持多种接入方式（如从NameNode获取、从配置文件读取等），实现模块解耦
 */
public interface ClusterConnector {

  /**
   * 获取集群中所有数据节点信息，转换为磁盘均衡器可识别的数据模型
   * @return 磁盘均衡器数据节点列表，包含每个节点的磁盘容量、使用情况等信息
   * @throws Exception 获取集群信息失败时抛出异常
   */
  List<DiskBalancerDataNode> getNodes() throws Exception;

  /**
   * 获取当前连接器的描述信息，用于日志记录和监控
   * @return 连接器的描述字符串，包含连接类型、连接地址等信息
   */
  String getConnectorInfo();
}