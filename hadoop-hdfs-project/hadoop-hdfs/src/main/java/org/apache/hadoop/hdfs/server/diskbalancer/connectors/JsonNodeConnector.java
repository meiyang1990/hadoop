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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import org.apache.hadoop.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hdfs.server.diskbalancer.datamodel.DiskBalancerCluster;
import org.apache.hadoop.hdfs.server.diskbalancer.datamodel
    .DiskBalancerDataNode;

import java.io.File;
import java.net.URL;
import java.util.List;

/**
 * 文件级：磁盘均衡器JSON格式集群元数据连接器，从JSON文件读取集群节点信息
 * 从JSON格式的集群定义文件中读取磁盘均衡器所需的集群节点数据，用于离线分析或测试场景
 */
public class JsonNodeConnector implements ClusterConnector {
  private static final Logger LOG =
      LoggerFactory.getLogger(JsonNodeConnector.class);
  private static final ObjectReader READER =
      new ObjectMapper().readerFor(DiskBalancerCluster.class);
  private final URL clusterURI;

  /**
   * 构造JSON节点连接器，指定集群定义JSON文件的位置
   * @param clusterURI 存储集群信息的JSON文件URL
   */
  public JsonNodeConnector(URL clusterURI) {
    this.clusterURI = clusterURI;
  }

  /**
   * 从JSON集群定义文件读取所有数据节点信息，返回节点列表供磁盘均衡器使用
   * @return 磁盘均衡器数据节点列表
   * @throws Exception 读取文件或解析JSON失败时抛出异常
   */
  @Override
  public List<DiskBalancerDataNode> getNodes() throws Exception {
    // 检查集群文件URL不为空
    Preconditions.checkNotNull(this.clusterURI);
    // 从URL获取文件路径
    String dataFilePath = this.clusterURI.getPath();
    LOG.info("Reading cluster info from file : " + dataFilePath);
    // 解析JSON文件生成集群对象
    DiskBalancerCluster cluster = READER.readValue(new File(dataFilePath));
    String message = String.format("Found %d node(s)",
        cluster.getNodes().size());
    LOG.info(message);
    // 返回解析得到的节点列表
    return cluster.getNodes();
  }

  /**
   * 获取当前连接器的描述信息，用于日志和调试
   * @return 连接器描述字符串
   */
  @Override
  public String getConnectorInfo() {
    return "Json Cluster Connector : Connects to a JSON file that describes a" +
        " cluster : " + clusterURI.toString();
  }
}