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

import org.apache.hadoop.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.server.balancer.NameNodeConnector;
import org.apache.hadoop.hdfs.server.diskbalancer.datamodel
    .DiskBalancerDataNode;
import org.apache.hadoop.hdfs.server.diskbalancer.datamodel.DiskBalancerVolume;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorageReport;
import org.apache.hadoop.hdfs.server.protocol.StorageReport;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.LinkedList;
import java.util.List;

/**
 * 文件所属模块：HDFS磁盘均衡器核心模块
 * 核心职责：实现磁盘均衡器与NameNode的连接器，从NameNode获取集群数据节点和存储信息，为磁盘均衡提供集群拓扑和容量数据
 */
/**
 * DBNameNodeConnector connects to Namenode and extracts information from a
 * given cluster.
 */
/**
 * 磁盘均衡器的NameNode连接器，负责从NameNode获取集群节点与存储信息，实现ClusterConnector接口
 */
class DBNameNodeConnector implements ClusterConnector {
  private static final Logger LOG =
      LoggerFactory.getLogger(DBNameNodeConnector.class);
  static final Path DISKBALANCER_ID_PATH = new Path("/system/diskbalancer.id");
  private final URI clusterURI;
  private final NameNodeConnector connector;

  /**
   * 构造DBNameNodeConnector实例，初始化与NameNode的连接
   * @param clusterURI 目标HDFS集群的NameNode地址
   * @param conf Hadoop配置对象
   * @throws IOException 连接NameNode失败时抛出
   * @throws URISyntaxException 地址格式错误时抛出
   */
  public DBNameNodeConnector(URI clusterURI, Configuration conf) throws
      IOException, URISyntaxException {

    // 允许多个磁盘均衡实例同时运行，准入控制由数据节点负责，因此关闭ID文件写入
    NameNodeConnector.setWrite2IdFile(false);

    try {
      // 初始化底层Hadoop均衡器的NameNode连接器
      connector = new NameNodeConnector("DiskBalancer",
          clusterURI, DISKBALANCER_ID_PATH, null, conf, 1);
    } catch (IOException ex) {
      LOG.error("Unable to connect to NameNode " + ex.toString());
      throw ex;
    }

    this.clusterURI = clusterURI;
  }

  /**
   * 获取集群所有活数据节点的磁盘均衡模型对象列表，用于后续均衡计算
   * @return 所有活数据节点的磁盘均衡模型列表
   * @throws Exception 获取或解析NameNode数据失败时抛出
   */
  @Override
  public List<DiskBalancerDataNode> getNodes() throws Exception {
    Preconditions.checkNotNull(this.connector);
    List<DiskBalancerDataNode> nodeList = new LinkedList<>();
    // 从NameNode获取所有在线数据节点的存储报告
    DatanodeStorageReport[] reports = this.connector
        .getLiveDatanodeStorageReport();

    // 遍历所有数据节点报告，转换为磁盘均衡数据模型
    for (DatanodeStorageReport report : reports) {
      DiskBalancerDataNode datanode = getBalancerNodeFromDataNode(
          report.getDatanodeInfo());
      getVolumeInfoFromStorageReports(datanode, report.getStorageReports());
      nodeList.add(datanode);
    }
    return nodeList;
  }

  /**
   * 获取当前连接器的描述信息
   * @return 连接器描述字符串
   */
  @Override
  public String getConnectorInfo() {
    return "Name Node Connector : " + clusterURI.toString();
  }

  /**
   * 将HDFS原生DatanodeInfo转换为磁盘均衡专用的DiskBalancerDataNode模型，提取核心信息
   * @param nodeInfo HDFS原生数据节点信息
   * @return 磁盘均衡专用数据节点模型
   */
  private DiskBalancerDataNode
      getBalancerNodeFromDataNode(DatanodeInfo nodeInfo) {
    Preconditions.checkNotNull(nodeInfo);
    DiskBalancerDataNode dbDataNode = new DiskBalancerDataNode(nodeInfo
        .getDatanodeUuid());
    dbDataNode.setDataNodeIP(nodeInfo.getIpAddr());
    dbDataNode.setDataNodeName(nodeInfo.getHostName());
    dbDataNode.setDataNodePort(nodeInfo.getIpcPort());
    return dbDataNode;
  }

  /**
   * 从存储报告中解析每个卷的容量、状态等信息，填充到磁盘均衡数据节点模型中
   * @param node 待填充的磁盘均衡数据节点对象
   * @param reports 数据节点的存储报告数组
   * @throws Exception 参数校验失败时抛出
   */
  private void getVolumeInfoFromStorageReports(DiskBalancerDataNode node,
                                               StorageReport[] reports)
      throws Exception {
    Preconditions.checkNotNull(node);
    Preconditions.checkNotNull(reports);
    // 遍历每个存储卷，提取信息并处理跳过逻辑
    for (StorageReport report : reports) {
      DatanodeStorage storage = report.getStorage();
      DiskBalancerVolume volume = new DiskBalancerVolume();
      volume.setCapacity(report.getCapacity());
      volume.setFailed(report.isFailed());
      volume.setUsed(report.getDfsUsed());

      // TODO : Should we do BlockPool level balancing at all ?
      // Does it make sense ? Balancer does do that. Right now
      // we only deal with volumes and not blockPools

      volume.setUuid(storage.getStorageID());

      // 只读共享卷和已故障卷跳过均衡，因为无法写入或移动数据
      volume.setSkip((storage.getState() == DatanodeStorage.State
          .READ_ONLY_SHARED) || report.isFailed());
      volume.setStorageType(storage.getStorageType().name());
      volume.setIsTransient(storage.getStorageType().isTransient());
      node.addVolume(volume);
    }

  }
}