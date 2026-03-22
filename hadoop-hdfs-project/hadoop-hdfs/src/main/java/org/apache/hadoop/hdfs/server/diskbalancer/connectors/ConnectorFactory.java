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
 * distributed under the License is distributed on an "AS IS" BASIS WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.hdfs.server.diskbalancer.connectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * 文件级注释：磁盘均衡器连接器工厂，根据URI类型创建对应类型的集群连接器，用于获取集群拓扑数据。
 * 支持从本地JSON文件和NameNode两种来源获取集群信息，工厂模式的实现。
 */
public final class ConnectorFactory {
  private static final Logger LOG =
      LoggerFactory.getLogger(ConnectorFactory.class);

  /**
   * 根据集群URI创建对应类型的集群连接器，用于对接不同来源的集群信息。
   * @param clusterURI 集群信息资源URI
   * @param conf Hadoop配置对象
   * @return 对应类型的集群连接器实例
   * @throws IOException IO读取异常
   * @throws URISyntaxException URI格式异常
   */
  public static ClusterConnector getCluster(URI clusterURI, Configuration
      conf) throws IOException, URISyntaxException {
    // 打印调试日志：输出完整集群URI
    LOG.debug("Cluster URI : {}" , clusterURI);
    // 打印调试日志：输出URI的协议类型
    LOG.debug("scheme : {}" , clusterURI.getScheme());
    // 如果协议是文件协议，创建本地JSON文件连接器用于读取离线集群信息
    if (clusterURI.getScheme().startsWith("file")) {
      LOG.debug("Creating a JsonNodeConnector");
      return new JsonNodeConnector(clusterURI.toURL());
    } else {
      // 其他协议创建NameNode连接器，从运行中的NameNode获取在线集群信息
      LOG.debug("Creating NameNode connector");
      return new DBNameNodeConnector(clusterURI, conf);
    }
  }

  /**
   * 工具类私有构造方法，禁止实例化工厂类。
   */
  private ConnectorFactory() {
    // never constructed
  }
}