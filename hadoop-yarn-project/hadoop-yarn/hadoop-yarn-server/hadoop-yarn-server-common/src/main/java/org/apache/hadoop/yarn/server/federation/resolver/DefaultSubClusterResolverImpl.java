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

package org.apache.hadoop.yarn.server.federation.resolver;

import java.io.BufferedReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 默认子集群和机架解析器实现类。
 * 
 * 该类从配置指定的逗号分隔文件加载节点-子集群-机架映射关系，
 * 文件路径由配置项 yarn.federation.machine-list 指定，每行格式为：
 * nodeName, subClusterId, rackName
 * 
 * 不符合格式的行会被忽略，映射关系仅在调用load()方法时加载一次，
 * 不支持文件变更后动态刷新。对节点名和机架名大小写不敏感，
 * 自动忽略首尾空白字符。
 * 
 * 用于联邦YARN环境中，根据节点名或机架名解析对应的子集群ID。
 */
public class DefaultSubClusterResolverImpl extends AbstractSubClusterResolver
    implements SubClusterResolver {

  private static final Logger LOG =
      LoggerFactory.getLogger(DefaultSubClusterResolverImpl.class);
  private Configuration conf;

  // 节点主机名在机器信息文件中的列索引
  private static final int NODE_NAME_INDEX = 0;

  // 子集群ID在机器信息文件中的列索引
  private static final int SUBCLUSTER_ID_INDEX = 1;

  // 机架名在机器信息文件中的列索引
  private static final int RACK_NAME_INDEX = 2;

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public Configuration getConf() {
    return this.conf;
  }

  @Override
  public SubClusterId getSubClusterForNode(String nodename)
      throws YarnException {
    // 统一转为大写后调用父类查询方法，实现大小写不敏感匹配
    return super.getSubClusterForNode(nodename.toUpperCase());
  }

  @Override
  public void load() {
    // 从配置中获取机器列表文件路径
    String fileName =
        this.conf.get(YarnConfiguration.FEDERATION_MACHINE_LIST, "");

    try {
      if (fileName == null || fileName.trim().length() == 0) {
        LOG.info(
            "The machine list file path is not specified in the configuration");
        return;
      }

      Path file = null;
      BufferedReader reader = null;

      try {
        // 解析文件路径
        file = Paths.get(fileName);
      } catch (InvalidPathException e) {
        LOG.info("The configured machine list file path {} does not exist",
            fileName);
        return;
      }

      try {
        // 打开UTF-8编码的文件读取流
        reader = Files.newBufferedReader(file, StandardCharsets.UTF_8);
        String line = null;
        // 逐行读取文件
        while ((line = reader.readLine()) != null) {
          // 按逗号分割列
          String[] tokens = line.split(",");
          // 只处理格式正确（3列）的行
          if (tokens.length == 3) {

            // 处理节点名：去除首尾空白，转为大写
            String nodeName = tokens[NODE_NAME_INDEX].trim().toUpperCase();
            // 创建子集群ID对象
            SubClusterId subClusterId =
                SubClusterId.newInstance(tokens[SUBCLUSTER_ID_INDEX].trim());
            // 处理机架名：去除首尾空白，转为大写
            String rackName = tokens[RACK_NAME_INDEX].trim().toUpperCase();

            if (LOG.isDebugEnabled()) {
              LOG.debug("Loading node into resolver: {} --> {}", nodeName,
                  subClusterId);
              LOG.debug("Loading rack into resolver: {} --> {} ", rackName,
                  subClusterId);
            }

            // 存入节点->子集群映射缓存
            this.getNodeToSubCluster().put(nodeName, subClusterId);
            // 存入机架->子集群集合映射缓存
            loadRackToSubCluster(rackName, subClusterId);
          } else {
            // 格式错误的行记录警告并跳过
            LOG.warn("Skipping malformed line in machine list: " + line);
          }
        }
      } finally {
        // 关闭文件读取流
        if (reader != null) {
          reader.close();
        }
      }
      LOG.info("Successfully loaded file {}", fileName);

    } catch (Exception e) {
      LOG.error("Failed to parse file " + fileName, e);
    }
  }

  /**
   * 加载机架与子集群的映射关系到缓存。
   * @param rackName 处理后的机架名
   * @param subClusterId 对应的子集群ID
   */
  private void loadRackToSubCluster(String rackName,
      SubClusterId subClusterId) {
    String rackNameUpper = rackName.toUpperCase();

    // 机架不存在则初始化空集合
    if (!this.getRackToSubClusters().containsKey(rackNameUpper)) {
      this.getRackToSubClusters().put(rackNameUpper,
          new HashSet<SubClusterId>());
    }

    // 将子集群ID添加到机架对应的集合中
    this.getRackToSubClusters().get(rackNameUpper).add(subClusterId);

  }

  @Override
  public Set<SubClusterId> getSubClustersForRack(String rackname)
      throws YarnException {
    // 统一转为大写后调用父类查询方法，实现大小写不敏感匹配
    return super.getSubClustersForRack(rackname.toUpperCase());
  }
}