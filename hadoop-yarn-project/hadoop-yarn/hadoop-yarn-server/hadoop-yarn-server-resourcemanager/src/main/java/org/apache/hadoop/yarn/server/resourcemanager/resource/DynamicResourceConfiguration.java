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

package org.apache.hadoop.yarn.server.resourcemanager.resource;

import java.io.InputStream;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceOption;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.resource.Resources;

/**
 * YARN RM 动态节点资源配置管理类，继承Hadoop Configuration，
 * 负责加载、存储和查询每个节点的动态资源配置信息，支持节点级资源覆盖配置。
 */
public class DynamicResourceConfiguration extends Configuration {

  private static final Logger LOG =
      LoggerFactory.getLogger(DynamicResourceConfiguration.class);

  @Private
  public static final String PREFIX = "yarn.resource.dynamic.";

  @Private
  public static final String DOT = ".";

  @Private
  public static final String NODES = "nodes";

  @Private
  public static final String VCORES = "vcores";

  @Private
  public static final String MEMORY = "memory";

  @Private
  public static final String OVERCOMMIT_TIMEOUT = "overcommittimeout";

  /**
   * 默认构造函数，使用空配置初始化。
   */
  public DynamicResourceConfiguration() {
    this(new Configuration());
  }

  /**
   * 使用已有配置初始化动态资源配置，加载默认动态配置文件。
   * @param configuration 基础配置对象
   */
  public DynamicResourceConfiguration(Configuration configuration) {
    super(configuration);
    // 添加动态资源配置文件到配置加载列表
    addResource(YarnConfiguration.DR_CONFIGURATION_FILE);
  }

  /**
   * 使用已有配置和输入流初始化动态资源配置，从输入流加载配置。
   * @param configuration 基础配置对象
   * @param drInputStream 动态资源配置输入流
   */
  public DynamicResourceConfiguration(Configuration configuration,
      InputStream drInputStream) {
    super(configuration);
    // 从输入流添加动态资源配置
    addResource(drInputStream);
  }

  /**
   * 生成指定节点的配置键前缀。
   * @param node 节点名称
   * @return 节点配置前缀字符串
   */
  private String getNodePrefix(String node) {
    String nodeName = PREFIX + node + DOT;
    return nodeName;
  }

  /**
   * 获取指定节点配置的vcore数量，不存在则返回默认值。
   * @param node 节点名称
   * @return 节点vcore数量
   */
  public int getVcoresPerNode(String node) {
    int vcoresPerNode =
      getInt(getNodePrefix(node) + VCORES,
        YarnConfiguration.DEFAULT_NM_VCORES);
    return vcoresPerNode;
  }

  /**
   * 设置指定节点的vcore数量。
   * @param node 节点名称
   * @param vcores 要设置的vcore数量
   */
  public void setVcoresPerNode(String node, int vcores) {
    setInt(getNodePrefix(node) + VCORES, vcores);
    LOG.debug("DRConf - setVcoresPerNode: nodePrefix={}, vcores={}",
        getNodePrefix(node), vcores);

  }

  /**
   * 获取指定节点配置的内存大小（单位MB），不存在则返回默认值。
   * @param node 节点名称
   * @return 节点内存大小（MB）
   */
  public int getMemoryPerNode(String node) {
    int memoryPerNode =
      getInt(getNodePrefix(node) + MEMORY,
        YarnConfiguration.DEFAULT_NM_PMEM_MB);
    return memoryPerNode;
  }

  /**
   * 设置指定节点的内存大小。
   * @param node 节点名称
   * @param memory 要设置的内存大小（MB）
   */
  public void setMemoryPerNode(String node, int memory) {
    setInt(getNodePrefix(node) + MEMORY, memory);
    LOG.debug("DRConf - setMemoryPerNode: nodePrefix={}, memory={}",
        getNodePrefix(node), memory);

  }

  /**
   * 获取指定节点的超配超时时间，不存在则返回默认值。
   * @param node 节点名称
   * @return 超配超时时间（毫秒）
   */
  public int getOverCommitTimeoutPerNode(String node) {
    int overCommitTimeoutPerNode =
      getInt(getNodePrefix(node) + OVERCOMMIT_TIMEOUT,
        ResourceOption.OVER_COMMIT_TIMEOUT_MILLIS_DEFAULT);
    return overCommitTimeoutPerNode;
  }

  /**
   * 设置指定节点的超配超时时间。
   * @param node 节点名称
   * @param overCommitTimeout 要设置的超配超时时间（毫秒）
   */
  public void setOverCommitTimeoutPerNode(String node, int overCommitTimeout) {
    setInt(getNodePrefix(node) + OVERCOMMIT_TIMEOUT, overCommitTimeout);
    LOG.debug("DRConf - setOverCommitTimeoutPerNode: nodePrefix={},"
        + " overCommitTimeout={}", getNodePrefix(node), overCommitTimeout);
  }

  /**
   * 获取配置了动态资源的所有节点列表。
   * @return 动态配置节点名称数组
   */
  public String[] getNodes() {
    String[] nodes = getStrings(PREFIX + NODES);
    return nodes;
  }

  /**
   * 设置配置了动态资源的节点列表。
   * @param nodes 动态配置节点名称数组
   */
  public void setNodes(String[] nodes) {
    set(PREFIX + NODES, StringUtils.arrayToString(nodes));
  }

  /**
   * 解析配置，生成所有动态配置节点的资源选项映射表。
   * @return NodeId到ResourceOption的映射表，包含每个节点的动态资源配置
   */
  public Map<NodeId, ResourceOption> getNodeResourceMap() {
    // 获取所有动态配置节点
    String[] nodes = getNodes();
    Map<NodeId, ResourceOption> resourceOptions
      = new HashMap<NodeId, ResourceOption> ();

    // 遍历每个节点，读取配置并转换为ResourceOption
    for (String node : nodes) {
      // 解析节点ID
      NodeId nid = NodeId.fromString(node);
      // 获取节点vcore配置
      int vcores = getVcoresPerNode(node);
      // 获取节点内存配置
      int memory = getMemoryPerNode(node);
      // 获取节点超配超时配置
      int overCommitTimeout = getOverCommitTimeoutPerNode(node);
      // 创建Resource对象
      Resource resource = Resources.createResource(memory, vcores);
      // 创建ResourceOption对象
      ResourceOption resourceOption =
          ResourceOption.newInstance(resource, overCommitTimeout);
      // 存入映射表
      resourceOptions.put(nid, resourceOption);
    }

    return resourceOptions;
  }
}