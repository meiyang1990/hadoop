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

package org.apache.hadoop.hdfs.server.diskbalancer.planner;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.util.Preconditions;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.hdfs.DFSConfigKeys.SUPPORTED_PACKAGES_CONFIG_NAME;

/**
 * 数据节点磁盘均衡计划，保存单个DataNode上所有卷组的均衡移动步骤列表。
 * 支持JSON序列化/反序列化，用于在规划器和执行器之间传递均衡计划。
 */
public class NodePlan {
  @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS,
      include = JsonTypeInfo.As.PROPERTY, property = "@class")
  private List<Step> volumeSetPlans;
  private String nodeName;
  private String nodeUUID;
  private int port;
  private long timeStamp;

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final ObjectReader READER = MAPPER.readerFor(NodePlan.class);
  private static final ObjectWriter WRITER = MAPPER.writerFor(
      MAPPER.constructType(NodePlan.class));
  private static final Configuration CONFIGURATION = new HdfsConfiguration();
  private static final Collection<String> SUPPORTED_PACKAGES = getAllowedPackages();

  /**
   * 获取计划创建时间戳。
   *
   * @return 计划创建的时间戳（毫秒）
   */
  public long getTimeStamp() {
    return timeStamp;
  }

  /**
   * 设置计划创建时间戳。
   *
   * @param timeStamp 计划创建的时间戳（毫秒）
   */
  public void setTimeStamp(long timeStamp) {
    this.timeStamp = timeStamp;
  }

  /**
   * 构造空的节点均衡计划。
   */
  public NodePlan() {
    volumeSetPlans = new LinkedList<>();
  }

  /**
   * 构造指定DataNode的空节点均衡计划。
   *
   * @param datanodeName DataNode地址名称
   * @param rpcPort DataNode RPC服务端口
   */
  public NodePlan(String datanodeName, int rpcPort) {
    volumeSetPlans = new LinkedList<>();
    this.nodeName = datanodeName;
    this.port = rpcPort;
  }

  /**
   * 获取当前节点所有均衡步骤列表。
   *
   * @return 均衡步骤列表
   */
  public List<Step> getVolumeSetPlans() {
    return volumeSetPlans;
  }

  /**
   * 向当前计划添加一个均衡步骤。
   *
   * @param nextStep 待添加的均衡步骤
   */
  void addStep(Step nextStep) {
    Preconditions.checkNotNull(nextStep);
    volumeSetPlans.add(nextStep);
  }

  /**
   * 设置DataNode节点名称。
   *
   * @param nodeName DataNode节点名称
   */
  public void setNodeName(String nodeName) {
    this.nodeName = nodeName;
  }

  /**
   * 设置均衡步骤列表。
   *
   * @param volumeSetPlans 均衡步骤列表
   */
  public void setVolumeSetPlans(List<Step> volumeSetPlans) {
    this.volumeSetPlans = volumeSetPlans;
  }

  /**
   * 获取DataNode节点名称。
   *
   * @return DataNode节点名称
   */
  public String getNodeName() {
    return nodeName;
  }

  /**
   * 设置DataNode URI地址。
   *
   * @param dataNodeName DataNode URI地址
   */
  public void setURI(String dataNodeName) {
    this.nodeName = dataNodeName;
  }

  /**
   * 获取DataNode RPC服务端口。
   *
   * @return RPC端口号
   */
  public int getPort() {
    return port;
  }

  /**
   * 设置DataNode RPC服务端口。
   *
   * @param port RPC端口号
   */
  public void setPort(int port) {
    this.port = port;
  }

  /**
   * 从JSON字符串解析生成NodePlan对象。
   * 会对JSON中的类信息进行安全校验，只允许加载配置允许的包中的类。
   *
   * @param json 待解析的JSON字符串
   * @return 解析完成的NodePlan对象
   * @throws IOException 解析失败或包含非法类信息时抛出异常
   */
  public static NodePlan parseJson(String json) throws IOException {
    JsonNode tree = READER.readTree(json);
    checkNodes(tree);
    return READER.readValue(tree);
  }

  /**
   * 递归遍历JSON树，检查所有@class属性对应的类是否在允许的包范围内，
   * 防止反序列化不受信任类带来的安全风险。
   *
   * @param node 根JSON节点
   * @throws IOException 发现不允许的类时抛出异常
   */
  private static void checkNodes(JsonNode node) throws IOException {
    if (node == null) {
      return;
    }

    // 如果是对象节点，遍历所有字段检查
    if (node.isObject()) {
      Iterator<Map.Entry<String, JsonNode>> fieldsIterator = node.fields();
      while (fieldsIterator.hasNext()) {
        Map.Entry<String, JsonNode> entry = fieldsIterator.next();
        // 检查@class类型属性是否合法
        if ("@class".equals(entry.getKey())) {
          String textValue = entry.getValue().asText();
          if (textValue != null && !textValue.isBlank() && !stepClassIsAllowed(textValue)) {
            throw new IOException("Invalid @class value in NodePlan JSON: " + textValue);
          }
        }
        // 递归检查当前字段值
        checkNodes(entry.getValue());
      }
    } else if (node.isArray()) {
      // 如果是数组节点，遍历每个元素递归检查
      for (int i = 0; i < node.size(); i++) {
        checkNodes(node.get(i));
      }
    }
  }

  /**
   * 将当前NodePlan对象序列化为JSON字符串。
   *
   * @return JSON格式字符串
   * @throws IOException 序列化失败时抛出异常
   */
  public String toJson() throws IOException {
    return WRITER.writeValueAsString(this);
  }

  /**
   * 获取DataNode的UUID。
   *
   * @return DataNode UUID
   */
  public String getNodeUUID() {
    return nodeUUID;
  }

  /**
   * 设置DataNode的UUID。
   *
   * @param nodeUUID DataNode UUID
   */
  public void setNodeUUID(String nodeUUID) {
    this.nodeUUID = nodeUUID;
  }

  /**
   * 检查指定类名是否在允许反序列化的包范围内。
   *
   * @param className 待检查的完整类名
   * @return 允许返回true，否则返回false
   */
  private static boolean stepClassIsAllowed(String className) {
    for (String pkg : SUPPORTED_PACKAGES) {
      if (className.startsWith(pkg)) {
        return true;
      }
    }
    return false;
  }

  /**
   * 从配置中加载允许反序列化的包列表。
   *
   * @return 允许的包名集合
   */
  private static Collection<String> getAllowedPackages() {
    return CONFIGURATION.getStringCollection(SUPPORTED_PACKAGES_CONFIG_NAME)
        .stream()
        .map(String::trim)
        .filter(s -> !s.isEmpty())
        .toList();
  }
}