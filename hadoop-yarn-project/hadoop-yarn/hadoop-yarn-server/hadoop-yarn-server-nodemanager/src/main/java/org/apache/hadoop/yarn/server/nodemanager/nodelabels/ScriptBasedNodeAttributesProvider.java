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
package org.apache.hadoop.yarn.server.nodemanager.nodelabels;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.NodeAttribute;
import org.apache.hadoop.yarn.api.records.NodeAttributeType;
import org.apache.hadoop.yarn.nodelabels.NodeLabelUtil;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.TimerTask;

import static org.apache.hadoop.yarn.conf.YarnConfiguration
    .NM_SCRIPT_BASED_NODE_ATTRIBUTES_PROVIDER_PATH;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.
    NM_SCRIPT_BASED_NODE_ATTRIBUTES_PROVIDER_OPTS;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.
    NM_NODE_ATTRIBUTES_PROVIDER_FETCH_INTERVAL_MS;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.
    NM_NODE_ATTRIBUTES_PROVIDER_FETCH_TIMEOUT_MS;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.
    DEFAULT_NM_NODE_ATTRIBUTES_PROVIDER_FETCH_INTERVAL_MS;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.
    DEFAULT_NM_NODE_ATTRIBUTES_PROVIDER_FETCH_TIMEOUT_MS;

/**
 * 基于外部脚本实现的节点属性提供者，定期执行自定义脚本采集节点属性。
 * 属于YARN NodeManager节点标签模块，支持用户通过自定义脚本动态上报节点属性。
 */
public class ScriptBasedNodeAttributesProvider extends NodeAttributesProvider{

  // 节点属性输出行前缀标记
  private static final String NODE_ATTRIBUTE_PATTERN = "NODE_ATTRIBUTE:";
  // 节点属性字段分隔符
  private static final String NODE_ATTRIBUTE_DELIMITER = ",";

  // 脚本执行器实例
  private NodeAttributeScriptRunner runner;

  /**
   * 构造函数，初始化脚本-based节点属性提供者。
   */
  public ScriptBasedNodeAttributesProvider() {
    super(ScriptBasedNodeAttributesProvider.class.getName());
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    super.serviceInit(conf);
    // 从配置中读取脚本路径
    String nodeAttributeProviderScript = conf.get(
        NM_SCRIPT_BASED_NODE_ATTRIBUTES_PROVIDER_PATH);
    // 从配置中读取脚本执行超时时间
    long scriptTimeout = conf.getLong(
        NM_NODE_ATTRIBUTES_PROVIDER_FETCH_TIMEOUT_MS,
        DEFAULT_NM_NODE_ATTRIBUTES_PROVIDER_FETCH_TIMEOUT_MS);
    // 从配置中读取脚本参数
    String[] scriptArgs = conf.getStrings(
        NM_SCRIPT_BASED_NODE_ATTRIBUTES_PROVIDER_OPTS,
        new String[] {});
    // 验证脚本配置有效性
    verifyConfiguredScript(nodeAttributeProviderScript);

    // 从配置中读取属性采集间隔时间
    long intervalTime = conf.getLong(
        NM_NODE_ATTRIBUTES_PROVIDER_FETCH_INTERVAL_MS,
        DEFAULT_NM_NODE_ATTRIBUTES_PROVIDER_FETCH_INTERVAL_MS);
    // 设置采集间隔
    this.setIntervalTime(intervalTime);

    // 初始化脚本执行器
    this.runner = new NodeAttributeScriptRunner(nodeAttributeProviderScript,
        scriptArgs, scriptTimeout, this);
  }

  @Override
  protected void cleanUp() throws Exception {
    // 清理脚本执行器资源
    runner.cleanUp();
  }

  @Override
  public TimerTask createTimerTask() {
    // 返回用于定时执行的任务实例
    return runner;
  }

  /**
   * 节点属性脚本执行器，负责执行用户脚本并解析输出结果。
   */
  private static class NodeAttributeScriptRunner extends
      NodeDescriptorsScriptRunner<NodeAttribute> {

    NodeAttributeScriptRunner(String scriptPath, String[] scriptArgs,
        long scriptTimeout, ScriptBasedNodeAttributesProvider provider) {
      super(scriptPath, scriptArgs, scriptTimeout, provider);
    }

    @Override
    Set<NodeAttribute> parseOutput(String scriptOutput) throws IOException {
      Set<NodeAttribute> attributeSet = new HashSet<>();
      // TODO finalize format

      // 按行分割脚本输出
      String[] splits = scriptOutput.split("\n");
      // 遍历每一行输出
      for (String line : splits) {
        String trimmedLine = line.trim();
        // 仅处理带有NODE_ATTRIBUTE前缀的行
        if (trimmedLine.startsWith(NODE_ATTRIBUTE_PATTERN)) {
          // 截取前缀之后的属性内容
          String nodeAttribute = trimmedLine
              .substring(NODE_ATTRIBUTE_PATTERN.length());
          // 按分隔符拆分属性字段
          String[] attributeStrs = nodeAttribute
              .split(NODE_ATTRIBUTE_DELIMITER);
          // 校验字段数量必须为3（名称、类型、值）
          if (attributeStrs.length != 3) {
            throw new IOException("Malformed output, expecting format "
                + NODE_ATTRIBUTE_PATTERN + ":" + "ATTRIBUTE_NAME"
                + NODE_ATTRIBUTE_DELIMITER + "ATTRIBUTE_TYPE"
                + NODE_ATTRIBUTE_DELIMITER + "ATTRIBUTE_VALUE; but get "
                + nodeAttribute);
          }

          // 禁止脚本中设置前缀，所有动态属性统一由系统添加分布式前缀
          if (attributeStrs[0].contains("/")) {
            throw new IOException("Node attributes reported by script"
                + " should not contain any prefix.");
          }

          // 自动添加分布式节点属性前缀，构造节点属性对象
          NodeAttribute na = NodeAttribute
              .newInstance(NodeAttribute.PREFIX_DISTRIBUTED,
                  attributeStrs[0],
                  NodeAttributeType.valueOf(attributeStrs[1]),
                  attributeStrs[2]);

          // 校验是否存在重复属性（前缀+名称唯一确定属性），重复则抛出异常避免歧义
          if (!attributeSet.add(na)) {
            throw new IOException("Ambiguous node attribute is found: "
                + na.toString() + ", a same attribute already exists");
          }
        }
      }

      // 更新到提供者前，校验所有采集到的节点属性合法性
      try {
        NodeLabelUtil.validateNodeAttributes(attributeSet);
      } catch (IOException e) {
        throw new IOException("Node attributes collected by the script "
            + "contains some invalidate entries. Detail message: "
            + e.getMessage());
      }
      return attributeSet;
    }
  }
}