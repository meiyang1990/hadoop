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

import java.io.IOException;
import java.util.Set;
import java.util.TimerTask;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.NodeLabel;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

/**
 * 基于外部脚本实现的节点标签提供者，通过执行用户配置的脚本获取当前节点的标签信息。
 * 脚本输出中以 "NODE_PARTITION:" 开头的行会被识别为节点分区标签。
 */
public class ScriptBasedNodeLabelsProvider extends NodeLabelsProvider {

  /** 脚本输出中匹配节点分区标签的前缀模式 */
  public static final String NODE_LABEL_PARTITION_PATTERN = "NODE_PARTITION:";

  private NodeDescriptorsScriptRunner runner;

  public ScriptBasedNodeLabelsProvider() {
    super(ScriptBasedNodeLabelsProvider.class.getName());
  }

  /*
   * Method which initializes the values for the script path and interval time.
   */
  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    // 从配置中获取脚本路径
    String nodeLabelsScriptPath =
        conf.get(YarnConfiguration.NM_SCRIPT_BASED_NODE_LABELS_PROVIDER_PATH);
    // 从配置中获取脚本执行超时时间，使用默认值兜底
    long scriptTimeout =
        conf.getLong(YarnConfiguration.NM_NODE_LABELS_PROVIDER_FETCH_TIMEOUT_MS,
            YarnConfiguration.DEFAULT_NM_NODE_LABELS_PROVIDER_FETCH_TIMEOUT_MS);
    // 从配置中获取脚本参数
    String[] scriptArgs = conf.getStrings(
        YarnConfiguration.NM_SCRIPT_BASED_NODE_LABELS_PROVIDER_SCRIPT_OPTS,
        new String[] {});
    // 校验脚本配置正确性
    verifyConfiguredScript(nodeLabelsScriptPath);

    // 从配置中获取脚本执行间隔，使用默认值兜底
    long taskInterval = conf.getLong(
        YarnConfiguration.NM_NODE_LABELS_PROVIDER_FETCH_INTERVAL_MS,
        YarnConfiguration.DEFAULT_NM_NODE_LABELS_PROVIDER_FETCH_INTERVAL_MS);
    // 设置标签刷新间隔
    this.setIntervalTime(taskInterval);
    // 初始化节点标签脚本执行器
    this.runner = new NodeLabelScriptRunner(nodeLabelsScriptPath, scriptArgs,
            scriptTimeout, this);

    super.serviceInit(conf);
  }

  /**
   * 清理脚本执行资源，终止标签获取脚本。
   */
  @Override
  public void cleanUp() {
    if (runner != null) {
      runner.cleanUp();
    }
  }

  /**
   * 定期执行脚本获取节点标签，并更新提供者的标签信息。
   */
  private static class NodeLabelScriptRunner extends
      NodeDescriptorsScriptRunner<NodeLabel> {

    NodeLabelScriptRunner(String scriptPath, String[] scriptArgs,
        long scriptTimeout, ScriptBasedNodeLabelsProvider provider) {
      super(scriptPath, scriptArgs, scriptTimeout, provider);
    }

    /**
     * 解析脚本输出，提取节点分区标签。
     *
     * @param scriptOutput 脚本执行输出字符串
     * @return 解析得到的节点标签集合
     * @throws IOException 解析失败时抛出
     */
    @Override
    Set<NodeLabel> parseOutput(String scriptOutput)
        throws IOException {
      String nodePartitionLabel = null;
      // 按行分割脚本输出
      String[] splits = scriptOutput.split("\n");
      // 遍历每一行输出寻找匹配前缀的标签
      for (String line : splits) {
        String trimmedLine = line.trim();
        if (trimmedLine.startsWith(NODE_LABEL_PARTITION_PATTERN)) {
          // 提取前缀后的标签内容
          nodePartitionLabel =
              trimmedLine.substring(NODE_LABEL_PARTITION_PATTERN.length());
        }
      }
      // 将提取到的标签转换为标准NodeLabel集合返回
      return convertToNodeLabelSet(nodePartitionLabel);
    }
  }

  @Override
  public TimerTask createTimerTask() {
    return runner;
  }
}