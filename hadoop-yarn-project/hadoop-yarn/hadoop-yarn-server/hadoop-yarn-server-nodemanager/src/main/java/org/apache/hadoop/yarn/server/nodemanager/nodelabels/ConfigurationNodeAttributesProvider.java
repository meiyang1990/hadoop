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

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.base.Strings;
import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableSet;
import org.apache.commons.lang3.EnumUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.NodeAttribute;
import org.apache.hadoop.yarn.api.records.NodeAttributeType;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.nodelabels.NodeLabelUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.TimerTask;
import java.util.Set;

/**
 * 基于配置文件的节点属性提供者，从Yarn配置中读取当前NodeManager节点的属性信息
 */
public class ConfigurationNodeAttributesProvider
    extends NodeAttributesProvider {

  private static final Logger LOG =
      LoggerFactory.getLogger(ConfigurationNodeAttributesProvider.class);

  // 多个节点属性之间的分隔符
  private static final String NODE_ATTRIBUTES_DELIMITER = ":";
  // 单个节点属性内部字段之间的分隔符
  private static final String NODE_ATTRIBUTE_DELIMITER = ",";

  /**
   * 构造基于配置的节点属性提供者
   */
  public ConfigurationNodeAttributesProvider() {
    super("Configuration Based Node Attributes Provider");
  }

  @Override
  /**
   * 初始化服务，从配置读取属性更新间隔，调用父类初始化
   */
  protected void serviceInit(Configuration conf) throws Exception {
    // 读取属性拉取间隔配置，使用默认值作为兜底
    long taskInterval = conf.getLong(YarnConfiguration
            .NM_NODE_ATTRIBUTES_PROVIDER_FETCH_INTERVAL_MS,
        YarnConfiguration
            .DEFAULT_NM_NODE_ATTRIBUTES_PROVIDER_FETCH_INTERVAL_MS);
    // 设置定时任务拉取间隔
    this.setIntervalTime(taskInterval);
    super.serviceInit(conf);
  }

  /**
   * 从配置文件更新节点属性，解析配置后更新提供者的描述符
   */
  private void updateNodeAttributesFromConfig(Configuration conf)
      throws IOException {
    // 读取配置中定义的节点属性字符串
    String configuredNodeAttributes = conf.get(
        YarnConfiguration.NM_PROVIDER_CONFIGURED_NODE_ATTRIBUTES, null);
    // 解析属性并更新到提供者
    setDescriptors(parseAttributes(configuredNodeAttributes));
  }

  @VisibleForTesting
  /**
   * 解析配置字符串生成节点属性集合
   * @param config 配置字符串，格式为多个属性用:分隔，单个属性用,分隔三个字段：名称,类型,值
   * @return 解析后的节点属性不可变集合
   * @throws IOException 配置格式错误时抛出异常
   */
  public Set<NodeAttribute> parseAttributes(String config)
      throws IOException {
    if (Strings.isNullOrEmpty(config)) {
      return ImmutableSet.of();
    }
    Set<NodeAttribute> attributeSet = new HashSet<>();
    // Configuration value should be in one line, format:
    // "ATTRIBUTE_NAME,ATTRIBUTE_TYPE,ATTRIBUTE_VALUE",
    // multiple node-attributes are delimited by ":".
    // Each attribute str should not container any space.
    // 按分隔符拆分多个属性
    String[] attributeStrs = config.split(NODE_ATTRIBUTES_DELIMITER);
    // 遍历每个属性字符串进行解析
    for (String attributeStr : attributeStrs) {
      // 拆分出名称、类型、值三个字段
      String[] fields = attributeStr.split(NODE_ATTRIBUTE_DELIMITER);
      // 校验字段数量必须为3
      if (fields.length != 3) {
        throw new IOException("Invalid value for "
            + YarnConfiguration.NM_PROVIDER_CONFIGURED_NODE_ATTRIBUTES
            + "=" + config);
      }

      // We don't allow user config to overwrite our dist prefix,
      // so disallow any prefix set in the configuration.
      // 禁止用户在配置中指定前缀，前缀将自动添加
      if (fields[0].contains("/")) {
        throw new IOException("Node attribute set in "
            + YarnConfiguration.NM_PROVIDER_CONFIGURED_NODE_ATTRIBUTES
            + " should not contain any prefix.");
      }

      // Make sure attribute type is valid.
      // 校验属性类型是否为合法枚举值
      if (!EnumUtils.isValidEnum(NodeAttributeType.class, fields[1])) {
        throw new IOException("Invalid node attribute type: "
            + fields[1] + ", valid values are "
            + Arrays.asList(NodeAttributeType.values()));
      }

      // Automatically setup prefix for collected attributes
      // 自动添加分布式前缀，创建节点属性实例
      NodeAttribute na = NodeAttribute.newInstance(
          NodeAttribute.PREFIX_DISTRIBUTED,
          fields[0],
          NodeAttributeType.valueOf(fields[1]),
          fields[2]);

      // Since a NodeAttribute is identical with another one as long as
      // their prefix and name are same, to avoid attributes getting
      // overwritten by ambiguous attribute, make sure it fails in such
      // case.
      // 检查是否存在重复属性，重复则抛出异常
      if (!attributeSet.add(na)) {
        throw new IOException("Ambiguous node attribute is found: "
            + na.toString() + ", a same attribute already exists");
      }
    }

    // Before updating the attributes to the provider,
    // verify if they are valid
    // 调用通用校验工具验证所有属性合法性
    try {
      NodeLabelUtil.validateNodeAttributes(attributeSet);
    } catch (IOException e) {
      throw new IOException("Node attributes set by configuration property: "
          + YarnConfiguration.NM_PROVIDER_CONFIGURED_NODE_ATTRIBUTES
          + " is not valid. Detail message: " + e.getMessage());
    }
    return attributeSet;
  }

  /**
   * 定时刷新配置的定时器任务，定期从配置文件重新加载节点属性
   */
  private class ConfigurationMonitorTimerTask extends TimerTask {
    @Override
    public void run() {
      try {
        // 重新加载Yarn配置，更新节点属性
        updateNodeAttributesFromConfig(new YarnConfiguration());
      } catch (Exception e) {
        // 加载失败记录错误日志
        LOG.error("Failed to update node attributes from "
            + YarnConfiguration.NM_PROVIDER_CONFIGURED_NODE_ATTRIBUTES, e);
      }
    }
  }

  @Override
  /**
   * 清理资源，本实现无需清理操作
   */
  protected void cleanUp() throws Exception {
    // Nothing to cleanup
  }

  @Override
  /**
   * 创建定时刷新任务实例
   * @return 配置监控定时器任务
   */
  public TimerTask createTimerTask() {
    return new ConfigurationMonitorTimerTask();
  }
}