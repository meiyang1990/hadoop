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

package org.apache.hadoop.yarn.server.resourcemanager.nodelabels;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.yarn.api.records.NodeAttribute;
import org.apache.hadoop.yarn.api.records.NodeLabel;

/**
 * 节点标签工具类，提供节点标签相关的通用转换与验证工具方法
 */
public final class NodeLabelsUtils {
  private static final Logger LOG =
      LoggerFactory.getLogger(NodeLabelsUtils.class);

  private NodeLabelsUtils() { /* Hidden constructor */ }

  /**
   * 将NodeLabel对象集合转换为标签名称字符串集合
   * @param nodeLabels 输入的NodeLabel对象集合
   * @return 标签名称字符串集合，输入为null时返回null
   */
  public static Set<String> convertToStringSet(Set<NodeLabel> nodeLabels) {
    if (null == nodeLabels) {
      return null;
    }
    Set<String> labels = new HashSet<String>();
    for (NodeLabel label : nodeLabels) {
      labels.add(label.getName());
    }
    return labels;
  }

  /**
   * 验证集中式节点标签配置是否已启用，未启用则抛出异常
   * @param operation 当前执行的操作名称，用于错误日志
   * @param isCentralizedNodeLabelConfiguration 集中式配置是否启用标识
   * @throws IOException 当集中式配置未启用时抛出IO异常
   */
  public static void verifyCentralizedNodeLabelConfEnabled(String operation,
      boolean isCentralizedNodeLabelConfiguration) throws IOException {
    if (!isCentralizedNodeLabelConfiguration) {
      String msg =
          String.format("Error when invoke method=%s because "
              + "centralized node label configuration is not enabled.",
              operation);
      LOG.error(msg);
      throw new IOException(msg);
    }
  }

  /**
   * 根据给定属性名称集合，从集群节点属性中过滤出匹配的节点属性
   *
   * @param attributeNames 需要匹配的节点属性名称集合
   * @param clusterNodeAttributes 集群全部节点属性集合
   * @return 匹配到的节点属性集合
   */
  public static Set <NodeAttribute> getNodeAttributesByName(
      Set<String> attributeNames, Set<NodeAttribute> clusterNodeAttributes) {
    return clusterNodeAttributes.stream()
        .filter(attribute -> attributeNames
            .contains(attribute.getAttributeKey().getAttributeName()))
        .collect(Collectors.toSet());
  }
}