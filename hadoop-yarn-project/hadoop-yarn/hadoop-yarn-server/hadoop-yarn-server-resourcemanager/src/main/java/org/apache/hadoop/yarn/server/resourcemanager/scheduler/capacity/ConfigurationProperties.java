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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * 容量调度器配置属性前缀字典树存储，用于优化配置查询性能。
 * 配置键按点号'.'分割，每个分段创建一个节点，
 * 完整配置存储在倒数第二个分段对应的节点中，减少节点创建数量。
 * 示例：yarn.scheduler.capacity.root.max-applications 和 yarn.scheduler.capacity.root.state
 * 会创建4个节点：yarn - scheduler - capacity - root，两个配置都存储在root节点中。
 */
public class ConfigurationProperties {
  private static final Logger LOG =
      LoggerFactory.getLogger(ConfigurationProperties.class);

  private final Map<String, PrefixNode> nodes;
  private static final String DELIMITER = "\\.";

  /**
   * 构造函数，适配Hadoop Configuration类型调用，仅处理String类型键值对。
   * @param props 待存储的配置属性集合
   */
  public ConfigurationProperties(Map<String, String> props) {
    this.nodes = new HashMap<>();
    storePropertiesInPrefixNodes(props);
  }

  /**
   * 根据前缀筛选配置属性，返回结果会裁剪掉前缀部分。
   * @param prefix 筛选配置使用的前缀
   * @return 匹配前缀的配置集合，键已裁剪前缀
   */
  public Map<String, String> getPropertiesWithPrefix(String prefix) {
    return getPropertiesWithPrefix(prefix, false);
  }

  /**
   * 根据前缀筛选配置属性，支持选择是否保留完整键名。
   * @param prefix 筛选配置使用的前缀
   * @param fullyQualifiedKey 是否保留完整键名，false则裁剪前缀
   * @return 匹配前缀的配置集合
   */
  public Map<String, String> getPropertiesWithPrefix(
      String prefix, boolean fullyQualifiedKey) {
    // 将前缀按点号拆分为分段列表
    List<String> propertyPrefixParts = splitPropertyByDelimiter(prefix);
    Map<String, String> properties = new HashMap<>();
    String trimPrefix;
    if (fullyQualifiedKey) {
      trimPrefix = "";
    } else {
      // 处理末尾带点号的前缀，移除末尾点号保证裁剪逻辑正确
      trimPrefix = prefix.endsWith(CapacitySchedulerConfiguration.DOT) ?
          prefix.substring(0, prefix.length() - 1) : prefix;
    }

    // 递归遍历字典树收集匹配前缀的所有配置
    collectPropertiesRecursively(nodes, properties,
        propertyPrefixParts.iterator(), trimPrefix);

    return properties;
  }

  /**
   * 递归遍历字典树，收集所有匹配前缀的配置属性。
   * @param childNodes 当前层级的子节点集合
   * @param properties 收集结果存储容器
   * @param prefixParts 前缀拆分后的分段迭代器
   * @param trimPrefix 需要从结果键中裁剪的前缀字符串，空表示不裁剪
   */
  private void collectPropertiesRecursively(
      Map<String, PrefixNode> childNodes, Map<String, String> properties,
      Iterator<String> prefixParts, String trimPrefix) {
    if (prefixParts.hasNext()) {
      // 获取当前前缀分段
      String prefix = prefixParts.next();
      PrefixNode candidate = childNodes.get(prefix);

      if (candidate != null) {
        // 已经匹配完所有前缀分段，复制当前节点存储的配置
        if (!prefixParts.hasNext()) {
          copyProperties(properties, trimPrefix, candidate.getValues());
        }
        // 继续递归遍历子节点，收集更深层级的配置
        collectPropertiesRecursively(candidate.getChildren(), properties,
            prefixParts, trimPrefix);
      }
    } else {
      // 前缀遍历完成，遍历所有子节点，收集所有后代节点的配置
      for (Map.Entry<String, PrefixNode> child : childNodes.entrySet()) {
        copyProperties(properties, trimPrefix, child.getValue().getValues());
        collectPropertiesRecursively(child.getValue().getChildren(),
            properties, prefixParts, trimPrefix);
      }
    }
  }


  /**
   * 将节点中存储的配置复制到结果集合，处理前缀裁剪逻辑。
   * @param copyTo 结果存储容器
   * @param trimPrefix 需要从键中裁剪的前缀字符串
   * @param copyFrom 源节点存储的配置集合
   */
  private void copyProperties(
      Map<String, String> copyTo, String trimPrefix,
      Map<String, String> copyFrom) {
    for (Map.Entry<String, String> configEntry : copyFrom.entrySet()) {
      String key = configEntry.getKey();
      String prefixToTrim = trimPrefix;

      if (!trimPrefix.isEmpty()) {
        // 键不等于前缀时，需要补充点号保证裁剪正确（避免误裁分类似前缀）
        if (!key.equals(trimPrefix)) {
          prefixToTrim += CapacitySchedulerConfiguration.DOT;
        }
        // 裁剪前缀部分得到短键名
        key = configEntry.getKey().substring(prefixToTrim.length());
      }

      copyTo.put(key, configEntry.getValue());
    }
  }

  /**
   * 将输入配置批量存储到字典树结构中。
   * @param props 待存储的配置集合
   */
  private void storePropertiesInPrefixNodes(Map<String, String> props) {
    for (Map.Entry<String, String> prop : props.entrySet()) {
      // 将配置键按点号拆分为分段列表
      List<String> propertyKeyParts = splitPropertyByDelimiter(prop.getKey());
      if (!propertyKeyParts.isEmpty()) {
        // 查找或创建对应路径的叶子节点，存储配置
        PrefixNode node = findOrCreatePrefixNode(nodes,
            propertyKeyParts.iterator());
        node.getValues().put(prop.getKey(), prop.getValue());
      } else {
        LOG.warn("Empty configuration property, skipping...");
      }
    }
  }

  /**
   * 递归查找或创建对应配置键路径的节点，返回存储配置的叶子节点。
   * @param children 当前层级的子节点集合
   * @param propertyKeyParts 配置键拆分后的分段迭代器
   * @return 存储该配置的最终节点
   */
  private PrefixNode findOrCreatePrefixNode(
      Map<String, PrefixNode> children, Iterator<String> propertyKeyParts) {
    String prefix = propertyKeyParts.next();
    PrefixNode candidate = children.get(prefix);
    // 节点不存在则创建新节点
    if (candidate == null) {
      candidate = new PrefixNode();
      children.put(prefix, candidate);
    }

    // 已经遍历完所有分段，返回当前节点存储配置
    if (!propertyKeyParts.hasNext()) {
      return candidate;
    }

    // 继续递归创建/查找下一级节点
    return findOrCreatePrefixNode(candidate.getChildren(),
        propertyKeyParts);
  }

  private List<String> splitPropertyByDelimiter(String property) {
    return Arrays.asList(property.split(DELIMITER));
  }


  /**
   * 字典树前缀节点，存储当前前缀对应的配置和子节点。
   * 每个节点代表配置键的一个分段，values存储所有以此分段结尾的完整配置，
   * children存储下一级分段的子节点。
   */
  private static class PrefixNode {
    private final Map<String, String> values;
    private final Map<String, PrefixNode> children;

    PrefixNode() {
      this.values = new HashMap<>();
      this.children = new HashMap<>();
    }

    public Map<String, String> getValues() {
      return values;
    }

    public Map<String, PrefixNode> getChildren() {
      return children;
    }
  }
}