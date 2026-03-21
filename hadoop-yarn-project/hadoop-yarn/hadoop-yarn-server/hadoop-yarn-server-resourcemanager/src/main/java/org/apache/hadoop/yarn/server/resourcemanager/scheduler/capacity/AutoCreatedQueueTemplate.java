// 这个文件已经全部加上中文注释
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import org.apache.hadoop.classification.VisibleForTesting;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.AUTO_QUEUE_CREATION_V2_PREFIX;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueuePrefixes.getQueuePrefix;

/**
 * 自动创建队列模板处理器，存储并管理自动创建队列的模板配置
 * 为动态自动创建的队列提供默认配置模板，支持通用模板、叶子队列模板和父队列模板三种类型
 */
public class AutoCreatedQueueTemplate {
  // 通用自动队列模板配置前缀
  public static final String AUTO_QUEUE_TEMPLATE_PREFIX =
      AUTO_QUEUE_CREATION_V2_PREFIX + "template.";
  // 仅叶子队列自动模板配置前缀
  public static final String AUTO_QUEUE_LEAF_TEMPLATE_PREFIX =
      AUTO_QUEUE_CREATION_V2_PREFIX + "leaf-template.";
  // 仅父队列自动模板配置前缀
  public static final String AUTO_QUEUE_PARENT_TEMPLATE_PREFIX =
      AUTO_QUEUE_CREATION_V2_PREFIX + "parent-template.";

  // 通配符队列，表示匹配任意队列路径
  public static final String WILDCARD_QUEUE = "*";

  // 存储通用模板配置属性
  private final Map<String, String> templateProperties = new HashMap<>();
  // 存储仅叶子队列专属模板配置属性
  private final Map<String, String> leafOnlyProperties = new HashMap<>();
  // 存储仅父队列专属模板配置属性
  private final Map<String, String> parentOnlyProperties = new HashMap<>();

  /**
   * 构造函数，从配置中加载模板配置
   * @param configuration 容量调度器配置
   * @param queuePath 当前父队列路径
   */
  public AutoCreatedQueueTemplate(CapacitySchedulerConfiguration configuration,
                                  QueuePath queuePath) {
    setTemplateConfigEntries(configuration, queuePath);
  }

  @VisibleForTesting
  /**
   * 获取指定队列路径的自动队列模板配置前缀
   * @param queuePath 队列路径
   * @return 完整配置前缀
   */
  public static String getAutoQueueTemplatePrefix(QueuePath queuePath) {
    return getQueuePrefix(queuePath) + AUTO_QUEUE_TEMPLATE_PREFIX;
  }

  /**
   * 获取父队列指定的所有通用模板属性
   * @return 模板属性键值对
   */
  public Map<String, String> getTemplateProperties() {
    return templateProperties;
  }

  /**
   * 获取父队列指定的仅叶子队列专属模板属性
   * @return 模板属性键值对
   */
  public Map<String, String> getLeafOnlyProperties() {
    return leafOnlyProperties;
  }

  /**
   * 获取父队列指定的仅父队列专属模板属性
   * @return 模板属性键值对
   */
  public Map<String, String> getParentOnlyProperties() {
    return parentOnlyProperties;
  }

  /**
   * 根据父队列模板，为子队列设置通用和类型专属模板配置（默认父队列类型）
   * @param conf 目标配置对象
   * @param childQueuePath 子队列路径，用于生成配置前缀
   */
  public void setTemplateEntriesForChild(CapacitySchedulerConfiguration conf,
                                         QueuePath childQueuePath) {
    setTemplateEntriesForChild(conf, childQueuePath, false);
  }

  /**
   * 根据父队列模板，为子队列设置通用和类型专属模板配置
   * @param conf 目标配置对象
   * @param childQueuePath 子队列路径，用于生成配置前缀
   * @param isLeaf 是否为叶子队列，决定使用叶子还是父队列专属模板
   */
  public void setTemplateEntriesForChild(CapacitySchedulerConfiguration conf,
                                         QueuePath childQueuePath,
                                         boolean isLeaf) {
    // 根队列不应用模板，直接返回
    if (childQueuePath.isRoot()) {
      return;
    }

    // 获取配置对象中的全部属性
    ConfigurationProperties configurationProperties =
        conf.getConfigurationProperties();

    // 获取该子队列已经显式配置的所有属性，避免覆盖用户自定义配置
    Set<String> alreadySetProps = configurationProperties
        .getPropertiesWithPrefix(getQueuePrefix(childQueuePath)).keySet();

    // 根据队列类型选择对应的专属模板
    Map<String, String> queueTypeSpecificTemplates = parentOnlyProperties;
    if (isLeaf) {
      queueTypeSpecificTemplates = leafOnlyProperties;
    }

    // 先应用队列类型专属模板配置
    for (Map.Entry<String, String> entry :
        queueTypeSpecificTemplates.entrySet()) {
      // 用户已经显式配置的属性不覆盖
      if (alreadySetProps.contains(entry.getKey())) {
        continue;
      }
      // 设置模板配置到子队列
      conf.set(getQueuePrefix(childQueuePath) + entry.getKey(), entry.getValue());
    }

    // 再应用通用模板配置
    for (Map.Entry<String, String> entry : templateProperties.entrySet()) {
      // 用户已显式配置 或 已经被类型专属模板覆盖的属性不覆盖
      if (alreadySetProps.contains(entry.getKey())
          || queueTypeSpecificTemplates.containsKey(entry.getKey())) {
        continue;
      }
      // 设置模板配置到子队列
      conf.set(getQueuePrefix(childQueuePath) + entry.getKey(), entry.getValue());
    }
  }

  /**
   * 从配置中解析并存储模板配置属性，遵循优先级规则：
   * 精确匹配队列路径 > 通配符路径，显式配置 > 模板默认值
   * @param configuration 容量调度器配置
   * @param queuePath 当前父队列路径
   */
  private void setTemplateConfigEntries(CapacitySchedulerConfiguration configuration,
                                        QueuePath queuePath) {
    if (!queuePath.isInvalid()) {
      ConfigurationProperties configurationProperties =
          configuration.getConfigurationProperties();

      // 获取当前队列允许的最大自动创建队列深度
      int maxAutoCreatedQueueDepth = configuration
          .getMaximumAutoCreatedQueueDepth(queuePath);
      // 生成所有带通配符的队列路径（从精确匹配逐步添加通配符到上层），按优先级从高到低排序
      List<QueuePath> wildcardedQueuePaths =
          queuePath.getWildcardedQueuePaths(maxAutoCreatedQueueDepth);

      // 按优先级从高到低遍历所有通配路径，高优先级已经设置的模板不会被低优先级覆盖
      for (QueuePath templateQueuePath: wildcardedQueuePaths) {
        // 获取该通配路径下的所有配置属性
        Map<String, String> queueProps = configurationProperties
            .getPropertiesWithPrefix(getQueuePrefix(templateQueuePath));

        // 分类存储模板属性
        for (Map.Entry<String, String> entry : queueProps.entrySet()) {
          storeConfiguredTemplates(entry.getKey(), entry.getValue());
        }
      }
    }
  }

  /**
   * 将解析到的模板配置分类存储到对应的属性映射中，遵循高优先级优先原则
   * @param templateKey 模板配置键
   * @param templateValue 模板配置值
   */
  private void storeConfiguredTemplates(
      String templateKey, String templateValue) {
    String prefix = "";
    Map<String, String> properties = templateProperties;

    // 判断模板类型，分类存储
    if (templateKey.startsWith(AUTO_QUEUE_TEMPLATE_PREFIX)) {
      // 通用模板
      prefix = AUTO_QUEUE_TEMPLATE_PREFIX;
    } else if (templateKey.startsWith(AUTO_QUEUE_LEAF_TEMPLATE_PREFIX)) {
      // 叶子队列专属模板
      prefix = AUTO_QUEUE_LEAF_TEMPLATE_PREFIX;
      properties = leafOnlyProperties;
    } else if (templateKey.startsWith(
        AUTO_QUEUE_PARENT_TEMPLATE_PREFIX)) {
      // 父队列专属模板
      prefix = AUTO_QUEUE_PARENT_TEMPLATE_PREFIX;
      properties = parentOnlyProperties;
    }

    if (!prefix.isEmpty()) {
      // 裁剪掉模板前缀，保留实际配置键名
      String key = templateKey.substring(prefix.length());
      // 高优先级先存储，低优先级不会覆盖，使用putIfAbsent保证优先级
      properties.putIfAbsent(key, templateValue);
    }
  }
}