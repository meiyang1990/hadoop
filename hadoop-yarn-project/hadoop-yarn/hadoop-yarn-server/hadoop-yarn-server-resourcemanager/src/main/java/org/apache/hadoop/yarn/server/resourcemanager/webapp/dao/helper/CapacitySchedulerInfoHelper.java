// 这个文件已经全部加上中文注释
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.helper;

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.AbstractCSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.AbstractLeafQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.AbstractParentQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.AutoCreatedLeafQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.ManagedParentQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.ParentQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacityVector;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AutoQueueTemplatePropertiesInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.LeafQueueTemplateInfo.ConfItem;

import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static org.apache.hadoop.yarn.nodelabels.CommonNodeLabelsManager.NO_LABEL;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacityVector.ResourceUnitCapacityType.PERCENTAGE;

/**
 * 容量调度器Web UI信息辅助工具类，为容量调度器队列查询页面提供队列元信息处理能力，
 * 包括队列容量配置模式、队列类型、创建方式、自动创建子队列 eligibility等信息的提取转换。
 */
public class CapacitySchedulerInfoHelper {
  private static final String PARENT_QUEUE = "parent";
  private static final String LEAF_QUEUE = "leaf";
  private static final String UNKNOWN_QUEUE = "unknown";
  private static final String STATIC_QUEUE = "static";
  private static final String LEGACY_DYNAMIC_QUEUE = "dynamicLegacy";
  private static final String FLEXIBLE_DYNAMIC_QUEUE = "dynamicFlexible";
  private static final String AUTO_CREATION_OFF = "off";
  private static final String AUTO_CREATION_LEGACY = "legacy";
  private static final String AUTO_CREATION_FLEXIBLE = "flexible";

  /** 工具类禁止实例化 */
  private CapacitySchedulerInfoHelper() {}

  /**
   * 获取队列容量配置模式（百分比、绝对资源、权重、混合模式）
   * @param queue 待查询队列对象
   * @return 容量配置模式名称
   */
  public static String getMode(CSQueue queue) {
    // 旧版队列模式处理
    if (((AbstractCSQueue) queue).getQueueContext().getConfiguration().isLegacyQueueMode()) {
      // 绝对资源配置模式
      if (queue.getCapacityConfigType() ==
              AbstractCSQueue.CapacityConfigType.ABSOLUTE_RESOURCE) {
        return "absolute";
      } else if (queue.getCapacityConfigType() ==
              AbstractCSQueue.CapacityConfigType.PERCENTAGE) {
        // 百分比配置模式，需要判断是否使用权重
        float weight = queue.getQueueCapacities().getWeight();
        if (weight == -1) {
          // -1表示未启用权重模式，返回百分比
          return "percentage";
        } else {
          return "weight";
        }
      }
    } else {
      // 新版队列模式，获取已定义的容量类型集合
      final Set<QueueCapacityVector.ResourceUnitCapacityType> definedCapacityTypes =
              queue.getConfiguredCapacityVector(NO_LABEL).getDefinedCapacityTypes();
      // 单容量类型模式
      if (definedCapacityTypes.size() == 1) {
        QueueCapacityVector.ResourceUnitCapacityType next = definedCapacityTypes.iterator().next();
        if (Objects.requireNonNull(next) == PERCENTAGE) {
          return "percentage";
        } else if (next == QueueCapacityVector.ResourceUnitCapacityType.ABSOLUTE) {
          return "absolute";
        } else if (next == QueueCapacityVector.ResourceUnitCapacityType.WEIGHT) {
          return "weight";
        }
      } else if (definedCapacityTypes.size() > 1) {
        // 多种容量类型混合模式
        return "mixed";
      }
    }

    return "unknown";
  }

  /**
   * 获取队列类型（父队列/叶子队列）
   * @param queue 待查询队列对象
   * @return 队列类型字符串
   */
  public static String getQueueType(CSQueue queue) {
    if (queue instanceof AbstractLeafQueue) {
      return LEAF_QUEUE;
    } else if (queue instanceof AbstractParentQueue) {
      return PARENT_QUEUE;
    }
    return UNKNOWN_QUEUE;
  }

  /**
   * 获取队列创建方式（静态/旧版动态/新版灵活动态）
   * @param queue 待查询队列对象
   * @return 创建方式字符串
   */
  public static String getCreationMethod(CSQueue queue) {
    if (queue instanceof AutoCreatedLeafQueue) {
      return LEGACY_DYNAMIC_QUEUE;
    } else if (((AbstractCSQueue)queue).isDynamicQueue()) {
      return FLEXIBLE_DYNAMIC_QUEUE;
    } else {
      return STATIC_QUEUE;
    }
  }

  /**
   * 获取队列自动创建子队列的资格模式（关闭/旧版/灵活版）
   * @param queue 待查询队列对象
   * @return 自动创建资格模式字符串
   */
  public static String getAutoCreationEligibility(CSQueue queue) {
    if (queue instanceof ManagedParentQueue) {
      return AUTO_CREATION_LEGACY;
    } else if (queue instanceof ParentQueue &&
        ((ParentQueue)queue).isEligibleForAutoQueueCreation()) {
      return AUTO_CREATION_FLEXIBLE;
    } else {
      return AUTO_CREATION_OFF;
    }
  }

  /**
   * 将自动队列模板属性转换为Web DAO对象
   * @param templateProperties 模板属性键值对
   * @return 封装后的自动队列模板属性DAO对象
   */
  public static AutoQueueTemplatePropertiesInfo getAutoCreatedTemplate(
      Map<String, String> templateProperties) {
    AutoQueueTemplatePropertiesInfo propertiesInfo =
        new AutoQueueTemplatePropertiesInfo();
    for (Map.Entry<String, String> e :
        templateProperties.entrySet()) {
      propertiesInfo.add(new ConfItem(e.getKey(), e.getValue()));
    }

    return propertiesInfo;
  }
}