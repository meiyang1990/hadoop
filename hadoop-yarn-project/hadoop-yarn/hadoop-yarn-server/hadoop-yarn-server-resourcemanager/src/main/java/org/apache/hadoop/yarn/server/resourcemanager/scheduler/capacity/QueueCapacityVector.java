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

import org.apache.hadoop.yarn.api.records.ResourceInformation;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * YARN容量调度器队列容量向量，存储每种资源的容量值和对应的容量计算类型
 * 支持百分比、绝对值、权重三种不同的容量配置方式，可处理多种资源混合配置场景
 */
public class QueueCapacityVector implements
    Iterable<QueueCapacityVector.QueueCapacityVectorEntry> {
  private static final String START_PARENTHESES = "[";
  private static final String END_PARENTHESES = "]";
  private static final String RESOURCE_DELIMITER = ",";
  private static final String VALUE_DELIMITER = "=";

  // 存储各资源容量值的资源向量
  private final ResourceVector resource;
  // 资源名 -> 容量计算类型 映射表
  private final Map<String, ResourceUnitCapacityType> capacityTypes
      = new HashMap<>();
  // 容量计算类型 -> 该类型下所有资源名 反向映射表
  private final Map<ResourceUnitCapacityType, Set<String>> capacityTypePerResource
      = new HashMap<>();

  public QueueCapacityVector() {
    this.resource = new ResourceVector();
  }

  private QueueCapacityVector(ResourceVector resource) {
    this.resource = resource;
  }

  /**
   * 创建零值容量向量，所有资源默认使用绝对容量类型
   *
   * @return 零值容量向量实例
   */
  public static QueueCapacityVector newInstance() {
    QueueCapacityVector newCapacityVector =
        new QueueCapacityVector(ResourceVector.newInstance());
    for (Map.Entry<String, Double> resourceEntry : newCapacityVector.resource) {
      newCapacityVector.storeResourceType(resourceEntry.getKey(),
          ResourceUnitCapacityType.ABSOLUTE);
    }

    return newCapacityVector;
  }

  /**
   * 创建均匀容量向量，所有资源使用相同的值和相同容量类型
   *
   * @param value 所有资源统一设置的容量值
   * @param capacityType 所有资源统一使用的容量类型
   * @return 均匀容量向量实例
   */
  public static QueueCapacityVector of(
      double value, ResourceUnitCapacityType capacityType) {
    QueueCapacityVector newCapacityVector =
        new QueueCapacityVector(ResourceVector.of(value));
    for (Map.Entry<String, Double> resourceEntry : newCapacityVector.resource) {
      newCapacityVector.storeResourceType(resourceEntry.getKey(), capacityType);
    }

    return newCapacityVector;
  }

  /**
   * 获取指定资源的容量条目，包含容量类型和容量值
   * @param resourceName 资源名称
   * @return 资源容量条目
   */
  public QueueCapacityVectorEntry getResource(String resourceName) {
    return new QueueCapacityVectorEntry(capacityTypes.get(resourceName),
        resourceName, resource.getValue(resourceName));
  }

  /**
   * 获取当前向量中定义的资源数量
   *
   * @return 资源数量
   */
  public int getResourceCount() {
    return capacityTypes.size();
  }

  /**
   * 设置指定资源的容量值和容量类型
   *
   * @param resourceName 资源名称
   * @param value        容量值
   * @param capacityType 容量计算类型
   */
  public void setResource(String resourceName, double value,
                          ResourceUnitCapacityType capacityType) {
    // 向后兼容处理: 将旧配置的"memory"转换为标准URI
    String convertedResourceName = resourceName;
    if (resourceName.equals("memory")) {
      convertedResourceName = ResourceInformation.MEMORY_URI;
    }
    // 设置资源容量值
    resource.setValue(convertedResourceName, value);
    // 存储资源容量类型映射关系
    storeResourceType(convertedResourceName, capacityType);
  }

  /**
   * 快捷获取内存资源的容量值
   *
   * @return 内存资源容量值
   */
  public double getMemory() {
    return resource.getValue(ResourceInformation.MEMORY_URI);
  }

  /**
   * 检查当前容量向量是否为空（未定义任何资源）
   * @return 空返回true，否则返回false
   */
  public boolean isEmpty() {
    return resource.isEmpty() && capacityTypePerResource.isEmpty() && capacityTypes.isEmpty();
  }

  /**
   * 获取指定容量类型下的所有资源名称
   *
   * @param capacityType 容量计算类型
   * @return 指定容量类型的所有资源名称集合
   */
  public Set<String> getResourceNamesByCapacityType(
      ResourceUnitCapacityType capacityType) {
    return new HashSet<>(capacityTypePerResource.getOrDefault(capacityType,
        Collections.emptySet()));
  }

  /**
   * 检查指定资源是否属于给定容量类型
   *
   * @param resourceName 资源名称
   * @param capacityType 容量计算类型
   * @return 属于指定类型返回true，否则返回false
   */
  public boolean isResourceOfType(
      String resourceName, ResourceUnitCapacityType capacityType) {
    return capacityTypes.containsKey(resourceName) &&
        capacityTypes.get(resourceName).equals(capacityType);
  }

  @Override
  public Iterator<QueueCapacityVectorEntry> iterator() {
    return new Iterator<QueueCapacityVectorEntry>() {
      private final Iterator<Map.Entry<String, Double>> resources =
          resource.iterator();
      private int i = 0;

      @Override
      public boolean hasNext() {
        return resources.hasNext() && capacityTypes.size() > i;
      }

      @Override
      public QueueCapacityVectorEntry next() {
        Map.Entry<String, Double> resourceInformation = resources.next();
        i++;
        return new QueueCapacityVectorEntry(
            capacityTypes.get(resourceInformation.getKey()),
            resourceInformation.getKey(), resourceInformation.getValue());
      }
    };
  }

  /**
   * 获取当前向量中所有已定义的容量类型集合
   *
   * @return 已定义容量类型集合
   */
  public Set<ResourceUnitCapacityType> getDefinedCapacityTypes() {
    return capacityTypePerResource.keySet();
  }

  /**
   * 检查当前向量是否为混合容量向量（使用了超过一种容量类型，不是均匀配置）
   * @return 混合容量向量返回true，否则返回false
   */
  public boolean isMixedCapacityVector() {
    return getDefinedCapacityTypes().size() > 1;
  }

  /**
   * 获取当前向量中所有已定义的资源名称集合
   * @return 资源名称集合
   */
  public Set<String> getResourceNames() {
    return resource.getResourceNames();
  }

  /**
   * 存储资源与容量类型的映射关系，处理类型变更时的旧映射清理
   * @param resourceName 资源名称
   * @param resourceType 容量计算类型
   */
  private void storeResourceType(
      String resourceName, ResourceUnitCapacityType resourceType) {
    // 如果该资源已有旧类型，且与新类型不同，清理旧映射
    if (capacityTypes.get(resourceName) != null
        && !capacityTypes.get(resourceName).equals(resourceType)) {
      capacityTypePerResource.get(capacityTypes.get(resourceName))
          .remove(resourceName);
      // 如果旧类型下没有资源了，移除该类型映射
      if (capacityTypePerResource.get(capacityTypes.get(resourceName)).isEmpty()) {
        capacityTypePerResource.remove(capacityTypes.get(resourceName));
      }
    }

    // 添加新类型映射
    capacityTypePerResource.putIfAbsent(resourceType, new HashSet<>());
    capacityTypePerResource.get(resourceType).add(resourceName);
    capacityTypes.put(resourceName, resourceType);
  }

  @Override
  public String toString() {
    StringBuilder stringVector = new StringBuilder();
    stringVector.append(START_PARENTHESES);

    int resourceCount = 0;
    // 遍历所有资源拼接字符串表示
    for (Map.Entry<String, Double> resourceEntry : resource) {
      resourceCount++;
      stringVector.append(resourceEntry.getKey())
          .append(VALUE_DELIMITER)
          .append(resourceEntry.getValue())
          .append(capacityTypes.get(resourceEntry.getKey()).postfix);
      // 不是最后一个资源，添加分隔符
      if (resourceCount < capacityTypes.size()) {
        stringVector.append(RESOURCE_DELIMITER);
      }
    }

    stringVector.append(END_PARENTHESES);

    return stringVector.toString();
  }

  /**
   * 资源容量计算类型枚举，每个类型对应配置语法中的后缀标识
   */
  public enum ResourceUnitCapacityType {
    // 百分比类型，后缀%
    PERCENTAGE("%"),
    // 绝对容量类型，无后缀
    ABSOLUTE(""),
    // 权重类型，后缀w
    WEIGHT("w");
    private final String postfix;

    ResourceUnitCapacityType(String postfix) {
      this.postfix = postfix;
    }

    public String getPostfix() {
      return postfix;
    }
  }

  /**
   * 单个资源容量条目，封装资源名称、容量值和容量类型
   */
  public static class QueueCapacityVectorEntry {
    private final ResourceUnitCapacityType vectorResourceType;
    private final double resourceValue;
    private final String resourceName;

    public QueueCapacityVectorEntry(ResourceUnitCapacityType vectorResourceType,
                                    String resourceName, double resourceValue) {
      this.vectorResourceType = vectorResourceType;
      this.resourceValue = resourceValue;
      this.resourceName = resourceName;
    }

    public ResourceUnitCapacityType getVectorResourceType() {
      return vectorResourceType;
    }

    public double getResourceValue() {
      return resourceValue;
    }

    public String getResourceName() {
      return resourceName;
    }

    public String getResourceWithPostfix() {
      return resourceValue + vectorResourceType.getPostfix();
    }
  }
}