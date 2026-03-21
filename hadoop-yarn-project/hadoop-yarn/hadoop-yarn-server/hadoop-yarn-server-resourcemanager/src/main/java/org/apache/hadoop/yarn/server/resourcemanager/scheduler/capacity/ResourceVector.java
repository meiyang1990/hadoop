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

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * 文件: 容量调度器资源向量，用于按资源名称存储资源的浮点值
 * 功能: 支持多资源的浮点型数值存储与运算，为容量调度器的资源计算提供基础数据结构
 * 表示按资源名称分组的浮点型资源值集合，用于YARN容量调度器的资源计算
 */
public class ResourceVector implements Iterable<Map.Entry<String, Double>> {
  // 按资源名称存储对应浮点值的映射
  private final Map<String, Double> resourcesByName = new HashMap<>();

  /**
   * 创建一个所有预定义资源值都初始化为0的空资源向量
   * @return 全零初始化的资源向量
   */
  public static ResourceVector newInstance() {
    ResourceVector zeroResourceVector = new ResourceVector();
    // 遍历所有已知资源类型，初始化为0
    for (ResourceInformation resource : ResourceUtils.getResourceTypesArray()) {
      zeroResourceVector.setValue(resource.getName(), 0);
    }

    return zeroResourceVector;
  }

  /**
   * 创建一个所有预定义资源都设置为相同值的资源向量
   * @param value 所有资源要设置的值
   * @return 所有资源值统一的资源向量
   */
  public static ResourceVector of(double value) {
    ResourceVector emptyResourceVector = new ResourceVector();
    // 遍历所有已知资源类型，设置为统一值
    for (ResourceInformation resource : ResourceUtils.getResourceTypesArray()) {
      emptyResourceVector.setValue(resource.getName(), value);
    }

    return emptyResourceVector;
  }

  /**
   * 基于已有的Resource对象创建对应的资源向量
   * @param resource 用来初始化资源向量的YARN Resource对象
   * @return 包含对应资源值的资源向量
   */
  public static ResourceVector of(Resource resource) {
    ResourceVector resourceVector = new ResourceVector();
    // 遍历Resource对象中所有资源，存入向量
    for (ResourceInformation resourceInformation : resource.getResources()) {
      resourceVector.setValue(resourceInformation.getName(),
          (double)resourceInformation.getValue());
    }

    return resourceVector;
  }

  /**
   * 减去另一个资源向量中所有资源对应的值
   * @param otherResourceVector 要减去的资源向量
   */
  public void decrement(ResourceVector otherResourceVector) {
    for (Map.Entry<String, Double> resource : otherResourceVector) {
      setValue(resource.getKey(), getValue(resource.getKey()) - resource.getValue());
    }
  }

  /**
   * 指定资源减去指定数值
   * @param resourceName 资源名称
   * @param value 要减去的数值
   */
  public void decrement(String resourceName, double value) {
    setValue(resourceName, getValue(resourceName) - value);
  }

  /**
   * 指定资源加上指定数值
   * @param resourceName 资源名称
   * @param value 要加上的数值
   */
  public void increment(String resourceName, double value) {
    setValue(resourceName, getValue(resourceName) + value);
  }

  public double getValue(String resourceName) {
    return resourcesByName.get(resourceName);
  }

  public void setValue(String resourceName, double value) {
    resourcesByName.put(resourceName, value);
  }

  public boolean isEmpty() {
    return resourcesByName.isEmpty();
  }

  public Set<String> getResourceNames() {
    return resourcesByName.keySet();
  }

  @Override
  public Iterator<Map.Entry<String, Double>> iterator() {
    return resourcesByName.entrySet().iterator();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    return this.resourcesByName.equals(((ResourceVector) o).resourcesByName);
  }

  @Override
  public int hashCode() {
    return resourcesByName.hashCode();
  }
}