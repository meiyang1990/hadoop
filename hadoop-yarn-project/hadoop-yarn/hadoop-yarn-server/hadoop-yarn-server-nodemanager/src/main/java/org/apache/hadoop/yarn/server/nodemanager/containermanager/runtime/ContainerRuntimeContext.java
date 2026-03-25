// 这个文件已经全部加上中文注释
/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.yarn.server.nodemanager.containermanager.runtime;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * 容器运行时上下文，用于封装容器运行时所需的容器信息和执行属性，为容器运行提供统一的上下文环境
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public final class ContainerRuntimeContext {
  private final Container container;
  private final Map<Attribute<?>, Object> executionAttributes;

  /**
   * 类型安全的属性键定义类，相比直接使用字符串键提供更好的类型安全保障
   * @param <T> 属性值的类型
   */
  public static final class Attribute<T> {
    private final Class<T> valueClass;
    private final String id;

    private Attribute(Class<T> valueClass, String id) {
        this.valueClass = valueClass;
        this.id = id;
    }

    @Override
    public int hashCode() {
      // 基于值类型和ID计算哈希值，确保两个相同属性的哈希一致
      return valueClass.hashCode() + 31 * id.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
      if (!(obj instanceof Attribute)){
        return false;
      }

      Attribute<?> attribute = (Attribute<?>) obj;

      // 属性相等需同时满足值类型相同和ID相同
      return valueClass.equals(attribute.valueClass) && id.equals(attribute.id);
    }

    /**
     * 工厂方法，创建一个新的属性键实例
     * @param valueClass 属性值的类型
     * @param id 属性唯一标识
     * @param <T> 属性值类型泛型
     * @return 新的属性键实例
     */
    public static <T> Attribute<T> attribute(Class<T> valueClass, String id) {
      return new Attribute<T>(valueClass, id);
    }
  }

  /**
   * ContainerRuntimeContext的构建器，使用Builder模式构造上下文对象
   */
  public static final class Builder {
    private final Container container;
    private Map<Attribute<?>, Object> executionAttributes;

    /**
     * 构造构建器，绑定目标容器
     * @param container 要构造上下文的容器
     */
    public Builder(Container container) {
      executionAttributes = new HashMap<>();
      this.container = container;
    }

    /**
     * 设置执行属性，保证类型安全
     * @param attribute 属性键
     * @param value 属性值
     * @param <E> 属性值类型
     * @return 当前构建器实例，支持链式调用
     */
    public <E> Builder setExecutionAttribute(Attribute<E> attribute, E value) {
      this.executionAttributes.put(attribute, attribute.valueClass.cast(value));
      return this;
    }

    /**
     * 构造最终的ContainerRuntimeContext实例
     * @return 构建完成的容器运行时上下文
     */
    public ContainerRuntimeContext build() {
      return new ContainerRuntimeContext(this);
    }
  }

  private ContainerRuntimeContext(Builder builder) {
    this.container = builder.container;
    this.executionAttributes = builder.executionAttributes;
  }

  /**
   * 获取上下文对应的容器实例
   * @return 容器实例
   */
  public Container getContainer() {
    return this.container;
  }

  /**
   * 获取所有执行属性的不可修改视图
   * @return 不可修改的执行属性映射
   */
  public Map<Attribute<?>, Object> getExecutionAttributes() {
    return Collections.unmodifiableMap(this.executionAttributes);
  }

  /**
   * 根据属性键获取指定类型的属性值
   * @param attribute 属性键
   * @param <E> 属性值类型
   * @return 类型转换后的属性值
   */
  public <E> E getExecutionAttribute(Attribute<E> attribute) {
    return attribute.valueClass.cast(executionAttributes.get(attribute));
  }
}