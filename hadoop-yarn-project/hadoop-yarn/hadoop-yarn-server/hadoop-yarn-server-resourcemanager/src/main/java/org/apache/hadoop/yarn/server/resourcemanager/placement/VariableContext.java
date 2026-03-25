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

package org.apache.hadoop.yarn.server.resourcemanager.placement;

import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableSet;
import org.apache.hadoop.yarn.server.resourcemanager.placement.csmappingrule.MappingRuleConditionalVariable;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * 应用放置上下文变量存储容器，用于容量调度器放置规则处理时存储上下文变量
 * 支持不可变变量（仅可设置一次），提供字符串变量替换工具方法
 * 简化设计，不继承Map接口，仅暴露所需功能
 */
public class VariableContext {
  /**
   * 存储普通变量的键值对
   */
  private Map<String, String> variables = new HashMap<>();
  /**
   * 存储变量原始值
   */
  private Map<String, String> originalVariables = new HashMap<>();

  /**
   * 存储条件变量的映射
   */
  private Map<String, MappingRuleConditionalVariable> conditionalVariables =
      new HashMap<>();

  /**
   * 存储不可变变量的名称集合，若为null则不检查不可变性
   */
  private Set<String> immutableNames;

  /**
   * 存储额外数据集，用于匹配规则判断，不参与变量替换
   */
  private Map<String, Set<String>> extraDataset = new HashMap<>();

  /**
   * 检查指定变量是否为不可变变量
   * @param name 变量名称
   * @return true 如果变量是不可变的
   */
  public boolean isImmutable(String name) {
    return (immutableNames != null && immutableNames.contains(name));
  }

  /**
   * 设置不可变变量集合，仅可设置一次
   * @param variableNames 不可变变量名称集合
   * @throws IllegalStateException 如果不可变集合已经设置过
   * @return 当前VariableContext实例，支持链式调用
   */
  public VariableContext setImmutables(Set<String> variableNames) {
    if (this.immutableNames != null) {
      throw new IllegalStateException("Immutable variables are already defined,"
          + " variable immutability cannot be changed once set!");
    }
    this.immutableNames = ImmutableSet.copyOf(variableNames);
    return this;
  }

  /**
   * 通过数组设置不可变变量名称，内部将转换为不可变集合
   * @param variableNames 不可变变量名称数组
   * @throws IllegalStateException 如果不可变集合已经设置过
   * @return 当前VariableContext实例，支持链式调用
   */
  public VariableContext setImmutables(String... variableNames) {
    if (this.immutableNames != null) {
      throw new IllegalStateException("Immutable variables are already defined,"
          + " variable immutability cannot be changed once set!");
    }
    this.immutableNames = ImmutableSet.copyOf(variableNames);
    return this;
  }

  /**
   * 添加或更新普通变量，若变量已存在且不可变则抛出异常
   * 若变量已定义为条件变量也不允许修改
   * @param name 变量名称
   * @param value 变量值
   * @throws IllegalStateException 如果变量不可变或已定义为条件变量
   * @return 当前VariableContext实例，支持链式调用
   */
  public VariableContext put(String name, String value) {
    if (variables.containsKey(name) && isImmutable(name)) {
      throw new IllegalStateException(
          "Variable '" + name + "' is immutable, cannot update it's value!");
    }

    if (conditionalVariables.containsKey(name)) {
      throw new IllegalStateException(
          "Variable '" + name + "' is already defined as a conditional" +
              " variable, cannot change it's value!");
    }
    variables.put(name, value);
    return this;
  }

  /**
   * 存储变量原始值
   * @param name 变量名称
   * @param value 原始值
   */
  public void putOriginal(String name, String value) {
    originalVariables.put(name, value);
  }

  /**
   * 添加条件变量，每个名称仅可添加一次
   * @param name 条件变量名称
   * @param variable 条件变量求值器实例
   * @return 当前VariableContext实例，支持链式调用
   */
  public VariableContext putConditional(String name,
      MappingRuleConditionalVariable variable) {
    if (conditionalVariables.containsKey(name)) {
      throw new IllegalStateException(
          "Variable '" + name + "' is conditional, cannot update it's value!");
    }
    conditionalVariables.put(name, variable);
    return this;
  }

  /**
   * 获取变量值，null值会替换为空字符串
   * @param name 变量名称
   * @return 变量值，null返回空字符串
   */
  public String get(String name) {
    String ret = variables.get(name);
    return ret == null ? "" : ret;
  }

  /**
   * 获取变量原始值
   * @param name 变量名称
   * @return 变量原始值
   */
  public String getOriginal(String name) {
    return originalVariables.get(name);
  }

  /**
   * 添加额外数据集，每个名称仅可添加一次
   * 额外数据集不参与字符串变量替换，仅可由规则显式访问用于匹配判断
   * @param name 数据集引用名称
   * @param set 要存储的数据集
   */
  public void putExtraDataset(String name, Set<String> set) {
    if (extraDataset.containsKey(name)) {
      throw new IllegalStateException(
          "Dataset '" + name + "' is already set!");
    }
    extraDataset.put(name, set);
  }

  /**
   * 根据名称获取额外数据集
   * @param name 数据集名称
   * @return 对应数据集，不存在则返回null
   */
  public Set<String> getExtraDataset(String name) {
    return extraDataset.get(name);
  }

  /**
   * 检查上下文是否包含指定普通变量
   * @param name 变量名称
   * @return true 如果包含该变量
   */
  public boolean containsKey(String name) {
    return variables.containsKey(name);
  }

  /**
   * 替换输入字符串中的所有变量，按变量名长度降序排序处理
   * 避免短变量名匹配长变量名字符串前缀导致错误替换
   * null值视为空字符串，输入null返回null
   * @param input 包含变量的输入字符串
   * @return 替换完成后的字符串
   */
  public String replaceVariables(String input) {
    if (input == null) {
      return null;
    }

    String[] keys = variables.keySet().toArray(new String[]{});
    // 按变量名长度降序排序，长变量先替换，避免短变量错误匹配长变量前缀
    Arrays.sort(keys, (a, b) -> b.length() - a.length());

    String ret = input;
    for (String key : keys) {
      // 跳过null键
      if (key == null) {
        continue;
      }
      ret = ret.replace(key, get(key));
    }

    return ret;
  }

  /**
   * 按点分隔队列路径进行变量替换，仅对完全匹配路径段的变量进行替换
   * 支持条件变量求值，仅路径段完全等于变量名才会替换
   * null值视为空字符串，输入null返回null
   * @param input 点分隔的队列路径字符串
   * @return 替换完成后的路径字符串
   */
  public String replacePathVariables(String input) {
    if (input == null) {
      return null;
    }

    // 按点分割路径段
    String[] parts = input.split("\\.");
    for (int i = 0; i < parts.length; i++) {
      String newVal = parts[i];
      // 先检查是否为普通变量，再检查是否为条件变量，未找到则保留原值
      if (variables.containsKey(parts[i])) {
        newVal = variables.get(parts[i]);
      } else if (conditionalVariables.containsKey(parts[i])) {
        MappingRuleConditionalVariable condVariable =
            conditionalVariables.get(parts[i]);
        if (condVariable != null) {
          // 调用条件变量求值器计算当前位置的值
          newVal = condVariable.evaluateInPath(parts, i);
        }
      }

      // null值替换为空字符串
      if (newVal == null) {
        newVal = "";
      }
      parts[i] = newVal;
    }

    return String.join(".", parts);
  }
}