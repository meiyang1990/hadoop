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
package org.apache.hadoop.yarn.server.resourcemanager.placement.csmappingrule;

/**
 * 容量调度器映射规则条件变量接口，定义规则匹配中条件变量的求值方法。
 * 在队列映射规则中，用于根据应用提交路径解析得到条件变量的值。
 */
public interface MappingRuleConditionalVariable {
  /**
   * 根据分割后的应用路径和当前位置，求值得到条件变量的结果字符串。
   * @param parts 按分割符拆分后的应用名称路径数组
   * @param currentIndex 当前匹配处理到的路径索引位置
   * @return 条件变量求值后的结果字符串
   */
  String evaluateInPath(String[] parts, int currentIndex);
}