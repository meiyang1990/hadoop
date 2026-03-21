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

import org.apache.hadoop.yarn.server.resourcemanager.placement.VariableContext;

/**
 * YARN容量调度器映射规则匹配器接口，定义匹配规则的统一执行接口。
 * 用于根据应用提交上下文信息，判断是否匹配当前映射规则。
 */
public interface MappingRuleMatcher {
  /**
   * 根据当前变量上下文判断是否匹配该规则
   * @param variables 变量上下文，包含匹配所需的所有上下文变量（如用户、队列名等）
   * @return 如果匹配当前规则返回true，否则返回false
   */
  boolean match(VariableContext variables);
}