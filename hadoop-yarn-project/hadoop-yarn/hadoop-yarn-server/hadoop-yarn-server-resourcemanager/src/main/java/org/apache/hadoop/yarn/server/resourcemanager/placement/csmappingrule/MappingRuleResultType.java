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
 * 容量调度器映射规则结果类型枚举，定义了映射规则匹配应用后可能产生的所有结果类型。
 * 用于YARN容量调度器的应用队列放置规则，指示规则匹配后的处理动作。
 */
public enum MappingRuleResultType {
  /**
   * 跳过当前规则，继续匹配下一条规则。
   */
  SKIP,

  /**
   * 拒绝该应用提交。
   */
  REJECT,

  /**
   * 将应用放置到匹配到的指定队列。
   */
  PLACE,

  /**
   * 将应用放置到通过%default变量标记的默认队列。
   */
  PLACE_TO_DEFAULT
}