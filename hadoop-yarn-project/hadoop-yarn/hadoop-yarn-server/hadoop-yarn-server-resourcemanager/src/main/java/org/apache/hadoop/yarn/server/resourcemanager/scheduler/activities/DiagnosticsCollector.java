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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.activities;

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.resource.PlacementConstraint;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;

/**
 *  YARN资源调度过程中的诊断信息收集通用接口，用于记录调度各类检查不通过时的诊断信息。
 */
public interface DiagnosticsCollector {

  /**
   * 收集通用诊断信息和详情。
   * @param diagnostics 诊断概要信息
   * @param details 诊断详细内容
   */
  void collect(String diagnostics, String details);

  /**
   * 获取收集到的诊断概要信息。
   * @return 诊断概要字符串
   */
  String getDiagnostics();

  /**
   * 获取收集到的诊断详情。
   * @return 诊断详情字符串
   */
  String getDetails();

  /**
   * 收集资源分配检查相关的诊断信息。
   * @param rc 资源计算器
   * @param required 应用/容器所需资源
   * @param available 节点可用资源
   */
  void collectResourceDiagnostics(ResourceCalculator rc,
      Resource required, Resource available);

  /**
   * 收集位置约束检查相关的诊断信息。
   * @param pc 位置约束对象
   * @param targetType 约束目标类型
   */
  void collectPlacementConstraintDiagnostics(PlacementConstraint pc,
      PlacementConstraint.TargetExpression.TargetType targetType);

  /**
   * 收集节点分区匹配检查相关的诊断信息。
   * @param requiredPartition 需求分区
   * @param nodePartition 节点当前分区
   */
  void collectPartitionDiagnostics(
      String requiredPartition, String nodePartition);
}