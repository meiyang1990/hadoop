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

import java.util.Set;

/**
 * YARN ResourceManager 调度通用诊断信息收集器实现，收集调度分配失败原因等诊断信息。
 */
public class GenericDiagnosticsCollector implements DiagnosticsCollector {

  /** 资源不足诊断信息前缀 */
  public final static String RESOURCE_DIAGNOSTICS_PREFIX =
      "insufficient resources=";

  /** 放置约束不满足诊断信息前缀 */
  public final static String PLACEMENT_CONSTRAINT_DIAGNOSTICS_PREFIX =
      "unsatisfied PC expression=";

  /** 节点分区不满足诊断信息前缀 */
  public final static String PARTITION_DIAGNOSTICS_PREFIX =
      "unsatisfied node partition=";

  private String diagnostics;

  private String details;

  /**
   * 收集通用诊断信息。
   * @param diagnosticsInfo 诊断摘要信息
   * @param detailsInfo 诊断详细信息
   */
  public void collect(String diagnosticsInfo, String detailsInfo) {
    this.diagnostics = diagnosticsInfo;
    this.details = detailsInfo;
  }

  /**
   * 获取诊断摘要信息。
   * @return 诊断摘要
   */
  public String getDiagnostics() {
    return diagnostics;
  }

  /**
   * 获取诊断详细信息。
   * @return 诊断详情
   */
  public String getDetails() {
    return details;
  }

  /**
   * 收集资源不足类型的诊断信息。
   * @param rc 资源计算器
   * @param required 请求的资源量
   * @param available 可用资源量
   */
  public void collectResourceDiagnostics(ResourceCalculator rc,
      Resource required, Resource available) {
    // 获取资源不足的资源名称列表
    Set<String> insufficientResourceNames =
        rc.getInsufficientResourceNames(required, available);
    // 构造诊断摘要
    this.diagnostics = new StringBuilder(RESOURCE_DIAGNOSTICS_PREFIX)
        .append(insufficientResourceNames).toString();
    // 构造资源需求和可用资源详情
    this.details = new StringBuilder().append("required=").append(required)
        .append(", available=").append(available).toString();
  }

  /**
   * 收集放置约束不满足类型的诊断信息。
   * @param pc 不满足的放置约束
   * @param targetType 目标类型
   */
  public void collectPlacementConstraintDiagnostics(PlacementConstraint pc,
      PlacementConstraint.TargetExpression.TargetType targetType) {
    // 构造放置约束不满足的诊断摘要
    this.diagnostics =
        new StringBuilder(PLACEMENT_CONSTRAINT_DIAGNOSTICS_PREFIX).append("\"")
            .append(pc).append("\", target-type=").append(targetType)
            .toString();
    this.details = null;
  }

  /**
   * 收集节点分区不满足类型的诊断信息。
   * @param requiredPartition 请求的分区
   * @param nodePartition 当前节点分区
   */
  public void collectPartitionDiagnostics(
      String requiredPartition, String nodePartition) {
    // 构造节点分区不满足的诊断摘要
    this.diagnostics =
        new StringBuilder(PARTITION_DIAGNOSTICS_PREFIX).append(nodePartition)
            .append(", required-partition=").append(requiredPartition)
            .toString();
    this.details = null;
  }
}