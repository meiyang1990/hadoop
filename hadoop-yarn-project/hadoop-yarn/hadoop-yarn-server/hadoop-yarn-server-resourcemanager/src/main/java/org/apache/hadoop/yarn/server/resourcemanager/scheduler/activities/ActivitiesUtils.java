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

import org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWSConsts;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ActivityNodeInfo;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * 调度活动记录工具类，提供活动节点的分组聚合转换功能，用于Web UI展示。
 */
public final class ActivitiesUtils {

  private ActivitiesUtils(){}

  /**
   * 根据分组方式对调度活动节点进行聚合，转换为Web UI可用的活动节点信息列表。
   * @param activityNodes 原始活动节点列表
   * @param groupBy 分组方式枚举
   * @return 聚合后的活动节点信息列表
   */
  public static List<ActivityNodeInfo> getRequestActivityNodeInfos(
      List<ActivityNode> activityNodes,
      RMWSConsts.ActivitiesGroupBy groupBy) {
    if (activityNodes == null) {
      return null;
    }
    // 按诊断信息分组场景：先按状态分组，再按简短诊断分组，收集对应节点ID列表
    if (groupBy == RMWSConsts.ActivitiesGroupBy.DIAGNOSTIC) {
      // 按状态 -> 简短诊断的层级分组，收集每个分组下的所有节点ID
      Map<ActivityState, Map<String, List<String>>> groupingResults =
          activityNodes.stream()
              .filter(e -> e.getNodeId() != null)
              .collect(Collectors.groupingBy(ActivityNode::getState, Collectors
                  .groupingBy(ActivityNode::getShortDiagnostic,
                      Collectors.mapping(e -> e.getNodeId() == null ? "" :
                          e.getNodeId().toString(), Collectors.toList()))));
      // 将分组结果转换为ActivityNodeInfo列表返回
      return groupingResults.entrySet().stream().flatMap(
          stateMap -> stateMap.getValue().entrySet().stream().map(
              diagMap -> new ActivityNodeInfo(stateMap.getKey(),
                  diagMap.getKey().isEmpty() ? null : diagMap.getKey(),
                  diagMap.getValue())))
          .collect(Collectors.toList());
    } else {
      // 不分组场景：直接转换每个非空节点为ActivityNodeInfo返回
      return activityNodes.stream().filter(e -> e.getNodeId() != null)
          .map(e -> new ActivityNodeInfo(e.getName(), e.getState(),
              e.getDiagnostic(), e.getNodeId())).collect(Collectors.toList());
    }
  }
}