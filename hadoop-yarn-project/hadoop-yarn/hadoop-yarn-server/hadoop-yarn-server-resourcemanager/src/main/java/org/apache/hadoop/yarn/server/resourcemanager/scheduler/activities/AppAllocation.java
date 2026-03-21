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

import org.apache.commons.collections4.CollectionUtils;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * YARN资源调度应用分配记录，保存单个应用在一段时间内的容器分配信息，
 * 一个应用分配可能包含多次分配尝试过程。
 */
public class AppAllocation {
  private Priority priority;
  private NodeId nodeId;
  private ContainerId containerId;
  private ActivityState activityState;
  private String diagnostic;
  private String queueName;
  private List<ActivityNode> allocationAttempts;
  private long timestamp;

  /**
   * 构造应用分配记录，初始化分配尝试列表。
   * @param priority 应用优先级
   * @param nodeId 目标节点ID
   * @param queueName 应用所属队列名称
   */
  public AppAllocation(Priority priority, NodeId nodeId, String queueName) {
    this.priority = priority;
    this.nodeId = nodeId;
    this.allocationAttempts = new ArrayList<>();
    this.queueName = queueName;
  }

  /**
   * 更新应用容器状态、时间戳和诊断信息。
   * @param cId 容器ID
   * @param appState 分配活动状态
   * @param ts 时间戳
   * @param diagnostic 诊断信息
   */
  public void updateAppContainerStateAndTime(ContainerId cId,
      ActivityState appState, long ts, String diagnostic) {
    this.timestamp = ts;
    this.containerId = cId;
    this.activityState = appState;
    this.diagnostic = diagnostic;
  }

  /**
   * 添加一次应用容器分配活动尝试。
   * @param cId 容器ID字符串
   * @param reqPriority 请求优先级
   * @param state 分配状态
   * @param diagnose 诊断信息
   * @param level 活动日志级别
   * @param nId 目标节点ID
   * @param allocationRequestId 分配请求ID
   */
  public void addAppAllocationActivity(String cId, Integer reqPriority,
      ActivityState state, String diagnose, ActivityLevel level, NodeId nId,
      Long allocationRequestId) {
    ActivityNode container = new ActivityNode(cId, null, reqPriority,
        state, diagnose, level, nId, allocationRequestId);
    this.allocationAttempts.add(container);
    // 如果本次分配被拒绝，整体状态标记为跳过
    if (state == ActivityState.REJECTED) {
      this.activityState = ActivityState.SKIPPED;
    } else {
      this.activityState = state;
    }
  }

  public String getNodeId() {
    return nodeId == null ? null : nodeId.toString();
  }

  public String getQueueName() {
    return queueName;
  }

  public ActivityState getActivityState() {
    return activityState;
  }

  public Priority getPriority() {
    return priority;
  }

  public String getContainerId() {
    if (containerId == null) {
      return null;
    }
    return containerId.toString();
  }

  public String getDiagnostic() {
    return diagnostic;
  }

  public long getTime() {
    return this.timestamp;
  }

  public List<ActivityNode> getAllocationAttempts() {
    return allocationAttempts;
  }

  /**
   * 根据请求优先级和分配请求ID过滤分配尝试列表，生成新的应用分配记录。
   * @param requestPriorities 需要保留的请求优先级集合，为空不过滤优先级
   * @param allocationRequestIds 需要保留的分配请求ID集合，为空不过滤请求ID
   * @return 过滤后的新应用分配记录
   */
  public AppAllocation filterAllocationAttempts(Set<Integer> requestPriorities,
      Set<Long> allocationRequestIds) {
    AppAllocation appAllocation =
        new AppAllocation(this.priority, this.nodeId, this.queueName);
    appAllocation.activityState = this.activityState;
    appAllocation.containerId = this.containerId;
    appAllocation.timestamp = this.timestamp;
    appAllocation.diagnostic = this.diagnostic;
    // 构建过滤条件：优先级匹配且请求ID匹配
    Predicate<ActivityNode> predicate = (e) ->
        (CollectionUtils.isEmpty(requestPriorities) || requestPriorities
            .contains(e.getRequestPriority())) && (
            CollectionUtils.isEmpty(allocationRequestIds)
                || allocationRequestIds.contains(e.getAllocationRequestId()));
    // 流过滤得到符合条件的分配尝试
    appAllocation.allocationAttempts =
        this.allocationAttempts.stream().filter(predicate)
            .collect(Collectors.toList());
    return appAllocation;
  }

  public void setAllocationAttempts(List<ActivityNode> allocationAttempts) {
    this.allocationAttempts = allocationAttempts;
  }
}