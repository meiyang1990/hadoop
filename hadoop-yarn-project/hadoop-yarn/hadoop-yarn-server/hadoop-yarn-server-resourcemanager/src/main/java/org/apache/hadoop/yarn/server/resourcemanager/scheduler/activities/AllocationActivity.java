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

import org.apache.hadoop.yarn.api.records.NodeId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 资源分配活动记录，记录调度分配过程中的一次操作活动
 * 可分为队列、应用、容器不同层级的活动，包含状态、诊断信息、优先级等属性
 */
public class AllocationActivity {
  private String childName = null;
  private String parentName = null;
  private Integer appPriority = null;
  private Integer requestPriority = null;
  private ActivityState state;
  private String diagnostic = null;
  private NodeId nodeId;
  private Long allocationRequestId;
  private ActivityLevel level;

  private static final Logger LOG =
      LoggerFactory.getLogger(AllocationActivity.class);

  /**
   * 构造分配活动记录，根据活动层级存储对应属性
   * @param parentName 父节点名称
   * @param queueName 当前活动节点名称
   * @param priority 活动优先级
   * @param state 活动状态
   * @param diagnostic 诊断信息
   * @param level 活动层级
   * @param nodeId 关联节点ID（节点层级活动使用
   * @param allocationRequestId 分配请求ID（请求层级活动使用
   */
  public AllocationActivity(String parentName, String queueName,
      Integer priority, ActivityState state, String diagnostic,
      ActivityLevel level, NodeId nodeId, Long allocationRequestId) {
    this.childName = queueName;
    this.parentName = parentName;
    if (level != null) {
      this.level = level;
      // 根据活动层级分类存储优先级和属性
      switch (level) {
      case APP:
        // 应用层级活动存储应用优先级
        this.appPriority = priority;
        break;
      case REQUEST:
        // 请求层级活动存储请求优先级和分配请求ID
        this.requestPriority = priority;
        this.allocationRequestId = allocationRequestId;
        break;
      case NODE:
        // 节点层级活动存储节点ID
        this.nodeId = nodeId;
        break;
      default:
        break;
      }
    }
    this.state = state;
    this.diagnostic = diagnostic;
  }

  /**
   * 将当前活动转换为活动树节点，用于构建分配活动树展示
   * @return 构建好的活动树节点
   */
  public ActivityNode createTreeNode() {
    return new ActivityNode(this.childName, this.parentName,
        this.level == ActivityLevel.APP ?
            this.appPriority : this.requestPriority,
        this.state, this.diagnostic, this.level,
        this.nodeId, this.allocationRequestId);
  }

  /**
   * 获取当前活动节点名称
   * @return 当前活动节点名称
   */
  public String getName() {
    return this.childName;
  }

  /**
   * 获取活动状态字符串
   * @return 活动状态字符串
   */
  public String getState() {
    return this.state.toString();
  }
}