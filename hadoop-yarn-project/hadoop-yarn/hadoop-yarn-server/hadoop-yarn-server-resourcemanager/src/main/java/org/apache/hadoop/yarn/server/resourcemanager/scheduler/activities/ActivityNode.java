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

import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.NodeId;

import java.util.LinkedList;
import java.util.List;

/**
 * 分配活动树中的树节点，记录YARN资源调度分配过程中不同层级的分配活动信息
 * 节点可代表队列、应用、分配请求或节点，成功分配后会生成子节点形成层级树
 */
public class ActivityNode {
  private String activityNodeName;
  private String parentName;
  private Integer appPriority;
  private Integer requestPriority;
  private ActivityState state;
  private String diagnostic;
  private NodeId nodeId;
  private Long allocationRequestId;

  private List<ActivityNode> childNode;

  /**
   * 构造活动节点，根据层级类型初始化对应优先级和标识信息
   * @param activityNodeName 节点名称
   * @param parentName 父节点名称
   * @param priority 优先级
   * @param state 分配活动状态
   * @param diagnostic 诊断信息
   * @param level 节点层级类型
   * @param nodeId 关联的节点ID
   * @param allocationRequestId 分配请求ID
   */
  public ActivityNode(String activityNodeName, String parentName,
      Integer priority, ActivityState state, String diagnostic,
      ActivityLevel level, NodeId nodeId, Long allocationRequestId) {
    this.activityNodeName = activityNodeName;
    this.parentName = parentName;
    if (level != null) {
      switch (level) {
      case APP:
        // 应用层级存储应用优先级
        this.appPriority = priority;
        break;
      case REQUEST:
        // 请求层级存储请求优先级和分配请求ID
        this.requestPriority = priority;
        this.allocationRequestId = allocationRequestId;
        break;
      case NODE:
        // 节点层级存储请求优先级、分配请求ID和节点ID
        this.requestPriority = priority;
        this.allocationRequestId = allocationRequestId;
        this.nodeId = nodeId;
        break;
      default:
        break;
      }
    }
    this.state = state;
    this.diagnostic = diagnostic;
    this.childNode = new LinkedList<>();
  }

  public String getName() {
    return this.activityNodeName;
  }

  public String getParentName() {
    return this.parentName;
  }

  /**
   * 添加子节点到链表头部，保证最新分配活动排在前面
   * @param node 子活动节点
   */
  public void addChild(ActivityNode node) {
    childNode.add(0, node);
  }

  public List<ActivityNode> getChildren() {
    return this.childNode;
  }

  public ActivityState getState() {
    return this.state;
  }

  public String getDiagnostic() {
    return this.diagnostic;
  }

  public Integer getAppPriority() {
    return appPriority;
  }

  public Integer getRequestPriority() {
    return requestPriority;
  }

  public NodeId getNodeId() {
    return nodeId;
  }

  public Long getAllocationRequestId() {
    return allocationRequestId;
  }

  /**
   * 判断当前节点是否为应用层级节点
   * @return 是否为应用层级节点
   */
  public boolean isAppType() {
    if (appPriority != null) {
      return true;
    } else {
      return false;
    }
  }

  /**
   * 判断当前节点是否为请求层级节点（非节点层级）
   * @return 是否为请求层级节点
   */
  public boolean isRequestType() {
    return requestPriority != null && nodeId == null;
  }

  /**
   * 获取截断后的短诊断信息，仅返回分隔符前的第一段
   * @return 短诊断信息
   */
  public String getShortDiagnostic() {
    if (this.diagnostic == null) {
      return "";
    } else {
      return StringUtils.split(this.diagnostic,
          ActivitiesManager.DIAGNOSTICS_DETAILS_SEPARATOR)[0];
    }
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(this.activityNodeName + " ")
        .append(this.appPriority + " ")
        .append(this.state + " ");
    if (this.nodeId != null) {
      sb.append(this.nodeId + " ");
    }
    if (!this.diagnostic.equals("")) {
      sb.append(this.diagnostic + "\n");
    }
    sb.append("\n");
    // 递归拼接所有子节点信息
    for (ActivityNode child : childNode) {
      sb.append(child.toString() + "\n");
    }
    return sb.toString();
  }

}