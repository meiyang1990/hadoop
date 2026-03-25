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

package org.apache.hadoop.yarn.server.resourcemanager.webapp.dao;

import org.apache.hadoop.thirdparty.com.google.common.collect.Iterables;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.activities.ActivitiesUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.activities.ActivityNode;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWSConsts;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.List;

/**
 * 应用资源请求分配信息的数据访问对象，用于在ResourceManager Web UI展示请求分配详情
 */
@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class AppRequestAllocationInfo {
  private Integer requestPriority;
  private Long allocationRequestId;
  private String allocationState;
  private String diagnostic;
  private List<ActivityNodeInfo> children;

  AppRequestAllocationInfo() {
  }

  /**
   * 从调度活动节点构造应用请求分配信息对象
   * @param activityNodes 调度活动节点列表
   * @param groupBy 活动分组方式
   */
  AppRequestAllocationInfo(List<ActivityNode> activityNodes,
      RMWSConsts.ActivitiesGroupBy groupBy) {
    // 获取最后一个活动节点，提取最终分配结果信息
    ActivityNode lastActivityNode = Iterables.getLast(activityNodes);
    this.requestPriority = lastActivityNode.getRequestPriority();
    this.allocationRequestId = lastActivityNode.getAllocationRequestId();
    this.allocationState = lastActivityNode.getState().name();
    // 如果是请求类型节点且包含诊断信息，保存诊断信息
    if (lastActivityNode.isRequestType()
        && lastActivityNode.getDiagnostic() != null) {
      this.diagnostic = lastActivityNode.getDiagnostic();
    }
    // 转换并构造子活动节点信息，按指定分组方式组织
    this.children = ActivitiesUtils
        .getRequestActivityNodeInfos(activityNodes, groupBy);
  }

  public Integer getRequestPriority() {
    return requestPriority;
  }

  public Long getAllocationRequestId() {
    return allocationRequestId;
  }

  public String getAllocationState() {
    return allocationState;
  }

  public List<ActivityNodeInfo> getChildren() {
    return children;
  }

  public String getDiagnostic() {
    return diagnostic;
  }
}