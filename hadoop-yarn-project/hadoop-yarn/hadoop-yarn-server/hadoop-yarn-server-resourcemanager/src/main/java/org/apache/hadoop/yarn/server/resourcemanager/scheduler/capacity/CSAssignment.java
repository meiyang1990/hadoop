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
package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.NodeType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.AssignmentInformation;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.util.resource.Resources;

import java.util.List;

/**
 * 容量调度器资源分配结果封装类，保存一次调度尝试得到的资源分配信息
 */
@Private
@Unstable
public class CSAssignment {
  // 空分配结果，表示本次未分配到任何资源
  public static final CSAssignment NULL_ASSIGNMENT =
      new CSAssignment(Resources.createResource(0, 0), NodeType.NODE_LOCAL);

  // 跳过分配结果，表示本次调度需要跳过该队列
  public static final CSAssignment SKIP_ASSIGNMENT =
      new CSAssignment(SkippedType.OTHER);

  // 本次分配获得的资源
  private Resource resource;
  // Container分配的位置层级类型（节点本地/机架本地/任意）
  private NodeType type;

  // 待分配请求的位置层级类型
  private NodeType requestLocalityType;
  // 超额预留容器，当预留资源超过实际需求时记录
  private RMContainer excessReservation;
  // 本次分配所属的应用
  private FiCaSchedulerApp application;
  // 跳过分配原因
  private SkippedType skipped;

  /**
   * 队列跳过分配的原因枚举
   */
  public enum SkippedType {
    // 无需跳过
    NONE,
    // 队列资源限制已满
    QUEUE_LIMIT,
    // 其他原因跳过
    OTHER
  }

  // 是否已满足预留资源请求
  private boolean fulfilledReservation;
  // 分配统计信息，记录分配和预留的资源数量
  private final AssignmentInformation assignmentInformation;
  // 是否是扩容分配（对已有Container增加资源）
  private boolean increaseAllocation;
  // 需要杀死的容器列表，用于扩容时抢占资源
  private List<RMContainer> containersToKill;

  // 被满足的预留容器，当fulfilledReservation = true时有效
  private RMContainer fulfilledReservedContainer;
  // 本次分配使用的调度模式
  private SchedulingMode schedulingMode;

  /**
   * 构造资源分配结果
   * @param resource 分配得到的资源
   * @param type 分配位置层级类型
   */
  public CSAssignment(Resource resource, NodeType type) {
    this(resource, type, null, null, SkippedType.NONE, false);
  }

  /**
   * 构造超额预留分配结果
   * @param application 所属应用
   * @param excessReservation 超额预留容器
   */
  public CSAssignment(FiCaSchedulerApp application,
      RMContainer excessReservation) {
    this(excessReservation.getContainer().getResource(), NodeType.NODE_LOCAL,
      excessReservation, application, SkippedType.NONE, false);
  }

  /**
   * 构造跳过分配结果
   * @param skipped 跳过原因
   */
  public CSAssignment(SkippedType skipped) {
    this(Resource.newInstance(0, 0), NodeType.NODE_LOCAL, null, null, skipped,
      false);
  }

  /**
   * 完整构造资源分配结果
   * @param resource 分配得到的资源
   * @param type 分配位置层级类型
   * @param excessReservation 超额预留容器
   * @param application 所属应用
   * @param skipped 跳过原因
   * @param fulfilledReservation 是否满足预留请求
   */
  public CSAssignment(Resource resource, NodeType type,
      RMContainer excessReservation, FiCaSchedulerApp application,
      SkippedType skipped, boolean fulfilledReservation) {
    this.resource = resource;
    this.type = type;
    this.excessReservation = excessReservation;
    this.application = application;
    this.skipped = skipped;
    this.fulfilledReservation = fulfilledReservation;
    this.assignmentInformation = new AssignmentInformation();
  }

  public Resource getResource() {
    return resource;
  }
  
  public void setResource(Resource resource) {
    this.resource = resource;
  }

  public NodeType getType() {
    return type;
  }
  
  public void setType(NodeType type) {
    this.type = type;
  }
  
  public FiCaSchedulerApp getApplication() {
    return application;
  }

  public void setApplication(FiCaSchedulerApp application) {
    this.application = application;
  }

  public RMContainer getExcessReservation() {
    return excessReservation;
  }

  public void setExcessReservation(RMContainer rmContainer) {
    excessReservation = rmContainer;
  }

  public SkippedType getSkippedType() {
    return skipped;
  }

  public void setSkippedType(SkippedType skippedType) {
    this.skipped = skippedType;
  }

  @Override
  public String toString() {
    String ret = "resource:" + resource.toString();
    ret += "; type:" + type;
    ret += "; excessReservation:" + excessReservation;
    ret +=
        "; applicationid:"
            + (application != null ? application.getApplicationId().toString()
                : "null");
    ret += "; skipped:" + skipped;
    ret += "; fulfilled reservation:" + fulfilledReservation;
    ret +=
        "; allocations(count/resource):"
            + assignmentInformation.getNumAllocations() + "/"
            + assignmentInformation.getAllocated().toString();
    ret +=
        "; reservations(count/resource):"
            + assignmentInformation.getNumReservations() + "/"
            + assignmentInformation.getReserved().toString();
    return ret;
  }
  
  public void setFulfilledReservation(boolean fulfilledReservation) {
    this.fulfilledReservation = fulfilledReservation;
  }

  public boolean isFulfilledReservation() {
    return this.fulfilledReservation;
  }
  
  public AssignmentInformation getAssignmentInformation() {
    return this.assignmentInformation;
  }
  
  public boolean isIncreasedAllocation() {
    return increaseAllocation;
  }

  public void setIncreasedAllocation(boolean flag) {
    increaseAllocation = flag;
  }

  public void setContainersToKill(List<RMContainer> containersToKill) {
    this.containersToKill = containersToKill;
  }

  public List<RMContainer> getContainersToKill() {
    return containersToKill;
  }

  public RMContainer getFulfilledReservedContainer() {
    return fulfilledReservedContainer;
  }

  public void setFulfilledReservedContainer(
      RMContainer fulfilledReservedContainer) {
    this.fulfilledReservedContainer = fulfilledReservedContainer;
  }

  public SchedulingMode getSchedulingMode() {
    return schedulingMode;
  }

  public void setSchedulingMode(SchedulingMode schedulingMode) {
    this.schedulingMode = schedulingMode;
  }

  public NodeType getRequestLocalityType() {
    return requestLocalityType;
  }

  public void setRequestLocalityType(NodeType requestLocalityType) {
    this.requestLocalityType = requestLocalityType;
  }
}