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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.common;

import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;

/**
 * YARN资源调度器容器分配信息统计类，统计分配和预留操作的数量、资源总量与详细分配记录
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class AssignmentInformation {

  /**
   * 容器分配操作类型枚举
   */
  public enum Operation {
    /** 分配新容器操作, 预留容器操作 */
    ALLOCATION, RESERVATION;
    private static int SIZE = Operation.values().length;
    static int size() {
      return SIZE;
    }
  }

  /**
   * 单次容器分配操作的详细信息
   */
  public static class AssignmentDetails {
    public RMContainer rmContainer;
    public ContainerId containerId;
    public String queue;

    /** 构造分配详细信息对象
     * @param rmContainer 目标RM容器对象
     * @param queue 所属队列名称
     */
    public AssignmentDetails(RMContainer rmContainer, String queue) {
      this.containerId = rmContainer.getContainerId();
      this.rmContainer = rmContainer;
      this.queue = queue;
    }
  }

  // 按操作类型统计的操作计数数组
  private final int[] operationCounts;
  // 按操作类型统计的总资源数组
  private final Resource[] operationResources;
  // 按操作类型分类的分配详细信息列表数组
  private final List<AssignmentDetails>[] operationDetails;

  /** 构造分配信息统计对象，初始化各类操作的统计数据
   */
  @SuppressWarnings("unchecked")
  public AssignmentInformation() {
    // 获取操作类型总数
    int numOps = Operation.size();
    // 初始化操作计数数组
    this.operationCounts = new int[numOps];
    // 初始化资源统计数组
    this.operationResources = new Resource[numOps];
    // 初始化详细信息列表数组
    this.operationDetails = new List[numOps];
    // 遍历初始化每个操作类型的统计初始值
    for (int i=0; i < numOps; i++) {
      operationCounts[i] = 0;
      operationResources[i] = Resource.newInstance(0, 0);
      operationDetails[i] = new ArrayList<>();
    }
  }

  /** 获取已分配容器数量
   * @return 已分配容器总数
   */
  public int getNumAllocations() {
    return operationCounts[Operation.ALLOCATION.ordinal()];
  }

  /** 分配容器计数加1 */
  public void incrAllocations() {
    increment(Operation.ALLOCATION, 1);
  }

  /** 分配容器计数增加指定值
   * @param by 增量值
   */
  public void incrAllocations(int by) {
    increment(Operation.ALLOCATION, by);
  }

  /** 获取预留容器数量
   * @return 预留容器总数
   */
  public int getNumReservations() {
    return operationCounts[Operation.RESERVATION.ordinal()];
  }

  /** 预留容器计数加1 */
  public void incrReservations() {
    increment(Operation.RESERVATION, 1);
  }

  /** 预留容器计数增加指定值
   * @param by 增量值
   */
  public void incrReservations(int by) {
    increment(Operation.RESERVATION, by);
  }

  /** 对指定操作类型增加指定增量
   * @param op 操作类型
   * @param by 增量值
   */
  private void increment(Operation op, int by) {
    operationCounts[op.ordinal()] += by;
  }

  /** 获取所有分配操作的总资源
   * @return 已分配总资源
   */
  public Resource getAllocated() {
    return operationResources[Operation.ALLOCATION.ordinal()];
  }

  /** 获取所有预留操作的总资源
   * @return 预留总资源
   */
  public Resource getReserved() {
    return operationResources[Operation.RESERVATION.ordinal()];
  }

  /** 添加指定操作类型的分配详细信息
   * @param op 操作类型
   * @param rmContainer RM容器对象
   * @param queue 所属队列
   */
  private void addAssignmentDetails(Operation op, RMContainer rmContainer,
      String queue) {
    getDetails(op).add(new AssignmentDetails(rmContainer, queue));
  }

  /** 添加分配操作的详细信息
   * @param rmContainer RM容器对象
   * @param queue 所属队列
   */
  public void addAllocationDetails(RMContainer rmContainer, String queue) {
    addAssignmentDetails(Operation.ALLOCATION, rmContainer, queue);
  }

  /** 添加预留操作的详细信息
   * @param rmContainer RM容器对象
   * @param queue 所属队列
   */
  public void addReservationDetails(RMContainer rmContainer, String queue) {
    addAssignmentDetails(Operation.RESERVATION, rmContainer, queue);
  }

  /** 获取指定操作类型的详细信息列表
   * @param op 操作类型
   * @return 详细信息列表
   */
  private List<AssignmentDetails> getDetails(Operation op) {
    return operationDetails[op.ordinal()];
  }

  /** 获取所有分配操作的详细信息列表
   * @return 分配操作详细信息列表
   */
  public List<AssignmentDetails> getAllocationDetails() {
    return getDetails(Operation.ALLOCATION);
  }

  /** 获取所有预留操作的详细信息列表
   * @return 预留操作详细信息列表
   */
  public List<AssignmentDetails> getReservationDetails() {
    return getDetails(Operation.RESERVATION);
  }

  /** 从指定操作类型的详细列表中获取第一个RM容器
   * @param op 操作类型
   * @return 第一个RM容器，无记录则返回null
   */
  private RMContainer getFirstRMContainerFromOperation(Operation op) {
    List<AssignmentDetails> assignDetails = getDetails(op);
    if (assignDetails != null && !assignDetails.isEmpty()) {
      return assignDetails.get(0).rmContainer;
    }
    return null;
  }

  /** 获取第一个分配或预留的RM容器，优先返回分配容器，分配为空则返回预留容器
   * @return 第一个分配或预留容器，都为空则返回null
   */
  public RMContainer getFirstAllocatedOrReservedRMContainer() {
    RMContainer rmContainer;
    rmContainer = getFirstRMContainerFromOperation(Operation.ALLOCATION);
    if (null != rmContainer) {
      return rmContainer;
    }
    return getFirstRMContainerFromOperation(Operation.RESERVATION);
  }

  /** 获取第一个分配或预留容器的容器ID
   * @return 第一个容器ID，没有则返回null
   */
  public ContainerId getFirstAllocatedOrReservedContainerId() {
    RMContainer rmContainer = getFirstAllocatedOrReservedRMContainer();
    if (null != rmContainer) {
      return rmContainer.getContainerId();
    }
    return null;
  }
}