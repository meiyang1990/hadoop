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

package org.apache.hadoop.yarn.server.resourcemanager.reservation.planning;

import java.util.HashSet;
import java.util.ListIterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.yarn.api.records.ReservationDefinition;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.api.records.ReservationRequest;
import org.apache.hadoop.yarn.api.records.ReservationRequestInterpreter;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.Plan;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.RLESparseResourceAllocation;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.RLESparseResourceAllocation.RLEOperator;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.ReservationAllocation;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.ReservationInterval;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.exceptions.ContractValidationException;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.exceptions.PlanningException;
import org.apache.hadoop.yarn.util.resource.Resources;

/**
 * YARN资源预留规划器，采用两阶段迭代规划算法处理预留分配。
 * 算法根据allocateLeft标志，按升序/降序遍历作业各个阶段，
 * 为每个阶段确定分配区间并完成资源分配，支持ANY/ALL/ORDER/ORDER_NO_GAP多种作业类型。
 */
public class IterativePlanner extends PlanningAlgorithm {

  // 存储算法执行中尚未提交到正式计划的临时修改
  private RLESparseResourceAllocation planModifications;

  // 从原始计划提取的负载数据
  private RLESparseResourceAllocation planLoads;
  private Resource capacity;
  private long step;

  // 作业基本参数
  private ReservationRequestInterpreter jobType;
  private long jobArrival;
  private long jobDeadline;

  // 两阶段对应的算法实现
  private StageExecutionInterval algStageExecutionInterval = null;
  private StageAllocator algStageAllocator = null;
  private final boolean allocateLeft;

  /**
   * 构造迭代规划器，指定阶段区间计算和分配的算法实现，以及遍历方向。
   * @param algStageExecutionInterval 阶段执行区间计算算法
   * @param algStageAllocator 阶段资源分配算法
   * @param allocateLeft 是否从左到右（时间升序）分配，false表示从右到左
   */
  public IterativePlanner(StageExecutionInterval algStageExecutionInterval,
      StageAllocator algStageAllocator, boolean allocateLeft) {

    this.allocateLeft = allocateLeft;
    setAlgStageExecutionInterval(algStageExecutionInterval);
    setAlgStageAllocator(algStageAllocator);

  }

  @Override
  public RLESparseResourceAllocation computeJobAllocation(Plan plan,
      ReservationId reservationId, ReservationDefinition reservation,
      String user) throws PlanningException {

    // 初始化规划所需的参数和数据
    initialize(plan, reservationId, reservation);

    // 创建存储本次分配结果的数据结构
    RLESparseResourceAllocation allocations =
        new RLESparseResourceAllocation(plan.getResourceCalculator());

    // 创建按指定方向遍历作业阶段的迭代器
    StageProvider stageProvider = new StageProvider(allocateLeft, reservation);

    // 当前处理的作业阶段
    ReservationRequest currentReservationStage;

    // 初始化周期性预留周期
    long period = 0;
    if(reservation.getRecurrenceExpression() != null){
      period = Long.parseLong(reservation.getRecurrenceExpression());
    }

    // 按指定方向遍历所有作业阶段
    while (stageProvider.hasNext()) {

      // 获取当前要分配的阶段
      currentReservationStage = stageProvider.next();

      // 验证当前阶段请求符合基本约束条件
      validateInputStage(plan, currentReservationStage);

      // 计算当前阶段允许分配的时间区间
      ReservationInterval stageInterval =
          setStageExecutionInterval(plan, reservation, currentReservationStage,
              allocations);
      Long stageArrival = stageInterval.getStartTime();
      Long stageDeadline = stageInterval.getEndTime();

      // 计算当前阶段的具体资源分配
      Map<ReservationInterval, Resource> curAlloc =
          computeStageAllocation(plan, currentReservationStage, stageArrival,
              stageDeadline, period, user, reservationId);

      // 分配失败处理
      if (curAlloc == null) {

        // ANY类型作业，跳过该阶段，尝试下一个
        if (jobType == ReservationRequestInterpreter.R_ANY) {
          continue;
        }

        // 非ANY类型作业，直接抛出异常，分配失败
        throw new PlanningException("The request cannot be satisfied");

      }

      // 验证ORDER_NO_GAP约束，保证阶段之间无间隙
      if (jobType == ReservationRequestInterpreter.R_ORDER_NO_GAP) {
        if (!validateOrderNoGap(allocations, curAlloc, allocateLeft)) {
          throw new PlanningException(
              "The allocation found does not respect ORDER_NO_GAP");
        }
      }

      // 将当前阶段分配结果加入总分配
      for (Entry<ReservationInterval, Resource> entry : curAlloc.entrySet()) {
        allocations.addInterval(entry.getKey(), entry.getValue());
      }

      // ANY类型只要找到一个满足的阶段即可结束
      if (jobType == ReservationRequestInterpreter.R_ANY) {
        break;
      }
    }

    // 最终结果为空，分配失败
    if (allocations.isEmpty()) {
      throw new PlanningException("The request cannot be satisfied");
    }

    return allocations;
  }

  /**
   * 验证ORDER_NO_GAP约束：保证当前阶段与已分配阶段之间无间隙。
   * @param allocations 已完成分配的阶段
   * @param curAlloc 当前待添加的阶段分配
   * @param allocateLeft 是否左到右分配
   * @return 符合约束返回true，否则返回false
   */
  protected static boolean validateOrderNoGap(
      RLESparseResourceAllocation allocations,
      Map<ReservationInterval, Resource> curAlloc, boolean allocateLeft) {

    // 左到右分配场景
    if (allocateLeft) {
      Long stageStartTime = findEarliestTime(curAlloc);
      Long allocationEndTime = allocations.getLatestNonNullTime();

      // 检查已有分配结束时间是否与当前阶段开始时间之间是否有间隙
      if ((allocationEndTime != -1) && (allocationEndTime < stageStartTime)) {
        return false;
      }
    // 右到左分配场景
    } else {
      Long stageEndTime = findLatestTime(curAlloc);
      Long allocationStartTime = allocations.getEarliestStartTime();

      // 检查已有分配开始时间与当前阶段结束时间之间是否有间隙
      if ((allocationStartTime != -1) && (stageEndTime < allocationStartTime)) {
        return false;
      }
    }

    // 验证当前阶段分配是非抢占式连续
    if (!isNonPreemptiveAllocation(curAlloc)) {
      return false;
    }

    // 验证通过
    return true;
  }

  /**
   * 初始化规划，从计划中读取基本参数，预处理已有负载数据。
   * @param plan 资源计划实例
   * @param reservationId 预留ID
   * @param reservation 预留定义
   * @throws PlanningException 初始化失败抛出规划异常
   */
  protected void initialize(Plan plan, ReservationId reservationId,
      ReservationDefinition reservation) throws PlanningException {

    // 获取计划的总容量和时间步长
    capacity = plan.getTotalCapacity();
    step = plan.getStep();

    // 获取作业的类型、到达时间和截止时间，并对齐到步长边界
    jobType = reservation.getReservationRequests().getInterpreter();
    jobArrival = stepRoundUp(reservation.getArrival(), step);
    jobDeadline = stepRoundDown(reservation.getDeadline(), step);

    // 初始化临时修改存储结构
    planModifications =
        new RLESparseResourceAllocation(plan.getResourceCalculator());

    // 读取作业时间范围内的累计负载
    planLoads = plan.getCumulativeLoadOverTime(jobArrival, jobDeadline);
    // 如果是更新已有预留，减去原有预留占用的资源
    ReservationAllocation oldRes = plan.getReservationById(reservationId);
    if (oldRes != null) {
      planLoads = RLESparseResourceAllocation.merge(
          plan.getResourceCalculator(), plan.getTotalCapacity(), planLoads,
          oldRes.getResourcesOverTime(jobArrival, jobDeadline),
          RLEOperator.subtract, jobArrival, jobDeadline);
    }
  }

  /**
   * 验证单个阶段请求的参数合法性。
   * @param plan 资源计划
   * @param rr 阶段请求
   * @throws ContractValidationException 参数非法抛出验证异常
   */
  private void validateInputStage(Plan plan, ReservationRequest rr)
      throws ContractValidationException {

    // 验证并发数不小于1
    if (rr.getConcurrency() < 1) {
      throw new ContractValidationException("Gang Size should be >= 1");
    }

    // 验证容器数量大于0
    if (rr.getNumContainers() <= 0) {
      throw new ContractValidationException("Num containers should be > 0");
    }

    // 验证容器总数是并发数的整数倍
    if (rr.getNumContainers() % rr.getConcurrency() != 0) {
      throw new ContractValidationException(
          "Parallelism must be an exact multiple of gang size");
    }

    // 验证单容器需求不超过集群最大单容器限制
    if (Resources.greaterThan(plan.getResourceCalculator(), capacity,
        rr.getCapability(), plan.getMaximumAllocation())) {

      throw new ContractValidationException(
          "Individual capability requests should not exceed cluster's "
              + "maxAlloc");

    }

  }

  /**
   * 验证当前阶段分配是非抢占式连续分配，即整个阶段是一个连续区间。
   * @param curAlloc 当前阶段分配结果
   * @return 是连续非抢占分配返回true，否则false
   */
  private static boolean isNonPreemptiveAllocation(
      Map<ReservationInterval, Resource> curAlloc) {
    // 非抢占式分配中，只有起点和终点各出现一次，因此端点集合大小应为2

    Set<Long> endPoints = new HashSet<Long>(2 * curAlloc.size());
    for (Entry<ReservationInterval, Resource> entry : curAlloc.entrySet()) {

      ReservationInterval interval = entry.getKey();
      Resource resource = entry.getValue();

      // 跳过无资源分配的区间
      if (Resources.equals(resource, Resource.newInstance(0, 0))) {
        continue;
      }

      // 获取区间端点
      Long left = interval.getStartTime();
      Long right = interval.getEndTime();

      // 端点去重：出现两次就移除，表示端点连接了两个区间
      if (!endPoints.contains(left)) {
        endPoints.add(left);
      } else {
        endPoints.remove(left);
      }

      // 对右端点执行相同处理
      if (!endPoints.contains(right)) {
        endPoints.add(right);
      } else {
        endPoints.remove(right);
      }
    }

    // 只有起点和终点各剩余一个端点，说明分配连续
    return (endPoints.size() == 2);

  }

  /**
   * 调用阶段执行区间计算算法获取区间。
   */
  protected ReservationInterval setStageExecutionInterval(Plan plan,
      ReservationDefinition reservation,
      ReservationRequest currentReservationStage,
      RLESparseResourceAllocation allocations) {
    return algStageExecutionInterval.computeExecutionInterval(plan,
        reservation, currentReservationStage, allocateLeft, allocations);
  }

  /**
   * 调用阶段分配算法计算具体资源分配。
   */
  protected Map<ReservationInterval, Resource> computeStageAllocation(Plan plan,
      ReservationRequest rr, long stageArrivalTime, long stageDeadline,
      long period, String user, ReservationId oldId) throws PlanningException {

    return algStageAllocator.computeStageAllocation(plan, planLoads,
        planModifications, rr, stageArrivalTime, stageDeadline, period, user,
        oldId);

  }

  /**
   * 设置阶段执行区间计算算法，支持链式调用。
   * @param alg 算法实现
   * @return 当前规划器实例
   */
  public IterativePlanner setAlgStageExecutionInterval(
      StageExecutionInterval alg) {

    this.algStageExecutionInterval = alg;
    return this; // 支持链式调用

  }

  /**
   * 设置阶段资源分配算法，支持链式调用。
   * @param alg 算法实现
   * @return 当前规划器实例
   */
  public IterativePlanner setAlgStageAllocator(StageAllocator alg) {

    this.algStageAllocator = alg;
    return this; // 支持链式调用

  }

  /**
   * 作业阶段迭代器，根据分配方向提供正序或倒序遍历阶段。
   */
  public static class StageProvider {

    private final boolean allocateLeft;

    private final ListIterator<ReservationRequest> li;

    /**
     * 构造阶段迭代器，根据分配方向设置起始位置。
     * @param allocateLeft 是否左到右分配
     * @param reservation 预留定义
     */
    public StageProvider(boolean allocateLeft,
        ReservationDefinition reservation) {

      this.allocateLeft = allocateLeft;
      int startingIndex;
      if (allocateLeft) {
        startingIndex = 0;
      } else {
        startingIndex =
            reservation.getReservationRequests().getReservationResources()
                .size();
      }
      // 根据起始位置获取迭代器
      li =
          reservation.getReservationRequests().getReservationResources()
              .listIterator(startingIndex);

    }

    /**
     * 检查是否还有下一个阶段。
     * @return 有下一个返回true
     */
    public boolean hasNext() {
      if (allocateLeft) {
        return li.hasNext();
      } else {
        return li.hasPrevious();
      }
    }

    /**
     * 获取下一个阶段。
     * @return 下一个阶段请求
     */
    public ReservationRequest next() {
      if (allocateLeft) {
        return li.next();
      } else {
        return li.previous();
      }
    }

    /**
     * 获取当前阶段在原始列表中的索引。
     * @return 当前阶段索引
     */
    public int getCurrentIndex() {
      if (allocateLeft) {
        return li.nextIndex() - 1;
      } else {
        return li.previousIndex() + 1;
      }
    }

  }

}