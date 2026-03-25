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

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.TreeSet;

import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.api.records.ReservationRequest;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.Plan;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.RLESparseResourceAllocation;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.RLESparseResourceAllocation.RLEOperator;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.ReservationInterval;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.exceptions.PlanningException;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;

/**
 * YARN预留资源调度的低成本对齐阶段分配器，迭代在总成本最低的时间区间分配容器。
 * 算法仅考虑长度为指定duration的不重叠区间，保证分配对齐。
 * 如果allocateLeft为true，区间对齐到最早开始时间；否则对齐到截止时间。
 * smoothnessFactor参数控制每次迭代最多分配多少个容器组，平衡分配平滑度和性能。
 */

public class StageAllocatorLowCostAligned implements StageAllocator {

  private final boolean allocateLeft;
  // 平滑因子，控制每次迭代最大分配量
  private int smoothnessFactor = 10;

  /**
   * 构造函数，使用默认平滑因子10。
   * @param allocateLeft 是否左对齐（对齐到最早开始时间）
   */
  public StageAllocatorLowCostAligned(boolean allocateLeft) {
    this.allocateLeft = allocateLeft;
  }

  /**
   * 构造函数，指定平滑因子和对齐方式。
   * @param smoothnessFactor 平滑因子，控制每次迭代最大分配量
   * @param allocateLeft 是否左对齐
   */
  public StageAllocatorLowCostAligned(int smoothnessFactor,
      boolean allocateLeft) {
    this.allocateLeft = allocateLeft;
    this.smoothnessFactor = smoothnessFactor;
  }

  @Override
  public Map<ReservationInterval, Resource> computeStageAllocation(Plan plan,
      RLESparseResourceAllocation planLoads,
      RLESparseResourceAllocation planModifications, ReservationRequest rr,
      long stageArrival, long stageDeadline, long period, String user,
      ReservationId oldId) throws PlanningException {

    // 获取资源计算器
    ResourceCalculator resCalc = plan.getResourceCalculator();
    // 获取集群总容量
    Resource capacity = plan.getTotalCapacity();

    // 计算当前用户可用资源分布（排除原有同名预留）
    RLESparseResourceAllocation netRLERes = plan.getAvailableResourceOverTime(
        user, oldId, stageArrival, stageDeadline, period);

    // 获取时间步长
    long step = plan.getStep();

    // 初始化分配请求容器
    RLESparseResourceAllocation allocationRequests =
        new RLESparseResourceAllocation(plan.getResourceCalculator());

    // 向上对齐预留时长到步长整数倍
    long duration = stepRoundUp(rr.getDuration(), step);
    // 计算可选区间总数
    int windowSizeInDurations =
        (int) ((stageDeadline - stageArrival) / duration);
    // 计算总共需要分配的容器组数量（gang：同一时间并发的容器组）
    int totalGangs = rr.getNumContainers() / rr.getConcurrency();
    // 每个gang包含的容器数
    int numContainersPerGang = rr.getConcurrency();
    // 每个gang需要的总资源量
    Resource gang =
        Resources.multiply(rr.getCapability(), numContainersPerGang);

    // 计算每个区间最多可分配gang数，保证分配均匀
    int maxGangsPerUnit = (int) Math
        .max(Math.floor(((double) totalGangs) / windowSizeInDurations), 1);
    // 结合平滑因子限制每次最多分配量
    maxGangsPerUnit = Math.max(maxGangsPerUnit / smoothnessFactor, 1);

    // 窗口大小不足，无法分配，返回失败
    if (windowSizeInDurations <= 0) {
      return null;
    }

    // 排序偏好：左对齐优先选早结束区间，右对齐优先选晚结束区间
    final int preferLeft = allocateLeft ? 1 : -1;

    // 按总成本排序的候选区间红黑树，每次取成本最低的分配
    TreeSet<DurationInterval> durationIntervalsSortedByCost =
        new TreeSet<DurationInterval>(new Comparator<DurationInterval>() {
          @Override
          public int compare(DurationInterval val1, DurationInterval val2) {
            // 优先按总成本升序排序
            int cmp = Double.compare(val1.getTotalCost(), val2.getTotalCost());
            if (cmp != 0) {
              return cmp;
            }
            // 成本相同时按对齐规则排序
            return preferLeft
                * Long.compare(val1.getEndTime(), val2.getEndTime());
          }
        });

    // 计算所有候选区间的结束时间点
    List<Long> intervalEndTimes =
        computeIntervalEndTimes(stageArrival, stageDeadline, duration);

    // 遍历所有候选结束时间生成区间
    for (long intervalEnd : intervalEndTimes) {

      long intervalStart = intervalEnd - duration;

      // 计算区间的总成本和可容纳gang数
      DurationInterval durationInterval =
          getDurationInterval(intervalStart, intervalEnd, planLoads,
              planModifications, capacity, netRLERes, resCalc, step, gang);

      // 区间至少可容纳一个gang，加入候选集
      if (durationInterval.canAllocate()) {
        durationIntervalsSortedByCost.add(durationInterval);
      }
    }

    // 开始迭代分配
    int remainingGangs = totalGangs;
    while (remainingGangs > 0) {

      // 没有可用区间，分配失败，跳出循环
      if (durationIntervalsSortedByCost.isEmpty()) {
        break;
      }

      // 取出当前成本最低的候选区间
      DurationInterval bestDurationInterval =
          durationIntervalsSortedByCost.first();
      // 计算本次分配的gang数量（不超过最大限制、剩余需求、区间容量）
      int numGangsToAllocate = Math.min(maxGangsPerUnit, remainingGangs);
      numGangsToAllocate =
          Math.min(numGangsToAllocate, bestDurationInterval.numCanFit());
      // 更新剩余待分配gang数
      remainingGangs -= numGangsToAllocate;

      // 创建预留区间
      ReservationInterval reservationInt =
          new ReservationInterval(bestDurationInterval.getStartTime(),
              bestDurationInterval.getEndTime());

      // 计算本次需要分配的总资源
      Resource reservationRes = Resources.multiply(rr.getCapability(),
          rr.getConcurrency() * numGangsToAllocate);

      // 更新当前预留的修改记录
      planModifications.addInterval(reservationInt, reservationRes);
      // 添加到分配结果
      allocationRequests.addInterval(reservationInt, reservationRes);

      // 从候选集中移除该区间（已分配，需要更新容量）
      durationIntervalsSortedByCost.remove(bestDurationInterval);

      // 重新计算该区间分配后的容量和成本
      DurationInterval updatedDurationInterval =
          getDurationInterval(bestDurationInterval.getStartTime(),
              bestDurationInterval.getStartTime() + duration, planLoads,
              planModifications, capacity, netRLERes, resCalc, step, gang);

      // 更新后仍可分配，重新加入候选集
      if (updatedDurationInterval.canAllocate()) {
        durationIntervalsSortedByCost.add(updatedDurationInterval);
      }

    }

    // 转换分配结果为Map格式
    Map<ReservationInterval, Resource> allocations =
        allocationRequests.toIntervalMap();

    // 所有gang分配完成，返回结果
    if (remainingGangs <= 0) {
      return allocations;
    } else {

      // 分配失败，回滚已经做的修改
      for (Map.Entry<ReservationInterval, Resource> tempAllocation : allocations
          .entrySet()) {

        planModifications.removeInterval(tempAllocation.getKey(),
            tempAllocation.getValue());

      }
      // 返回null标识分配失败
      return null;

    }

  }

  /**
   * 根据对齐规则生成所有候选区间的结束时间列表。
   * @param stageEarliestStart 阶段最早开始时间
   * @param stageDeadline 阶段截止时间
   * @param duration 单个区间时长
   * @return 候选区间结束时间列表
   */
  private List<Long> computeIntervalEndTimes(long stageEarliestStart,
      long stageDeadline, long duration) {

    List<Long> intervalEndTimes = new ArrayList<Long>();
    if (!allocateLeft) {
      // 右对齐：从截止时间向左生成区间
      for (long intervalEnd = stageDeadline; intervalEnd >= stageEarliestStart
          + duration; intervalEnd -= duration) {
        intervalEndTimes.add(intervalEnd);
      }
    } else {
      // 左对齐：从最早开始时间向右生成区间
      for (long intervalStart =
          stageEarliestStart; intervalStart <= stageDeadline
              - duration; intervalStart += duration) {
        intervalEndTimes.add(intervalStart + duration);
      }
    }

    return intervalEndTimes;
  }

  /**
   * 计算指定时间区间的总成本和可容纳gang数量。
   * @param startTime 区间开始时间
   * @param endTime 区间结束时间
   * @param planLoads 已有负载
   * @param planModifications 当前预留新增负载
   * @param capacity 集群总容量
   * @param netRLERes 可用资源分布
   * @param resCalc 资源计算器
   * @param step 时间步长
   * @param requestedResources 每个gang需要的资源
   * @return 计算完成的DurationInterval对象
   * @throws PlanningException 规划异常
   */
  protected static DurationInterval getDurationInterval(long startTime,
      long endTime, RLESparseResourceAllocation planLoads,
      RLESparseResourceAllocation planModifications, Resource capacity,
      RLESparseResourceAllocation netRLERes, ResourceCalculator resCalc,
      long step, Resource requestedResources) throws PlanningException {

    // 计算区间总成本
    double totalCost = getDurationIntervalTotalCost(startTime, endTime,
        planLoads, planModifications, capacity, resCalc, step);

    // 计算区间可容纳的最大gang数量
    int gangsCanFit = getDurationIntervalGangsCanFit(startTime, endTime,
        planModifications, capacity, netRLERes, resCalc, requestedResources);

    // 构建并返回区间对象
    return new DurationInterval(startTime, endTime, totalCost, gangsCanFit);

  }

  /**
   * 计算指定区间的总成本（负载占总容量的比例累计和）。
   * @param startTime 区间开始时间
   * @param endTime 区间结束时间
   * @param planLoads 已有负载
   * @param planModifications 当前预留新增负载
   * @param capacity 集群总容量
   * @param resCalc 资源计算器
   * @param step 时间步长
   * @return 区间总成本
   * @throws PlanningException 规划异常
   */
  protected static double getDurationIntervalTotalCost(long startTime,
      long endTime, RLESparseResourceAllocation planLoads,
      RLESparseResourceAllocation planModifications, Resource capacity,
      ResourceCalculator resCalc, long step) throws PlanningException {

    // 合并已有负载和当前新增负载，得到区间总负载
    RLESparseResourceAllocation currentLoad =
        RLESparseResourceAllocation.merge(resCalc, capacity, planLoads,
            planModifications, RLEOperator.add, startTime, endTime);

    // 获取负载的分段表示
    NavigableMap<Long, Resource> mapCurrentLoad = currentLoad.getCumulative();

    // 初始化总成本
    double totalCost = 0.0;
    Long tPrev = -1L;
    Resource loadPrev = Resources.none();
    double cost = 0.0;

    // 遍历每个时间分段计算成本
    for (Entry<Long, Resource> e : mapCurrentLoad.entrySet()) {
      Long t = e.getKey();
      Resource load = e.getValue();
      if (tPrev != -1L) {
        // 截断到查询开始边界
        tPrev = Math.max(tPrev, startTime);
        // 计算当前分段负载的单位成本
        cost = calcCostOfLoad(loadPrev, capacity, resCalc);
        // 累加当前分段总成本 = 单位成本 * 分段包含的步数
        totalCost = totalCost + cost * (t - tPrev) / step;
      }

      tPrev = t;
      loadPrev = load;
    }

    // 处理最后一个分段（循环未覆盖）
    if (loadPrev != null) {
      // 截断到查询开始边界
      tPrev = Math.max(tPrev, startTime);
      // 计算最后分段成本
      cost = calcCostOfLoad(loadPrev, capacity, resCalc);
      totalCost = totalCost + cost * (endTime - tPrev) / step;
    }

    // 返回总成本
    return totalCost;
  }

  /**
   * 计算区间可容纳的最大gang数量，取区间内所有时间点的最小值。
   * @param startTime 区间开始时间
   * @param endTime 区间结束时间
   * @param planModifications 当前预留新增负载
   * @param capacity 集群总容量
   * @param netRLERes 可用资源分布
   * @param resCalc 资源计算器
   * @param requestedResources 每个gang需要的资源
   * @return 可容纳的最大gang数量
   * @throws PlanningException 规划异常
   */
  protected static int getDurationIntervalGangsCanFit(long startTime,
      long endTime, RLESparseResourceAllocation planModifications,
      Resource capacity, RLESparseResourceAllocation netRLERes,
      ResourceCalculator resCalc, Resource requestedResources)
      throws PlanningException {

    // 初始化为最大值，后续逐步取最小值
    int gangsCanFit = Integer.MAX_VALUE;
    int curGangsCanFit;

    // 计算扣除当前新增负载后的可用资源
    RLESparseResourceAllocation netAvailableResources =
        RLESparseResourceAllocation.merge(resCalc, capacity, netRLERes,
            planModifications, RLEOperator.subtractTestNonNegative, startTime,
            endTime);

    // 获取可用资源分段表示
    NavigableMap<Long, Resource> mapAvailableCapacity =
        netAvailableResources.getCumulative();

    // 遍历每个分段计算可容纳gang数
    for (Entry<Long, Resource> e : mapAvailableCapacity.entrySet()) {
      Long t = e.getKey();
      Resource curAvailable = e.getValue();
      // 超过区间结束，停止遍历
      if (t >= endTime) {
        break;
      }

      // 可用资源为空，无法容纳任何gang
      if (curAvailable == null) {
        gangsCanFit = 0;
      } else {
        // 计算当前时间点可容纳gang数
        curGangsCanFit = (int) Math.floor(Resources.divide(resCalc, capacity,
            curAvailable, requestedResources));
        // 更新最小值（整个区间容量由最紧时间点决定）
        if (curGangsCanFit < gangsCanFit) {
          gangsCanFit = curGangsCanFit;
        }
      }
    }
    return gangsCanFit;
  }

  /**
   * 按步长遍历计算区间总成本。
   * @param startTime 区间开始时间
   * @param endTime 区间结束时间
   * @param planLoads 已有负载
   * @param planModifications 当前预留新增负载
   * @param capacity 集群总容量
   * @param resCalc 资源计算器
   * @param step 时间步长
   * @return 区间总成本
   */
  protected double calcCostOfInterval(long startTime, long endTime,
      RLESparseResourceAllocation planLoads,
      RLESparseResourceAllocation planModifications, Resource capacity,
      ResourceCalculator resCalc, long step) {

    // 累加每个时间步成本
    double totalCost = 0.0;
    for (long t = startTime; t < endTime; t += step) {
      totalCost += calcCostOfTimeSlot(t, planLoads, planModifications, capacity,
          resCalc);
    }

    // 返回总成本
    return totalCost;

  }

  /**
   * 计算单个时间步的成本。
   * @