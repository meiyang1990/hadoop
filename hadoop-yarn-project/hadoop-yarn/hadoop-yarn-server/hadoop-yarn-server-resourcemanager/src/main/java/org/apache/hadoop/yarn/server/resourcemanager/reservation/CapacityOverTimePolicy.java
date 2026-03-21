// 这个文件已经全部加上中文注释
/*******************************************************************************
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *******************************************************************************/
package org.apache.hadoop.yarn.server.resourcemanager.reservation;

import org.apache.hadoop.classification.InterfaceAudience.LimitedPrivate;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.RLESparseResourceAllocation.RLEOperator;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.exceptions.PlanningException;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.exceptions.PlanningQuotaException;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueuePath;
import org.apache.hadoop.yarn.util.resource.Resources;

import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

/**
 * 基于时间窗口容量约束的预约配额策略，同时检查瞬时资源占用和滑动窗口内平均资源占用，允许瞬时峰值但限制长期平均用量，防止资源滥用。
 * 通过配置瞬时最大容量、平均容量和窗口长度，可以灵活调整策略：从严格瞬时容量限制到完全弹性分配均可支持。
 */
@LimitedPrivate("yarn")
@Unstable
public class CapacityOverTimePolicy extends NoOverCommitPolicy {

  private ReservationSchedulerConfiguration conf;
  private long validWindow;
  private float maxInst;
  private float maxAvg;

  /**
   * 初始化策略，从配置中读取当前预约队列的窗口长度、瞬时最大容量和平均容量参数。
   * @param reservationQueue 目标预约队列名称
   * @param conf 预约调度配置
   */
  @Override
  public void init(String reservationQueue,
      ReservationSchedulerConfiguration conf) {
    this.conf = conf;
    QueuePath reservationQueuePath = new QueuePath(reservationQueue);
    validWindow = this.conf.getReservationWindow(reservationQueuePath);
    maxInst = this.conf.getInstantaneousMaxCapacity(reservationQueuePath) / 100;
    maxAvg = this.conf.getAverageCapacity(reservationQueuePath) / 100;
  }

  /**
   * 验证新提交预约是否满足容量约束：先调用父类检查瞬时资源上限，再检查滑动窗口内平均资源用量不超过配额。
   * 算法通过检查RLE编码所有时间拐点+窗口对齐点，仅在这些点计算积分，相比逐时隙检查大幅降低计算量。
   * @param plan 当前集群资源分配计划
   * @param reservation 待验证的预约分配
   * @throws PlanningException 验证失败抛出异常
   */
  @Override
  public void validate(Plan plan, ReservationAllocation reservation)
      throws PlanningException {


    // 调用父类验证：检查用户匹配、集群物理容量限制、瞬时最大容量
    try {
      super.validate(plan, reservation);
    } catch (PlanningException p) {
      // 封装为配额异常抛出
      throw new PlanningQuotaException(p);
    }

    // 计算需要检查的时间范围：预约开始时间前推一个窗口，结束时间后推一个窗口
    long checkStart = reservation.getStartTime() - validWindow;
    long checkEnd = reservation.getEndTime() + validWindow;

    //---- 检查平均容量积分约束 --------

    // 获取用户已有的所有预约在检查范围内的资源消耗
    RLESparseResourceAllocation consumptionForUserOverTime =
        plan.getConsumptionForUserOverTime(reservation.getUser(),
            checkStart, checkEnd);

    // 如果是更新已有预约，先移除旧版本的资源占用
    ReservationAllocation old =
        plan.getReservationById(reservation.getReservationId());
    if (old != null) {
      consumptionForUserOverTime =
          RLESparseResourceAllocation.merge(plan.getResourceCalculator(),
              plan.getTotalCapacity(), consumptionForUserOverTime,
              old.getResourcesOverTime(checkStart, checkEnd), RLEOperator.add,
              checkStart, checkEnd);
    }

    // 获取本次新预约在检查范围内的资源占用
    RLESparseResourceAllocation resRLE =
        reservation.getResourcesOverTime(checkStart, checkEnd);

    // 合并得到添加新预约后，用户完整的资源占用时间线
    RLESparseResourceAllocation toCheck = RLESparseResourceAllocation
        .merge(plan.getResourceCalculator(), plan.getTotalCapacity(),
            consumptionForUserOverTime, resRLE, RLEOperator.add, Long.MIN_VALUE,
            Long.MAX_VALUE);

    // 存储积分上升沿和下降沿点：上升沿表示窗口起点开始累加，下降沿表示窗口终点停止累加
    NavigableMap<Long, Resource> integralUp = new TreeMap<>();
    NavigableMap<Long, Resource> integralDown = new TreeMap<>();

    long prevTime = toCheck.getEarliestStartTime();
    IntegralResource prevResource = new IntegralResource(0L, 0L);
    IntegralResource runningTot = new IntegralResource(0L, 0L);

    // 插入窗口对齐的中间检查点，确保每个完整窗口都有检查点
    Map<Long, Resource> temp = new TreeMap<>();
    for (Map.Entry<Long, Resource> pointToCheck : toCheck.getCumulative()
        .entrySet()) {

      Long timeToCheck = pointToCheck.getKey();
      Resource resourceToCheck = pointToCheck.getValue();

      // 获取下一个RLE时间点
      Long nextPoint = toCheck.getCumulative().higherKey(timeToCheck);
      if (nextPoint == null || toCheck.getCumulative().get(nextPoint) == null) {
        continue;
      }
      // 在当前RLE段内插入所有窗口对齐点
      for (int i = 1; i <= (nextPoint - timeToCheck) / validWindow; i++) {
        temp.put(timeToCheck + (i * validWindow), resourceToCheck);
      }
    }
    // 合并原有RLE拐点和新增窗口对齐点
    temp.putAll(toCheck.getCumulative());

    // 计算每个时间点的累计积分，生成上升沿和下降沿
    for (Map.Entry<Long, Resource> currPoint : temp.entrySet()) {

      Long currTime = currPoint.getKey();
      Resource currResource = currPoint.getValue();

      // 累加前一个时间段的积分贡献
      prevResource.multiplyBy(currTime - prevTime);
      runningTot.add(prevResource);
      // 在当前窗口起点记录累计积分
      integralUp.put(currTime, normalizeToResource(runningTot, validWindow));
      // 在当前窗口终点记录累计积分
      integralDown.put(currTime + validWindow,
          normalizeToResource(runningTot, validWindow));

      // 更新当前区间资源用量
      if (currResource != null) {
        prevResource.memory = currResource.getMemorySize();
        prevResource.vcores = currResource.getVirtualCores();
      } else {
        prevResource.memory = 0L;
        prevResource.vcores = 0L;
      }
      prevTime = currTime;
    }

    // 将上升沿和下降沿转换为RLE结构
    RLESparseResourceAllocation intUp =
        new RLESparseResourceAllocation(integralUp,
            plan.getResourceCalculator());
    RLESparseResourceAllocation intDown =
        new RLESparseResourceAllocation(integralDown,
            plan.getResourceCalculator());

    // 相减得到每个时间点对应的窗口平均资源用量
    RLESparseResourceAllocation integral = RLESparseResourceAllocation
        .merge(plan.getResourceCalculator(), plan.getTotalCapacity(), intUp,
            intDown, RLEOperator.subtract, Long.MIN_VALUE, Long.MAX_VALUE);

    // 构造平均资源配额上限
    NavigableMap<Long, Resource> tlimit = new TreeMap<>();
    Resource maxAvgRes = Resources.multiply(plan.getTotalCapacity(), maxAvg);
    tlimit.put(toCheck.getEarliestStartTime() - validWindow, maxAvgRes);
    RLESparseResourceAllocation targetLimit =
        new RLESparseResourceAllocation(tlimit, plan.getResourceCalculator());

    // 检查所有时间点窗口平均用量是否都不超过配额，若超过则抛出异常
    try {

      RLESparseResourceAllocation.merge(plan.getResourceCalculator(),
          plan.getTotalCapacity(), targetLimit, integral,
          RLEOperator.subtractTestNonNegative, checkStart, checkEnd);

    } catch (PlanningException p) {
      throw new PlanningQuotaException(
          "Integral (avg over time) quota capacity " + maxAvg
              + " over a window of " + validWindow / 1000 + " seconds, "
              + " would be exceeded by accepting reservation: " + reservation
              .getReservationId(), p);
    }
  }

  /**
   * 将累积积分归一化为窗口平均资源，转换为Resource对象。
   * @param runningTot 累计积分
   * @param window 窗口长度
   * @return 归一化后的平均资源
   */
  private Resource normalizeToResource(IntegralResource runningTot,
      long window) {
    // 归一化到窗口平均，四舍五入，Resource当前使用int存储，后续会改为long
    int memory = (int) Math.round((double) runningTot.memory / window);
    int vcores = (int) Math.round((double) runningTot.vcores / window);
    return Resource.newInstance(memory, vcores);
  }

  /**
   * 计算当前队列中用户可用的瞬时资源，扣除用户已用资源后与全局可用资源取较小值。
   * @param available 全局可用资源时间线
   * @param plan 当前资源分配计划
   * @param user 用户名
   * @param oldId 如果是更新预约，原有预约ID
   * @param start 起始时间
   * @param end 结束时间
   * @return 用户瞬时可用资源时间线
   * @throws PlanningException 计算过程异常
   */
  @Override
  public RLESparseResourceAllocation availableResources(
      RLESparseResourceAllocation available, Plan plan, String user,
      ReservationId oldId, long start, long end) throws PlanningException {

    // 计算瞬时最大可用资源配额
    Resource planTotalCapacity = plan.getTotalCapacity();
    Resource maxInsRes = Resources.multiply(planTotalCapacity, maxInst);
    NavigableMap<Long, Resource> instQuota = new TreeMap<Long, Resource>();
    instQuota.put(start, maxInsRes);

    RLESparseResourceAllocation instRLEQuota =
        new RLESparseResourceAllocation(instQuota,
            plan.getResourceCalculator());

    // 获取用户当前已用资源时间线
    RLESparseResourceAllocation used =
        plan.getConsumptionForUserOverTime(user, start, end);

    // 如果是更新预约，加回原有预约占用的资源
    ReservationAllocation old = plan.getReservationById(oldId);
    if (old != null) {
      used = RLESparseResourceAllocation.merge(plan.getResourceCalculator(),
          Resources.clone(plan.getTotalCapacity()), used,
          old.getResourcesOverTime(start, end), RLEOperator.subtract, start,
          end);
    }

    // 从配额中扣除已用资源得到剩余瞬时可用
    instRLEQuota = RLESparseResourceAllocation
        .merge(plan.getResourceCalculator(), planTotalCapacity, instRLEQuota,
            used, RLEOperator.subtract, start, end);

    // 和全局可用资源取较小值，得到最终可用
    instRLEQuota = RLESparseResourceAllocation
        .merge(plan.getResourceCalculator(), planTotalCapacity, available,
            instRLEQuota, RLEOperator.min, start, end);

    return instRLEQuota;
  }

  /**
   * 获取当前策略配置的有效检查窗口长度。
   * @return 窗口长度（毫秒）
   */
  @Override
  public long getValidWindow() {
    return validWindow;
  }

  /**
   * 使用long存储累积积分的辅助类，避免Resource使用int存储导致积分溢出。
   * 行为与DefaultResourceCalculator保持一致，待Resource改为long后可移除。
   */
  private static class IntegralResource {
    long memory;
    long vcores;

    public IntegralResource(Resource resource) {
      this.memory = resource.getMemorySize();
      this.vcores = resource.getVirtualCores();
    }

    public IntegralResource(long mem, long vcores) {
      this.memory = mem;
      this.vcores = vcores;
    }

    public void add(Resource r) {
      memory += r.getMemorySize();
      vcores += r.getVirtualCores();
    }

    public void add(IntegralResource r) {
      memory += r.memory;
      vcores += r.vcores;
    }

    public void subtract(Resource r) {
      memory -= r.getMemorySize();
      vcores -= r.getVirtualCores();
    }

    public IntegralResource negate() {
      return new IntegralResource(-memory, -vcores);
    }

    public void multiplyBy(long window) {
      memory = memory * window;
      vcores = vcores * window;
    }

    public long compareTo(IntegralResource other) {
      long diff = memory - other.memory;
      if (diff == 0) {
        diff = vcores - other.vcores;
      }
      return diff;
    }

    @Override
    public String toString() {
      return "<memory:" + memory + ", vCores:" + vcores + ">";
    }

  }

}