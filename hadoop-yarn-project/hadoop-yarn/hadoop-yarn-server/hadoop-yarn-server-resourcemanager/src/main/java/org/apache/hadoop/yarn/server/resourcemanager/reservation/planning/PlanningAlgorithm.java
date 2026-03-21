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

import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ReservationDefinition;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.InMemoryReservationAllocation;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.PeriodicRLESparseResourceAllocation;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.Plan;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.RLESparseResourceAllocation;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.ReservationAllocation;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.ReservationInterval;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.exceptions.ContractValidationException;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.exceptions.PlanningException;

/**
 * YARN容量预留规划算法的抽象基类，定义了预留规划的通用执行流程，子类只需实现具体分配逻辑。
 */
public abstract class PlanningAlgorithm implements ReservationAgent {

  /**
   * 在指定规划Plan中为预留定义执行实际资源分配。
   *
   * @param reservationId 预留ID
   * @param user 预留所属用户
   * @param plan 要分配资源的目标规划对象
   * @param contract 用户请求的资源要求
   * @param oldReservation 已存在的旧预留，新建请求时为null
   * @return 分配是否成功
   *
   * @throws PlanningException 无法找到可用资源分配时抛出
   * @throws ContractValidationException 预留请求验证失败时抛出
   */
  protected boolean allocateUser(ReservationId reservationId, String user,
      Plan plan, ReservationDefinition contract,
      ReservationAllocation oldReservation) throws PlanningException,
      ContractValidationException {

    // 调整预留定义，处理系统层面的不完美（如大容器调度延迟）
    ReservationDefinition adjustedContract = adjustContract(plan, contract);

    // 计算任务的资源分配
    RLESparseResourceAllocation allocation =
            computeJobAllocation(plan, reservationId, adjustedContract, user);

    long period = Long.parseLong(contract.getRecurrenceExpression());

    // 如果是周期性请求，封装为周期性分配对象
    if (contract.getRecurrenceExpression() != null) {
      if (period > 0) {
        allocation =
            new PeriodicRLESparseResourceAllocation(allocation, period);
      }
    }

    // 未找到有效分配，抛出异常
    if (allocation == null) {
      throw new PlanningException(
              "The planning algorithm could not find a valid allocation"
                      + " for your request");
    }

    // 转换分配结果为带零填充的区间资源映射
    long step = plan.getStep();

    // 将起止时间对齐到规划步长
    long jobArrival = stepRoundUp(adjustedContract.getArrival(), step);
    long jobDeadline = stepRoundUp(adjustedContract.getDeadline(), step);

    // 转换分配结果，补充起止位置的零资源填充
    Map<ReservationInterval, Resource> mapAllocations =
        allocationsToPaddedMap(allocation, jobArrival, jobDeadline, period);

    // 创建预留分配对象
    ReservationAllocation capReservation =
        new InMemoryReservationAllocation(reservationId, // ID
            adjustedContract, // 调整后的资源契约
            user, // 用户名
            plan.getQueueName(), // 队列名
            adjustedContract.getArrival(), adjustedContract.getDeadline(),
            mapAllocations, // 分配结果
            plan.getResourceCalculator(), // 资源计算器
            plan.getMinimumAllocation()); // 最小分配单元

    // 添加或更新预留到规划中
    if (oldReservation != null) {
      return plan.updateReservation(capReservation);
    } else {
      return plan.addReservation(capReservation, false);
    }

  }

  /**
   * 将分配结果转换为带零填充的区间-资源映射，保证覆盖整个请求时间范围。
   *
   * @param allocation 计算得到的分配结果
   * @param jobArrival 对齐后的请求开始时间
   * @param jobDeadline 对齐后的请求结束时间
   * @param period 周期性周期，非周期为0
   * @return 带零填充的区间资源映射
   */
  private Map<ReservationInterval, Resource> allocationsToPaddedMap(
      RLESparseResourceAllocation allocation, long jobArrival, long jobDeadline,
      long period) {

    // 零资源实例
    Resource zeroResource = Resource.newInstance(0, 0);

    if (period > 0) {
      // 如果单次请求时长超过周期，添加整周期零填充
      if ((jobDeadline - jobArrival) >= period) {
        allocation.addInterval(new ReservationInterval(0L, period),
            zeroResource);
      }
      // 对周期内取模得到相对起止时间
      jobArrival = jobArrival % period;
      jobDeadline = jobDeadline % period;

      if (jobArrival <= jobDeadline) {
        // 起止都在同一周期区间，填充前后两端
        allocation.addInterval(new ReservationInterval(0, jobArrival),
            zeroResource);
        allocation.addInterval(new ReservationInterval(jobDeadline, period),
            zeroResource);
      } else {
        // 跨周期，填充中间空闲区间
        allocation.addInterval(new ReservationInterval(jobDeadline, jobArrival),
            zeroResource);
      }
    } else {
      // 非周期性请求，填充开始前空闲区间
      long earliestStart = findEarliestTime(allocation.toIntervalMap());
      if (jobArrival < earliestStart) {
        allocation.addInterval(
            new ReservationInterval(jobArrival, earliestStart), zeroResource);
      }

      // 填充结束后空闲区间
      long latestEnd = findLatestTime(allocation.toIntervalMap());
      if (latestEnd < jobDeadline) {
        allocation.addInterval(new ReservationInterval(latestEnd, jobDeadline),
            zeroResource);
      }
    }
    return allocation.toIntervalMap();
  }

  /**
   * 抽象方法：由子类实现具体的作业资源分配计算。
   *
   * @param plan 目标规划对象
   * @param reservationId 预留ID
   * @param reservation 调整后的预留定义
   * @param user 预留所属用户
   * @return 计算得到的资源分配结果
   * @throws PlanningException 分配失败时抛出
   * @throws ContractValidationException 请求验证失败时抛出
   */
  public abstract RLESparseResourceAllocation computeJobAllocation(Plan plan,
      ReservationId reservationId, ReservationDefinition reservation,
      String user) throws PlanningException, ContractValidationException;

  @Override
  public boolean createReservation(ReservationId reservationId, String user,
      Plan plan, ReservationDefinition contract) throws PlanningException {
    // 调用通用分配逻辑，新建预留
    return allocateUser(reservationId, user, plan, contract, null);
  }

  @Override
  public boolean updateReservation(ReservationId reservationId, String user,
      Plan plan, ReservationDefinition contract) throws PlanningException {
    // 获取旧预留信息
    ReservationAllocation oldAlloc = plan.getReservationById(reservationId);
    // 调用通用分配逻辑，更新预留
    return allocateUser(reservationId, user, plan, contract, oldAlloc);
  }

  @Override
  public boolean deleteReservation(ReservationId reservationId, String user,
      Plan plan) throws PlanningException {
    // 调用规划接口删除现有预留
    return plan.deleteReservation(reservationId);
  }

  /**
   * 查找所有区间中最早的开始时间。
   * @param sesInt 区间资源映射
   * @return 最早开始时间
   */
  protected static long findEarliestTime(
      Map<ReservationInterval, Resource> sesInt) {

    long ret = Long.MAX_VALUE;
    for (Entry<ReservationInterval, Resource> s : sesInt.entrySet()) {
      if (s.getKey().getStartTime() < ret && s.getValue() != null) {
        ret = s.getKey().getStartTime();
      }
    }
    return ret;
  }

  /**
   * 查找所有区间中最晚的结束时间。
   * @param sesInt 区间资源映射
   * @return 最晚结束时间
   */
  protected static long findLatestTime(Map<ReservationInterval,
      Resource> sesInt) {

    long ret = Long.MIN_VALUE;
    for (Entry<ReservationInterval, Resource> s : sesInt.entrySet()) {
      if (s.getKey().getEndTime() > ret && s.getValue() != null) {
        ret = s.getKey().getEndTime();
      }
    }
    return ret;
  }

  /**
   * 向下按步长对齐时间。
   * @param t 原始时间
   * @param step 步长
   * @return 对齐后的时间
   */
  protected static long stepRoundDown(long t, long step) {
    return (t / step) * step;
  }

  /**
   * 向上按步长对齐时间。
   * @param t 原始时间
   * @param step 步长
   * @return 对齐后的时间
   */
  protected static long stepRoundUp(long t, long step) {
    return ((t + step - 1) / step) * step;
  }

  /**
   * 调整原始预留契约，处理系统层面的不完美（如大容器调度延迟），当前实现直接返回原契约。
   * @param plan 目标规划对象
   * @param originalContract 原始预留契约
   * @return 调整后的预留契约
   */
  private ReservationDefinition adjustContract(Plan plan,
      ReservationDefinition originalContract) {

    // 预留扩展点：可在此添加调整逻辑，例如基于队列指标调整资源需求
    return originalContract;
  }

  @Override
  public void init(Configuration conf) {
  }
}