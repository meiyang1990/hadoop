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

package org.apache.hadoop.yarn.server.resourcemanager.reservation;

import java.util.List;

import org.apache.hadoop.yarn.api.protocolrecords.ReservationDeleteRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationListRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationSubmissionRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationUpdateRequest;
import org.apache.hadoop.yarn.api.records.ReservationDefinition;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.api.records.ReservationRequest;
import org.apache.hadoop.yarn.api.records.ReservationRequestInterpreter;
import org.apache.hadoop.yarn.api.records.ReservationRequests;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.ipc.RPCUtil;
import org.apache.hadoop.yarn.server.resourcemanager.RMAuditLogger;
import org.apache.hadoop.yarn.server.resourcemanager.RMAuditLogger.AuditConstants;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Queue;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.resource.Resources;

/**
 * YARN资源预约请求输入验证器，负责对各类预约操作请求进行前置合法性校验，
 * 提前发现非法请求并抛出错误信息，遵循fail-fast设计原则。
 */
public class ReservationInputValidator {

  private final Clock clock;

  /**
   * 构造预约输入验证器，使用指定时钟获取当前时间用于时间校验。
   * @param clock 时钟实例，用于获取当前时间
   */
  public ReservationInputValidator(Clock clock) {
    this.clock = clock;
  }

  /**
   * 验证指定预约ID是否存在且关联合法规划。
   * @param reservationSystem 预约系统实例
   * @param reservationId 待验证预约ID
   * @param auditConstant 审计日志常量，标识操作类型
   * @return 预约关联的规划实例
   * @throws YarnException 验证失败时抛出异常
   */
  private Plan validateReservation(ReservationSystem reservationSystem,
      ReservationId reservationId, String auditConstant) throws YarnException {
    // 检查预约ID非空
    if (reservationId == null) {
      String message = "Missing reservation id."
          + " Please try again by specifying a reservation id.";
      RMAuditLogger.logFailure("UNKNOWN", auditConstant,
          "validate reservation input", "ClientRMService", message);
      throw RPCUtil.getRemoteException(message);
    }
    // 根据预约ID获取所属队列
    String queue = reservationSystem.getQueueForReservation(reservationId);
    String nullQueueErrorMessage =
        "The specified reservation with ID: " + reservationId
            + " is unknown. Please try again with a valid reservation.";
    String nullPlanErrorMessage = "The specified reservation: " + reservationId
        + " is not associated with any valid plan."
        + " Please try again with a valid reservation.";
    // 从队列获取对应规划并验证合法性
    return getPlanFromQueue(reservationSystem, queue, auditConstant,
        nullQueueErrorMessage, nullPlanErrorMessage);
  }

  /**
   * 验证预约定义的合法性，检查各项参数是否符合约束要求。
   * @param reservationId 预约ID
   * @param contract 预约定义实例
   * @param plan 预约关联规划
   * @param auditConstant 审计日志常量，标识操作类型
   * @throws YarnException 验证失败时抛出异常
   */
  private void validateReservationDefinition(ReservationId reservationId,
      ReservationDefinition contract, Plan plan, String auditConstant)
      throws YarnException {
    String message = "";
    // 检查预约定义非空
    if (contract == null) {
      message = "Missing reservation definition."
          + " Please try again by specifying a reservation definition.";
      RMAuditLogger.logFailure("UNKNOWN", auditConstant,
          "validate reservation input definition", "ClientRMService", message);
      throw RPCUtil.getRemoteException(message);
    }
    // 检查截止时间不能是过去时间
    if (contract.getDeadline() <= clock.getTime()) {
      message = "The specified deadline: " + contract.getDeadline()
          + " is the past. Please try again with deadline in the future.";
      RMAuditLogger.logFailure("UNKNOWN", auditConstant,
          "validate reservation input definition", "ClientRMService", message);
      throw RPCUtil.getRemoteException(message);
    }
    // 检查预约资源请求非空
    ReservationRequests resReqs = contract.getReservationRequests();
    if (resReqs == null) {
      message = "No resources have been specified to reserve."
          + "Please try again by specifying the resources to reserve.";
      RMAuditLogger.logFailure("UNKNOWN", auditConstant,
          "validate reservation input definition", "ClientRMService", message);
      throw RPCUtil.getRemoteException(message);
    }
    List<ReservationRequest> resReq = resReqs.getReservationResources();
    if (resReq == null || resReq.isEmpty()) {
      message = "No resources have been specified to reserve."
          + " Please try again by specifying the resources to reserve.";
      RMAuditLogger.logFailure("UNKNOWN", auditConstant,
          "validate reservation input definition", "ClientRMService", message);
      throw RPCUtil.getRemoteException(message);
    }
    // 计算最小总持续时间和最大资源需求
    long minDuration = 0;
    Resource maxGangSize = Resource.newInstance(0, 0);
    ReservationRequestInterpreter type =
        contract.getReservationRequests().getInterpreter();
    // 遍历所有资源请求计算聚合值
    for (ReservationRequest rr : resReq) {
      // 根据解释类型不同计算最小持续时间
      if (type == ReservationRequestInterpreter.R_ALL
          || type == ReservationRequestInterpreter.R_ANY) {
        // 并行请求取最大时长
        minDuration = Math.max(minDuration, rr.getDuration());
      } else {
        // 串行请求累加时长
        minDuration += rr.getDuration();
      }
      // 累加计算最大资源需求（并发数 * 单请求资源）
      maxGangSize = Resources.max(plan.getResourceCalculator(),
          plan.getTotalCapacity(), maxGangSize,
          Resources.multiply(rr.getCapability(), rr.getConcurrency()));
    }
    // 验证时间窗口满足最小持续时间要求（R_ANY跳过校验）
    long duration = contract.getDeadline() - contract.getArrival();
    if (duration < minDuration && type != ReservationRequestInterpreter.R_ANY) {
      message = "The time difference (" + (duration) + ") between arrival ("
          + contract.getArrival() + ") " + "and deadline ("
          + contract.getDeadline() + ") must "
          + " be greater or equal to the minimum resource duration ("
          + minDuration + ")";
      RMAuditLogger.logFailure("UNKNOWN", auditConstant,
          "validate reservation input definition", "ClientRMService", message);
      throw RPCUtil.getRemoteException(message);
    }
    // 验证最大资源需求不超过队列总容量（R_ANY跳过校验）
    if (Resources.greaterThan(plan.getResourceCalculator(),
        plan.getTotalCapacity(), maxGangSize, plan.getTotalCapacity())
        && type != ReservationRequestInterpreter.R_ANY) {
      message = "The size of the largest gang in the reservation definition ("
          + maxGangSize + ") exceed the capacity available ("
          + plan.getTotalCapacity() + " )";
      RMAuditLogger.logFailure("UNKNOWN", auditConstant,
          "validate reservation input definition", "ClientRMService", message);
      throw RPCUtil.getRemoteException(message);
    }
    // 验证周期预约的周期表达式合法性
    String recurrenceExpression = contract.getRecurrenceExpression();
    try {
      long recurrence = Long.parseLong(recurrenceExpression);
      // 周期不能为负
      if (recurrence < 0) {
        message = "Negative Period : " + recurrenceExpression + ". Please try"
            + " again with a non-negative long value as period.";
        throw RPCUtil.getRemoteException(message);
      }
      // 周期预约的单次时长不能超过周期
      if (recurrence > 0 && duration > recurrence) {
        message = "Duration of the requested reservation: " + duration
            + " is greater than the recurrence: " + recurrence
            + ". Please try again with a smaller duration.";
        throw RPCUtil.getRemoteException(message);
      }
      // 周期必须能整除规划支持的最大周期
      if (recurrence > 0 && plan.getMaximumPeriodicity() % recurrence != 0) {
        message = "The maximum periodicity: " + plan.getMaximumPeriodicity() +
            " must be divisible by the recurrence expression provided: " +
            recurrence + ". Please try again with a recurrence expression" +
            " that satisfies this requirement.";
        throw RPCUtil.getRemoteException(message);
      }
    } catch (NumberFormatException e) {
      message = "Invalid period " + recurrenceExpression + ". Please try"
          + " again with a non-negative long value as period.";
      throw RPCUtil.getRemoteException(message);
    }
  }

  /**
   * 根据队列名称获取对应规划并使用默认错误信息验证。
   * @param reservationSystem 预约系统实例
   * @param queue 队列名称
   * @param auditConstant 审计日志常量，标识操作类型
   * @return 对应规划实例
   * @throws YarnException 验证失败时抛出异常
   */
  private Plan getPlanFromQueue(ReservationSystem reservationSystem,
      String queue, String auditConstant) throws YarnException {
    String nullQueueErrorMessage = "The queue is not specified."
        + " Please try again with a valid reservable queue.";
    String nullPlanErrorMessage = "The specified queue: " + queue
        + " is not managed by reservation system."
        + " Please try again with a valid reservable queue.";
    return getPlanFromQueue(reservationSystem, queue, auditConstant,
        nullQueueErrorMessage, nullPlanErrorMessage);
  }

  /**
   * 根据队列名称获取对应规划并使用自定义错误信息验证。
   * @param reservationSystem 预约系统实例
   * @param queue 队列名称
   * @param auditConstant 审计日志常量，标识操作类型
   * @param nullQueueErrorMessage 队列为空时的错误信息
   * @param nullPlanErrorMessage 规划不存在时的错误信息
   * @return 对应规划实例
   * @throws YarnException 验证失败时抛出异常
   */
  private Plan getPlanFromQueue(ReservationSystem reservationSystem,
      String queue, String auditConstant, String nullQueueErrorMessage,
      String nullPlanErrorMessage) throws YarnException {
    // 检查队列名称非空
    if (queue == null || queue.isEmpty()) {
      RMAuditLogger.logFailure("UNKNOWN", auditConstant,
          "validate reservation input", "ClientRMService",
          nullQueueErrorMessage);
      throw RPCUtil.getRemoteException(nullQueueErrorMessage);
    }
    // 从预约系统获取队列关联规划
    Plan plan = reservationSystem.getPlan(queue);
    // 检查规划存在
    if (plan == null) {
      RMAuditLogger.logFailure("UNKNOWN", auditConstant,
          "validate reservation input", "ClientRMService",
          nullPlanErrorMessage);
      throw RPCUtil.getRemoteException(nullPlanErrorMessage);
    }
    return plan;
  }

  /**
   * 验证预约提交请求的合法性。
   * @param reservationSystem 预约系统实例
   * @param request 预约提交请求
   * @param reservationId 预约ID
   * @return 对应规划实例
   * @throws YarnException 验证失败时抛出异常
   */
  public Plan validateReservationSubmissionRequest(
      ReservationSystem reservationSystem, ReservationSubmissionRequest request,
      ReservationId reservationId) throws YarnException {
    String message;
    // 检查预约ID非空
    if (reservationId == null) {
      message = "Reservation id cannot be null. Please try again specifying "
          + " a valid reservation id by creating a new reservation id.";
      throw RPCUtil.getRemoteException(message);
    }
    // 检查队列合法性并获取规划
    String queue = request.getQueue();
    Plan plan = getPlanFromQueue(reservationSystem, queue,
        AuditConstants.SUBMIT_RESERVATION_REQUEST);

    // 验证预约定义合法性
    validateReservationDefinition(reservationId,
        request.getReservationDefinition(), plan,
        AuditConstants.SUBMIT_RESERVATION_REQUEST);
    return plan;
  }

  /**
   * 验证预约更新请求的合法性。
   * @param reservationSystem 预约系统实例
   * @param request 预约更新请求
   * @return 对应规划实例
   * @throws YarnException 验证失败时抛出异常
   */
  public Plan validateReservationUpdateRequest(
      ReservationSystem reservationSystem, ReservationUpdateRequest request)
      throws YarnException {
    ReservationId reservationId = request.getReservationId();
    // 验证预约ID存在性
    Plan plan = validateReservation(reservationSystem, reservationId,
        AuditConstants.UPDATE_RESERVATION_REQUEST);
    // 验证新预约定义合法性
    validateReservationDefinition(reservationId,
        request.getReservationDefinition(), plan,
        AuditConstants.UPDATE_RESERVATION_REQUEST);
    return plan;
  }

  /**
   * 验证预约列表查询请求的合法性。
   * @param reservationSystem 预约系统实例
   * @param request 预约列表查询请求
   * @return 对应规划实例
   * @throws YarnException 验证失败时抛出异常
   */
  public Plan validateReservationListRequest(
      ReservationSystem reservationSystem, ReservationListRequest request)
      throws YarnException {
    String queue = request.getQueue();
    // 检查结束时间晚于开始时间
    if (request.getEndTime() < request.getStartTime()) {
      String errorMessage = "The specified end time must be greater than "
          + "the specified start time.";
      RMAuditLogger.logFailure("UNKNOWN",
          AuditConstants.LIST_RESERVATION_REQUEST,
          "validate list reservation input", "ClientRMService", errorMessage);
      throw RPCUtil.getRemoteException(errorMessage);
    }
    // 检查队列合法性并获取规划
    return getPlanFromQueue(reservationSystem, queue,
        AuditConstants.LIST_RESERVATION_REQUEST);
  }

  /**
   * 验证预约删除请求的合法性。
   * @param reservationSystem 预约系统实例
   * @param request 预约删除请求
   * @return 对应规划实例
   * @throws YarnException 验证失败时抛出异常
   */
  public Plan validateReservationDeleteRequest(
      ReservationSystem reservationSystem, ReservationDeleteRequest request)
      throws YarnException {
    return validateReservation(reservationSystem, request.getReservationId(),
        AuditConstants.DELETE_RESERVATION_REQUEST);
  }
}