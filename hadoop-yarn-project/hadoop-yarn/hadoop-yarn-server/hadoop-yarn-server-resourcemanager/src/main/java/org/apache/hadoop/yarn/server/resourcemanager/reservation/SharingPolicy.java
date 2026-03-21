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

import org.apache.hadoop.classification.InterfaceAudience.LimitedPrivate;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.exceptions.PlanningException;

/**
 * YARN资源预留共享策略接口，定义了验证新预留分配是否符合队列资源共享规则的契约。
 * 不同实现类会 enforce 不同的资源共享约束规则，保障集群资源在预留任务间合理分配。
 */
@LimitedPrivate("yarn")
@Unstable
public interface SharingPolicy {

  /**
   * 初始化共享策略，绑定到对应计划队列并加载配置参数。
   * 
   * @param planQueuePath 当前计划所属队列的路径
   * @param conf 预留调度配置对象
   */
  void init(String planQueuePath, ReservationSchedulerConfiguration conf);

  /**
   * 执行共享策略验证，检查新预留分配是否符合规则，不符合则抛出异常。
   * 
   * @param plan 当前资源计划，包含已分配的所有预留信息
   * @param newAllocation 拟添加到计划的新预留分配
   * @throws PlanningException 若分配不符合策略规则则抛出此异常
   */
  void validate(Plan plan, ReservationAllocation newAllocation)
      throws PlanningException;

  /**
   * 基于业务规则快速计算当前策略下剩余可用资源，用于预留规划阶段快速剪枝。
   * 例如限制单个用户最大并行容器数，会根据已有用户预留计算剩余可用量。
   *
   * @param available 不考虑当前策略约束时的可用资源
   * @param plan 当前资源计划引用
   * @param user 提交预留请求的用户名
   * @param oldId (可选) 正在更新的原有预留ID，计算时需排除原有预留占用
   * @param start 查询时间范围的起始时间戳
   * @param end 查询时间范围的结束时间戳
   *
   * @return 应用当前策略后的可用资源，基于RLE稀疏数组表示
   *
   * @throws PlanningException 请求无效时抛出异常
   */
  RLESparseResourceAllocation availableResources(
      RLESparseResourceAllocation available, Plan plan, String user,
      ReservationId oldId, long start, long end) throws PlanningException;

  /**
   * 获取当前策略下预留的有效时间窗口，用于过期预留归档清理。
   * 早于(当前时间 - 有效窗口)的预留可以从活动计划中安全删除归档。
   * 
   * @return 策略定义的有效时间窗口大小，单位毫秒
   */
  long getValidWindow();

}