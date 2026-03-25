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

package org.apache.hadoop.yarn.server.resourcemanager.monitor.capacity;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;


/**
 * 容量调度抢占计算过程中，存储单个应用在分区上的临时资源统计数据的数据结构
 * 用于跟踪单个应用的资源可用量、待分配资源需求和当前资源使用情况
 */
public class TempAppPerPartition extends AbstractPreemptionEntity {

  // 以下字段在计算过程中保持不变，供抢占候选选择策略使用
  private final int priority;
  private final ApplicationId applicationId;
  private TempUserPerPartition tempUser;

  FiCaSchedulerApp app;

  /**
   * 构造单个应用分区的临时抢占统计数据
   * @param app 调度器中的应用对象
   * @param usedPerPartition 当前分区已使用资源
   * @param amUsedPerPartition 当前分区应用master占用资源
   * @param reserved 当前分区预留资源
   * @param pendingPerPartition 当前分区待分配资源
   */
  TempAppPerPartition(FiCaSchedulerApp app, Resource usedPerPartition,
      Resource amUsedPerPartition, Resource reserved,
      Resource pendingPerPartition) {
    super(app.getQueueName(), usedPerPartition, amUsedPerPartition, reserved,
        pendingPerPartition);

    this.priority = app.getPriority().getPriority();
    this.applicationId = app.getApplicationId();
    this.app = app;
  }

  public FiCaSchedulerApp getFiCaSchedulerApp() {
    return app;
  }

  /**
   * 累加需要抢占的资源量
   * @param killable 本次新增可抢占的资源
   */
  public void assignPreemption(Resource killable) {
    Resources.addTo(toBePreempted, killable);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(" NAME: " + getApplicationId()).append(" PRIO: ").append(priority)
        .append(" CUR: ").append(getUsed()).append(" PEN: ").append(pending)
        .append(" RESERVED: ").append(reserved).append(" IDEAL_ASSIGNED: ")
        .append(idealAssigned).append(" PREEMPT_OTHER: ")
        .append(getToBePreemptFromOther()).append(" IDEAL_PREEMPT: ")
        .append(toBePreempted).append(" ACTUAL_PREEMPT: ")
        .append(getActuallyToBePreempted()).append("\n");

    return sb.toString();
  }

  /**
   * 拼接日志输出字符串，输出当前应用分区资源统计信息
   * @param sb 用于拼接日志的StringBuilder
   */
  void appendLogString(StringBuilder sb) {
    sb.append(queueName).append(", ").append(getUsed().getMemorySize())
        .append(", ").append(getUsed().getVirtualCores()).append(", ")
        .append(pending.getMemorySize()).append(", ")
        .append(pending.getVirtualCores()).append(", ")
        .append(idealAssigned.getMemorySize()).append(", ")
        .append(idealAssigned.getVirtualCores()).append(", ")
        .append(toBePreempted.getMemorySize()).append(", ")
        .append(toBePreempted.getVirtualCores()).append(", ")
        .append(getActuallyToBePreempted().getMemorySize()).append(", ")
        .append(getActuallyToBePreempted().getVirtualCores());
  }

  public int getPriority() {
    return priority;
  }

  public ApplicationId getApplicationId() {
    return applicationId;
  }

  public String getUser() {
    return this.app.getUser();
  }

  /**
   * 扣除实际已完成抢占的资源量
   * @param resourceCalculator 资源计算器
   * @param cluster 集群总资源
   * @param toBeDeduct 需要扣除的资源量
   */
  public void deductActuallyToBePreempted(ResourceCalculator resourceCalculator,
      Resource cluster, Resource toBeDeduct) {
    if (Resources.greaterThan(resourceCalculator, cluster,
        getActuallyToBePreempted(), toBeDeduct)) {
      Resources.subtractFrom(getActuallyToBePreempted(), toBeDeduct);
    }
  }

  public void setTempUserPerPartition(TempUserPerPartition tu) {
    tempUser = tu;
  }

  public TempUserPerPartition getTempUserPerPartition() {
    return tempUser;
  }
}