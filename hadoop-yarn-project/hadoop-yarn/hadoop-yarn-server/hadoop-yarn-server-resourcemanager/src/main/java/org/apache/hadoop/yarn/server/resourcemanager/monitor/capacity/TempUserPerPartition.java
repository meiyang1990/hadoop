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

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.UsersManager.User;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;

/**
 * 容量抢占计算器使用的临时数据结构，记录单个分区下单个用户的资源使用、待分配资源等抢占计算相关临时信息
 */
public class TempUserPerPartition extends AbstractPreemptionEntity {

  private final User user;
  private Resource userLimit;
  private boolean donePreemptionQuotaForULDelta = false;

  /**
   * 构造单个分区下用户的临时抢占计算数据
   */
  TempUserPerPartition(User user, String queueName, Resource usedPerPartition,
      Resource amUsedPerPartition, Resource reserved,
      Resource pendingPerPartition) {
    super(queueName, usedPerPartition, amUsedPerPartition, reserved,
        pendingPerPartition);
    this.user = user;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(" NAME: " + getUserName()).append(" CUR: ").append(getUsed())
        .append(" PEN: ").append(pending).append(" RESERVED: ").append(reserved)
        .append(" AM_USED: ").append(amUsed).append(" USER_LIMIT: ")
        .append(getUserLimit()).append(" IDEAL_ASSIGNED: ")
        .append(idealAssigned).append(" USED_WO_AMUSED: ")
        .append(getUsedDeductAM()).append(" IDEAL_PREEMPT: ")
        .append(toBePreempted).append(" ACTUAL_PREEMPT: ")
        .append(getActuallyToBePreempted()).append("\n");

    return sb.toString();
  }

  /**
   * 获取用户名
   * @return 用户名
   */
  public String getUserName() {
    return user.getUserName();
  }

  /**
   * 获取该用户在当前分区的资源限额
   * @return 用户资源限额
   */
  public Resource getUserLimit() {
    return userLimit;
  }

  /**
   * 设置该用户在当前分区的资源限额
   * @param userLimitResource 资源限额
   */
  public void setUserLimit(Resource userLimitResource) {
    this.userLimit = userLimitResource;
  }

  /**
   * 检查当前用户已用资源（扣除AM资源）是否超过用户限额
   * @param rc 资源计算器
   * @param clusterResource 集群总资源
   * @return 是否超过限额
   */
  public boolean isUserLimitReached(ResourceCalculator rc,
      Resource clusterResource) {
    if (Resources.greaterThan(rc, clusterResource, getUsedDeductAM(),
        userLimit)) {
      return true;
    }
    return false;
  }

  /**
   * 检查是否已完成针对用户限额差额的抢占配额计算
   * @return 是否已完成
   */
  public boolean isPreemptionQuotaForULDeltaDone() {
    return this.donePreemptionQuotaForULDelta;
  }

  /**
   * 更新针对用户限额差额的抢占配额计算完成状态
   * @param done 完成状态
   */
  public void updatePreemptionQuotaForULDeltaAsDone(boolean done) {
    this.donePreemptionQuotaForULDelta = done;
  }
}