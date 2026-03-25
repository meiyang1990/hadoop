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

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Queue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerDynamicEditException;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.PlanQueue;

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity
    .ReservationQueue;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 容量调度器专用计划同步器，实现了{@link PlanFollower}接口。
 * 该组件会被定时器定期触发，负责将预约计划(Plan)的状态同步到底层CapacityScheduler，
 * 通过动态增删改叶子队列，让调度器行为和预约计划保持一致，同时更新计划中集群可用资源视图。
 * 
 * 该实现相对无状态，能够处理任意计划变更（通过对比现有队列找出差异），
 * 对同步频率不敏感，支持RM重启后直接工作（无需追赶历史状态）。
 */
public class CapacitySchedulerPlanFollower extends AbstractSchedulerPlanFollower {

  private static final Logger LOG = LoggerFactory
      .getLogger(CapacitySchedulerPlanFollower.class);

  private CapacityScheduler cs;

  @Override
  public void init(Clock clock, ResourceScheduler sched, Collection<Plan> plans) {
    super.init(clock, sched, plans);
    LOG.info("Initializing Plan Follower Policy:"
        + this.getClass().getCanonicalName());
    // 校验当前调度器必须是CapacityScheduler
    if (!(sched instanceof CapacityScheduler)) {
      throw new YarnRuntimeException(
          "CapacitySchedulerPlanFollower can only work with CapacityScheduler");
    }
    this.cs = (CapacityScheduler) sched;
  }

  @Override
  protected Queue getPlanQueue(String planQueueName) {
    CSQueue queue = cs.getQueue(planQueueName);
    if (!(queue instanceof PlanQueue)) {
      LOG.error("The Plan is not an PlanQueue!");
      return null;
    }
    return queue;
  }

  @Override
  protected List<? extends Queue> getChildReservationQueues(Queue queue) {
    PlanQueue planQueue = (PlanQueue)queue;
    List<CSQueue> childQueues = planQueue.getChildQueues();
    return childQueues;
  }

  @Override
  protected void addReservationQueue(
      String planQueueName, Queue queue, String currResId) {
    PlanQueue planQueue = (PlanQueue)queue;
    try {
      // 创建新的预约队列，挂靠到对应计划队列下
      ReservationQueue resQueue =
          new ReservationQueue(cs.getQueueContext(), currResId, planQueue);
      // 将新队列添加到容量调度器
      cs.addQueue(resQueue);
    } catch (SchedulerDynamicEditException e) {
      LOG.warn(
          "Exception while trying to activate reservation: {} for plan: {}",
          currResId, planQueueName, e);
    } catch (IOException e) {
      LOG.warn(
          "Exception while trying to activate reservation: {} for plan: {}",
          currResId, planQueueName, e);
    }
  }

  @Override
  protected void createDefaultReservationQueue(
      String planQueueName, Queue queue, String defReservationId) {
    PlanQueue planQueue = (PlanQueue)queue;
    // 默认预约队列不存在才创建
    if (cs.getQueue(defReservationId) == null) {
      try {
        // 创建默认预约队列，用于放置未匹配到预约的作业
        ReservationQueue defQueue =
            new ReservationQueue(cs.getQueueContext(), defReservationId, planQueue);
        cs.addQueue(defQueue);
      } catch (SchedulerDynamicEditException e) {
        LOG.warn(
            "Exception while trying to create default reservation queue for plan: {}",
            planQueueName, e);
      } catch (IOException e) {
        LOG.warn(
            "Exception while trying to create default reservation queue for " +
                "plan: {}",
            planQueueName, e);
      }
    }
  }

  @Override
  protected Resource getPlanResources(
      Plan plan, Queue queue, Resource clusterResources) {
    PlanQueue planQueue = (PlanQueue)queue;
    // 获取计划队列占集群总资源的绝对容量比例
    float planAbsCap = planQueue.getAbsoluteCapacity();
    // 根据比例计算计划可用总资源
    Resource planResources = Resources.multiply(clusterResources, planAbsCap);
    // 更新计划的总容量信息
    plan.setTotalCapacity(planResources);
    return planResources;
  }

  @Override
  protected Resource getReservationQueueResourceIfExists(Plan plan,
      ReservationId reservationId) {
    // 根据预约ID从调度器获取对应预约队列
    CSQueue resQueue = cs.getQueue(reservationId.toString());
    Resource reservationResource = null;
    if (resQueue != null) {
      // 根据队列绝对容量计算已分配资源量
      reservationResource = Resources.multiply(cs.getClusterResource(),
          resQueue.getAbsoluteCapacity());
    }
    return reservationResource;
  }

}