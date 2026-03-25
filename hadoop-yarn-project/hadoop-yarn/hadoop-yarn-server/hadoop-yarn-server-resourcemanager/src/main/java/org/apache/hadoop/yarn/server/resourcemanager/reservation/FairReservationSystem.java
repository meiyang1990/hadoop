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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;

/**
 * 适配FairScheduler调度器的预约系统实现，将预约能力与公平调度器集成
 */
public class FairReservationSystem extends AbstractReservationSystem {

  // 关联的公平调度器实例
  private FairScheduler fairScheduler;

  public FairReservationSystem() {
    super(FairReservationSystem.class.getName());
  }

  @Override
  public void reinitialize(Configuration conf, RMContext rmContext)
      throws YarnException {
    // 从RM上下文获取当前调度器实例
    ResourceScheduler scheduler = rmContext.getScheduler();
    // 验证当前调度器确实是FairScheduler，否则抛出异常
    if (!(scheduler instanceof FairScheduler)) {
      throw new YarnRuntimeException("Class "
          + scheduler.getClass().getCanonicalName() + " not instance of "
          + FairScheduler.class.getCanonicalName());
    }
    // 类型转换后保存FairScheduler引用
    fairScheduler = (FairScheduler) scheduler;
    // 保存配置
    this.conf = conf;
    // 调用父类完成初始化
    super.reinitialize(conf, rmContext);
  }

  @Override
  protected ReservationSchedulerConfiguration
      getReservationSchedulerConfiguration() {
    // 从FairScheduler获取调度配置
    return fairScheduler.getAllocationConfiguration();
  }

  @Override
  protected ResourceCalculator getResourceCalculator() {
    // 从FairScheduler获取资源计算器
    return fairScheduler.getResourceCalculator();
  }

  @Override
  protected QueueMetrics getRootQueueMetrics() {
    // 从FairScheduler获取根队列指标
    return fairScheduler.getRootQueueMetrics();
  }

  @Override
  protected Resource getMinAllocation() {
    // 从FairScheduler获取最小资源分配量
    return fairScheduler.getMinimumResourceCapability();
  }

  @Override
  protected Resource getMaxAllocation() {
    // 从FairScheduler获取最大资源分配量
    return fairScheduler.getMaximumResourceCapability();
  }

  @Override
  protected String getPlanQueuePath(String planQueueName) {
    // 公平调度器中计划名称就是完整队列路径，直接返回
    return planQueueName; }

  @Override
  protected Resource getPlanQueueCapacity(String planQueueName) {
    // 获取计划队列父队列的稳定公平份额作为计划容量
    return fairScheduler.getQueueManager().getParentQueue(planQueueName, false)
        .getSteadyFairShare();
  }

  @Override
  public Plan getPlan(String planName) {
    // make sure plan name is a full queue name in fair scheduler. For example,
    // "root.default" is the full queue name for "default".
    // 从公平调度器队列管理器获取对应队列
    FSQueue queue = fairScheduler.getQueueManager().getQueue(planName);

    if (queue != null) {
      // 队列存在，调用父类获取对应预约计划
      return super.getPlan(queue.getQueueName());
    } else {
      // 队列不存在，返回null
      return null;
    }
  }
}