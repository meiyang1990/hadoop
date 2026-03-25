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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 文件说明：容量调度器专属预约系统实现，基于CapacityScheduler实现ReservationSystem接口，
 * 为YARN容量调度器提供资源预约功能，支持按时段预留集群资源给特定计划/队列使用。
 */
@LimitedPrivate("yarn")
@Unstable
public class CapacityReservationSystem extends AbstractReservationSystem {

  // 日志记录器
  private static final Logger LOG = LoggerFactory
      .getLogger(CapacityReservationSystem.class);

  // 关联的CapacityScheduler容量调度器实例
  private CapacityScheduler capScheduler;

  /**
   * 构造函数，初始化容量预约系统
   */
  public CapacityReservationSystem() {
    super(CapacityReservationSystem.class.getName());
  }

  /**
   * 重新初始化容量预约系统，验证并关联容量调度器
   * @param conf 配置对象
   * @param rmContext RM上下文对象
   * @throws YarnException 初始化异常
   */
  @Override
  public void reinitialize(Configuration conf, RMContext rmContext)
      throws YarnException {
    // 从RM上下文获取调度器实例
    ResourceScheduler scheduler = rmContext.getScheduler();
    // 验证调度器类型必须是CapacityScheduler
    if (!(scheduler instanceof CapacityScheduler)) {
      throw new YarnRuntimeException("Class "
          + scheduler.getClass().getCanonicalName() + " not instance of "
          + CapacityScheduler.class.getCanonicalName());
    }
    // 类型转换并保存容量调度器引用
    capScheduler = (CapacityScheduler) scheduler;
    this.conf = conf;
    // 调用父类完成初始化
    super.reinitialize(conf, rmContext);
  }

  @Override
  protected Resource getMinAllocation() {
    // 从容量调度器获取容器最小分配资源
    return capScheduler.getMinimumResourceCapability();
  }

  @Override
  protected Resource getMaxAllocation() {
    // 从容量调度器获取容器最大分配资源
    return capScheduler.getMaximumResourceCapability();
  }

  @Override
  protected ResourceCalculator getResourceCalculator() {
    // 从容量调度器获取资源计算器
    return capScheduler.getResourceCalculator();
  }

  @Override
  protected QueueMetrics getRootQueueMetrics() {
    // 从容量调度器获取根队列指标
    return capScheduler.getRootQueueMetrics();
  }

  @Override
  protected String getPlanQueuePath(String planQueueName) {
    // 获取预约计划对应队列的完整路径
    return capScheduler.getQueue(planQueueName).getQueuePath();
  }

  @Override
  protected Resource getPlanQueueCapacity(String planQueueName) {
    // 获取最小分配单位
    Resource minAllocation = getMinAllocation();
    // 获取资源计算器
    ResourceCalculator rescCalc = getResourceCalculator();
    // 根据队列名获取预约计划对应的队列
    CSQueue planQueue = capScheduler.getQueue(planQueueName);
    // 根据队列绝对容量计算出该队列可用于预约的总资源
    return rescCalc.multiplyAndNormalizeDown(capScheduler.getClusterResource(),
        planQueue.getAbsoluteCapacity(), minAllocation);
  }

  @Override
  public Plan getPlan(String planName) {
    // 对计划名做标准化处理后调用父类获取对应计划
    return super.getPlan(capScheduler.normalizeQueueName(planName));
  }

  @Override
  protected ReservationSchedulerConfiguration
      getReservationSchedulerConfiguration() {
    // 从容量调度器获取预约调度配置
    return capScheduler.getConfiguration();
  }

}