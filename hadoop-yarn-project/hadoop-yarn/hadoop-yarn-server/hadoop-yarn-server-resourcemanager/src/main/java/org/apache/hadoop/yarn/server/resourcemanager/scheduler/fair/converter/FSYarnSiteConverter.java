// 这个文件已经全部加上中文注释
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.converter;

import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.PREFIX;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.monitor.capacity.ProportionalCapacityPreemptionPolicy;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.AutoCreatedQueueDeletionPolicy;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueConfigurationAutoRefreshPolicy;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairSchedulerConfiguration;
import org.apache.hadoop.yarn.util.resource.DominantResourceCalculator;

/**
 * 将公平调度器（Fair Scheduler）的Yarn站点配置转换为容量调度器（Capacity Scheduler）配置。
 * 用于配置迁移场景，将现有公平调度器配置自动转换为容量调度器可用格式。
 *
 */
public class FSYarnSiteConverter {
  private boolean preemptionEnabled;
  private boolean sizeBasedWeight;

  /**
   * 转换公平调度器站点配置到容量调度器站点配置。
   * @param conf 原公平调度器配置对象
   * @param yarnSiteConfig 目标Yarn站点配置对象，转换结果写入此对象
   * @param drfUsed 是否使用DRF（主导资源公平）资源计算策略
   * @param enableAsyncScheduler 是否启用异步调度
   * @param userPercentage 是否按用户百分比分配资源
   * @param preemptionMode 抢占策略模式
   */
  @SuppressWarnings({"deprecation", "checkstyle:linelength"})
  public void convertSiteProperties(Configuration conf,
      Configuration yarnSiteConfig, boolean drfUsed,
      boolean enableAsyncScheduler, boolean userPercentage,
      FSConfigToCSConfigConverterParams.PreemptionMode preemptionMode) {
    // 修改调度器实现类为容量调度器
    yarnSiteConfig.set(YarnConfiguration.RM_SCHEDULER,
        CapacityScheduler.class.getCanonicalName());

    // 如果源配置启用了持续调度，转换异步调度配置
    if (conf.getBoolean(
        FairSchedulerConfiguration.CONTINUOUS_SCHEDULING_ENABLED,
        FairSchedulerConfiguration.DEFAULT_CONTINUOUS_SCHEDULING_ENABLED)) {
      yarnSiteConfig.setBoolean(
          CapacitySchedulerConfiguration.SCHEDULE_ASYNCHRONOUSLY_ENABLE, enableAsyncScheduler);
      // 读取调度间隔配置
      int interval = conf.getInt(
          FairSchedulerConfiguration.CONTINUOUS_SCHEDULING_SLEEP_MS,
          FairSchedulerConfiguration.DEFAULT_CONTINUOUS_SCHEDULING_SLEEP_MS);
      // 写入容量调度器对应配置项
      yarnSiteConfig.setInt(PREFIX +
          "schedule-asynchronously.scheduling-interval-ms", interval);
    }

    // 必须开启RM调度器监视器，以支持容量调度器队列自动刷新
    yarnSiteConfig.setBoolean(
        YarnConfiguration.RM_SCHEDULER_ENABLE_MONITORS, true);

    // 如果源配置启用了抢占，转换抢占相关配置
    if (conf.getBoolean(FairSchedulerConfiguration.PREEMPTION,
        FairSchedulerConfiguration.DEFAULT_PREEMPTION)) {
      preemptionEnabled = true;

      // 添加比例容量抢占策略到监视器策略列表
      String policies = addMonitorPolicy(ProportionalCapacityPreemptionPolicy.
          class.getCanonicalName(), yarnSiteConfig);
      yarnSiteConfig.set(YarnConfiguration.RM_SCHEDULER_MONITOR_POLICIES,
          policies);

      // 转换抢占前等待时间配置
      int waitTimeBeforeKill = conf.getInt(
          FairSchedulerConfiguration.WAIT_TIME_BEFORE_KILL,
          FairSchedulerConfiguration.DEFAULT_WAIT_TIME_BEFORE_KILL);
      yarnSiteConfig.setInt(
          CapacitySchedulerConfiguration.PREEMPTION_WAIT_TIME_BEFORE_KILL,
          waitTimeBeforeKill);

      // 转换饥饿检查间隔配置
      long waitBeforeNextStarvationCheck = conf.getLong(
          FairSchedulerConfiguration.WAIT_TIME_BEFORE_NEXT_STARVATION_CHECK_MS,
          FairSchedulerConfiguration.DEFAULT_WAIT_TIME_BEFORE_NEXT_STARVATION_CHECK_MS);
      yarnSiteConfig.setLong(
          CapacitySchedulerConfiguration.PREEMPTION_MONITORING_INTERVAL,
          waitBeforeNextStarvationCheck);
    } else {
      // 源配置未开启抢占时，如果模式为NO_POLICY则清空抢占策略
      if (preemptionMode ==
          FSConfigToCSConfigConverterParams.PreemptionMode.NO_POLICY) {
        yarnSiteConfig.set(YarnConfiguration.RM_SCHEDULER_MONITOR_POLICIES, "");
      }
    }

    // 处理自动创建队列的自动删除策略
    if (!userPercentage) {
      // 添加自动队列删除策略到监视器策略列表
      String policies = addMonitorPolicy(AutoCreatedQueueDeletionPolicy.
          class.getCanonicalName(), yarnSiteConfig);
      yarnSiteConfig.set(YarnConfiguration.RM_SCHEDULER_MONITOR_POLICIES,
          policies);

      // 设置队列过期删除间隔为10秒，与公平调度器默认行为一致
      yarnSiteConfig.setInt(CapacitySchedulerConfiguration.
          AUTO_CREATE_CHILD_QUEUE_EXPIRED_TIME, 10);
    }

    // 转换多容器分配开关配置
    if (conf.getBoolean(FairSchedulerConfiguration.ASSIGN_MULTIPLE,
        FairSchedulerConfiguration.DEFAULT_ASSIGN_MULTIPLE)) {
      yarnSiteConfig.setBoolean(
          CapacitySchedulerConfiguration.ASSIGN_MULTIPLE_ENABLED, true);
    } else {
      yarnSiteConfig.setBoolean(
          CapacitySchedulerConfiguration.ASSIGN_MULTIPLE_ENABLED, false);
    }

    // 必须开启容量调度器配置自动刷新策略
    yarnSiteConfig.set(YarnConfiguration.RM_SCHEDULER_MONITOR_POLICIES,
        addMonitorPolicy(QueueConfigurationAutoRefreshPolicy
            .class.getCanonicalName(), yarnSiteConfig));

    // 转换每次心跳最大分配容器数配置，仅当非默认值时写入
    int maxAssign = conf.getInt(FairSchedulerConfiguration.MAX_ASSIGN,
        FairSchedulerConfiguration.DEFAULT_MAX_ASSIGN);
    if (maxAssign != FairSchedulerConfiguration.DEFAULT_MAX_ASSIGN) {
      yarnSiteConfig.setInt(
          CapacitySchedulerConfiguration.MAX_ASSIGN_PER_HEARTBEAT,
          maxAssign);
    }

    // 转换节点局部性延迟阈值，仅当非默认值时写入
    float localityThresholdNode = conf.getFloat(
        FairSchedulerConfiguration.LOCALITY_THRESHOLD_NODE,
        FairSchedulerConfiguration.DEFAULT_LOCALITY_THRESHOLD_NODE);
    if (localityThresholdNode !=
        FairSchedulerConfiguration.DEFAULT_LOCALITY_THRESHOLD_NODE) {
      yarnSiteConfig.setFloat(CapacitySchedulerConfiguration.NODE_LOCALITY_DELAY,
          localityThresholdNode);
    }

    // 转换机架局部性附加延迟阈值，仅当非默认值时写入
    float localityThresholdRack = conf.getFloat(
        FairSchedulerConfiguration.LOCALITY_THRESHOLD_RACK,
        FairSchedulerConfiguration.DEFAULT_LOCALITY_THRESHOLD_RACK);
    if (localityThresholdRack !=
        FairSchedulerConfiguration.DEFAULT_LOCALITY_THRESHOLD_RACK) {
      yarnSiteConfig.setFloat(
          CapacitySchedulerConfiguration.RACK_LOCALITY_ADDITIONAL_DELAY,
          localityThresholdRack);
    }

    // 记录是否开启基于任务大小的权重调整
    if (conf.getBoolean(FairSchedulerConfiguration.SIZE_BASED_WEIGHT,
        FairSchedulerConfiguration.DEFAULT_SIZE_BASED_WEIGHT)) {
      sizeBasedWeight = true;
    }

    // 如果启用DRF策略，设置容量调度器资源计算器为主导资源计算器
    if (drfUsed) {
      yarnSiteConfig.set(
          CapacitySchedulerConfiguration.RESOURCE_CALCULATOR_CLASS,
          DominantResourceCalculator.class.getCanonicalName());
    }

    // 如果要求启用异步调度，设置对应配置项
    if (enableAsyncScheduler) {
      yarnSiteConfig.setBoolean(CapacitySchedulerConfiguration.SCHEDULE_ASYNCHRONOUSLY_ENABLE, true);
    }
  }

  /**
   * 获取转换后是否启用抢占。
   * @return 抢占是否启用
   */
  public boolean isPreemptionEnabled() {
    return preemptionEnabled;
  }

  /**
   * 获取转换后是否启用基于大小的权重。
   * @return 是否启用基于大小的权重
   */
  public boolean isSizeBasedWeight() {
    return sizeBasedWeight;
  }

  /**
   * 向现有监视器策略列表添加新的策略类名。
   * @param policyName 要添加的策略全类名
   * @param yarnSiteConfig Yarn站点配置对象
   * @return 更新后的策略列表字符串
   */
  private String addMonitorPolicy(String policyName,
      Configuration yarnSiteConfig) {
    String policies =
        yarnSiteConfig.get(YarnConfiguration.RM_SCHEDULER_MONITOR_POLICIES);
    if (policies == null || policies.isEmpty()) {
      policies = policyName;
    } else {
      policies += "," + policyName;
    }
    return policies;
  }

}