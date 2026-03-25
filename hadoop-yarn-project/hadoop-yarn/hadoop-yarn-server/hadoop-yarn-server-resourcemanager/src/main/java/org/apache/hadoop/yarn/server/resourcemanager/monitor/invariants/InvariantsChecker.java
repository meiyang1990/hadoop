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
package org.apache.hadoop.yarn.server.resourcemanager.monitor.invariants;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.monitor.SchedulingEditPolicy;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 抽象不变量检查器基类，为所有具体不变量检查器提供公共上下文和基础能力，
 * 用于在YARN调度过程中验证调度系统状态是否满足预期不变量条件。
 */
public abstract class InvariantsChecker implements SchedulingEditPolicy {

  private static final Logger LOG =
      LoggerFactory.getLogger(InvariantsChecker.class);
  // 配置项：是否在违反不变量时抛出异常
  public static final String THROW_ON_VIOLATION =
      "yarn.resourcemanager.invariant-checker.throw-on-violation";
  // 配置项：不变量检查的监控间隔（毫秒）
  public static final String INVARIANT_MONITOR_INTERVAL =
      "yarn.resourcemanager.invariant-checker.monitor-interval";

  private Configuration conf;
  private RMContext context;
  private ResourceScheduler scheduler;
  private boolean throwOnInvariantViolation;
  private long monitoringInterval;

  @Override
  public void init(Configuration config, RMContext rmContext,
      ResourceScheduler scheduler) {
    this.conf = config;
    this.context = rmContext;
    this.scheduler = scheduler;
    // 从配置加载是否抛出异常的设置，默认不抛出仅日志警告
    this.throwOnInvariantViolation =
        conf.getBoolean(InvariantsChecker.THROW_ON_VIOLATION, false);
    // 从配置加载监控间隔，默认1秒检查一次
    this.monitoringInterval =
        conf.getLong(InvariantsChecker.INVARIANT_MONITOR_INTERVAL, 1000L);

    LOG.info("Invariant checker " + this.getPolicyName()
        + " enabled. Monitoring every " + monitoringInterval
        + "ms, throwOnViolation=" + throwOnInvariantViolation);
  }

  @Override
  public long getMonitoringInterval() {
    return monitoringInterval;
  }

  @Override
  public String getPolicyName() {
    return this.getClass().getSimpleName();
  }

  /**
   * 根据配置决定是抛出异常还是仅记录警告日志，处理不变量违反事件。
   * @param message 违反不变量的描述信息
   * @throws InvariantViolationException 如果配置开启了抛出异常则抛出该异常
   */
  public void logOrThrow(String message) throws InvariantViolationException {
    if (getThrowOnInvariantViolation()) {
      throw new InvariantViolationException(message);
    } else {
      LOG.warn(message);
    }
  }

  public boolean getThrowOnInvariantViolation() {
    return throwOnInvariantViolation;
  }

  public Configuration getConf() {
    return conf;
  }

  public RMContext getContext() {
    return context;
  }

  public ResourceScheduler getScheduler() {
    return scheduler;
  }

}