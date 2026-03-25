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

package org.apache.hadoop.yarn.server.nodemanager.health;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.service.AbstractService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Timer;
import java.util.TimerTask;

/**
 * 文件说明：YARN NodeManager定时健康检查服务抽象基类，基于Timer实现周期性健康检查任务调度
 * 
 * 定时健康检查服务骨架，用于定期执行指定健康检查任务，并维护节点健康状态信息。
 * 子类只需要实现具体健康检查TimerTask即可复用定时调度、状态管理能力。
 *
 * @see NodeHealthScriptRunner
 */
public abstract class TimedHealthReporterService extends AbstractService
    implements HealthReporter {

  private static final Logger LOG =
      LoggerFactory.getLogger(TimedHealthReporterService.class);

  // 节点当前健康状态标识
  private boolean isHealthy;
  // 节点健康报告文本
  private String healthReport;
  // 上次健康检查报告时间戳
  private long lastReportedTime;

  // 定时任务调度器
  private Timer timer;
  // 具体健康检查任务
  private TimerTask task;
  // 健康检查间隔（毫秒）
  private long intervalMs;
  // 是否在服务启动前立即执行一次健康检查
  private boolean runBeforeStartup;

  /**
   * 构造定时健康检查服务，默认不提前执行检查
   * @param name 服务名称
   * @param intervalMs 健康检查间隔（毫秒）
   */
  TimedHealthReporterService(String name, long intervalMs) {
    super(name);
    this.isHealthy = true;
    this.healthReport = "";
    this.lastReportedTime = System.currentTimeMillis();
    this.intervalMs = intervalMs;
    this.runBeforeStartup = false;
  }

  /**
   * 构造定时健康检查服务，可配置是否提前执行检查
   * @param name 服务名称
   * @param intervalMs 健康检查间隔（毫秒）
   * @param runBeforeStartup 是否启动服务前立即执行一次检查
   */
  TimedHealthReporterService(String name, long intervalMs,
      boolean runBeforeStartup) {
    super(name);
    this.isHealthy = true;
    this.healthReport = "";
    this.lastReportedTime = System.currentTimeMillis();
    this.intervalMs = intervalMs;
    this.runBeforeStartup = runBeforeStartup;
  }

  @VisibleForTesting
  void setTimerTask(TimerTask timerTask) {
    task = timerTask;
  }

  @VisibleForTesting
  TimerTask getTimerTask() {
    return task;
  }

  /**
   * 启动健康监控服务，初始化并启动定时任务调度
   */
  @Override
  public void serviceStart() throws Exception {
    // 检查任务是否已设置，未设置则抛出异常
    if (task == null) {
      throw new Exception("Health reporting task hasn't been set!");
    }
    // 创建后台定时线程
    timer = new Timer("HealthReporterService-Timer", true);
    // 初始延迟时间默认0
    long delay = 0;
    // 如果配置启动前执行，设置初始延迟为间隔，立即执行一次任务
    if (runBeforeStartup) {
      delay = intervalMs;
      task.run();
    }

    // 按固定速率调度健康检查任务
    timer.scheduleAtFixedRate(task, delay, intervalMs);
    super.serviceStart();
  }

  /**
   * 停止健康监控服务，关闭定时器释放资源
   */
  @Override
  protected void serviceStop() throws Exception {
    if (timer != null) {
      timer.cancel();
    }
    super.serviceStop();
  }

  @Override
  public boolean isHealthy() {
    return isHealthy;
  }

  /**
   * 设置节点当前健康状态
   *
   * @param healthy 节点是否健康
   */
  protected synchronized void setHealthy(boolean healthy) {
    this.isHealthy = healthy;
  }

  @Override
  public String getHealthReport() {
    return healthReport;
  }

  /**
   * 设置节点健康检查报告文本
   *
   * @param report 健康报告文本
   */
  private synchronized void setHealthReport(String report) {
    this.healthReport = report;
  }

  @Override
  public long getLastHealthReportTime() {
    return lastReportedTime;
  }

  /**
   * 设置上次健康检查报告时间
   *
   * @param lastReportedTime 上次报告时间戳
   */
  private synchronized void setLastReportedTime(long lastReportedTime) {
    this.lastReportedTime = lastReportedTime;
  }

  /**
   * 更新节点状态为健康，清空报告并更新检查时间
   */
  synchronized void setHealthyWithoutReport() {
    this.setHealthy(true);
    this.setHealthReport("");
    this.setLastReportedTime(System.currentTimeMillis());
  }

  /**
   * 更新节点状态为不健康，设置报告并更新检查时间
   * @param output 不健康原因报告文本
   */
  synchronized void setUnhealthyWithReport(String output) {
    LOG.info("Health status being set as: \"" + output + "\".");
    this.setHealthy(false);
    this.setHealthReport(output);
    this.setLastReportedTime(System.currentTimeMillis());
  }
}