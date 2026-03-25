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
package org.apache.hadoop.yarn.server.resourcemanager.monitor;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.util.concurrent.SubjectInheritingThread;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;

import org.apache.hadoop.classification.VisibleForTesting;

/**
 * YARN ResourceManager 调度监控服务，定期执行调度编辑策略，实现抢占等动态调度功能
 */
public class SchedulingMonitor extends AbstractService {

  private final SchedulingEditPolicy scheduleEditPolicy;
  private static final Logger LOG =
      LoggerFactory.getLogger(SchedulingMonitor.class);

  // 定期执行调度检查的定时任务执行器
  private ScheduledExecutorService ses;
  // 定时任务句柄
  private ScheduledFuture<?> handler;
  // 服务停止标志
  private volatile boolean stopped;
  // 监控执行间隔（毫秒）
  private long monitorInterval;
  // RM上下文对象
  private RMContext rmContext;

  /**
   * 构造调度监控实例
   * @param rmContext ResourceManager上下文
   * @param scheduleEditPolicy 要执行的调度编辑策略
   */
  public SchedulingMonitor(RMContext rmContext,
      SchedulingEditPolicy scheduleEditPolicy) {
    super("SchedulingMonitor (" + scheduleEditPolicy.getPolicyName() + ")");
    this.scheduleEditPolicy = scheduleEditPolicy;
    this.rmContext = rmContext;
  }

  @VisibleForTesting
  public synchronized SchedulingEditPolicy getSchedulingEditPolicy() {
    return scheduleEditPolicy;
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    LOG.info("Initializing SchedulingMonitor=" + getName());
    // 初始化调度编辑策略
    scheduleEditPolicy.init(conf, rmContext, rmContext.getScheduler());
    // 获取配置的监控执行间隔
    this.monitorInterval = scheduleEditPolicy.getMonitoringInterval();
    super.serviceInit(conf);
  }

  @Override
  public void serviceStart() throws Exception {
    LOG.info("Starting SchedulingMonitor=" + getName());
    assert !stopped : "starting when already stopped";
    // 创建单线程定时执行器，使用自定义线程工厂创建可继承访问主体的线程
    ses = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
      public Thread newThread(Runnable r) {
        Thread t = new SubjectInheritingThread(r);
        t.setName(getName());
        return t;
      }
    });
    // 启动定时调度检查任务
    schedulePreemptionChecker();
    super.serviceStart();
  }

  /**
   * 提交固定周期的调度检查任务到定时执行器
   */
  private void schedulePreemptionChecker() {
    handler = ses.scheduleAtFixedRate(new PolicyInvoker(),
        0, monitorInterval, TimeUnit.MILLISECONDS);
  }

  @Override
  public void serviceStop() throws Exception {
    stopped = true;
    if (handler != null) {
      LOG.info("Stop " + getName());
      // 取消正在执行的任务
      handler.cancel(true);
      // 关闭定时执行器
      ses.shutdown();
    }
    super.serviceStop();
  }

  @VisibleForTesting
  public void invokePolicy(){
    scheduleEditPolicy.editSchedule();
  }

  /**
   * 定时执行调度策略的任务类
   */
  private class PolicyInvoker implements Runnable {
    @Override
    public void run() {
      try {
        // 检查间隔是否变更，如果变更则重新调度任务
        if (monitorInterval != scheduleEditPolicy.getMonitoringInterval()) {
          handler.cancel(true);
          monitorInterval = scheduleEditPolicy.getMonitoringInterval();
          schedulePreemptionChecker();
        } else {
          // 执行调度编辑策略
          invokePolicy();
        }
      } catch (Throwable t) {
        // 执行异常不终止服务，仅记录日志后跳过本次执行，下次继续尝试
        LOG.error("Exception raised while executing preemption"
            + " checker, skip this run..., exception=", t);
      }
    }
  }
}