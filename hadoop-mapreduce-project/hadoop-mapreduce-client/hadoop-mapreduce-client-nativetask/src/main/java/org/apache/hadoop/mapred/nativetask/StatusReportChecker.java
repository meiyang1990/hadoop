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
package org.apache.hadoop.mapred.nativetask;

import java.io.IOException;

import org.apache.hadoop.mapred.Task.TaskReporter;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormatCounter;
import org.apache.hadoop.util.concurrent.SubjectInheritingThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 状态报告检查器，负责定期从原生任务获取运行状态，并上报给MapReduce框架
 * 为原生任务提供状态和指标的周期性同步能力，保证MapReduce框架能实时获取原生任务的运行进度
 */
class StatusReportChecker implements Runnable {

  private static final Logger LOG =
      LoggerFactory.getLogger(StatusReportChecker.class);
  public static final int INTERVAL = 1000; // milliseconds

  private Thread checker;
  private final TaskReporter reporter;
  private final long interval;

  /**
   * 使用默认检查间隔构造状态报告检查器
   * @param reporter MapReduce任务状态上报器
   */
  public StatusReportChecker(TaskReporter reporter) {
    this(reporter, INTERVAL);
  }

  /**
   * 使用自定义检查间隔构造状态报告检查器
   * @param reporter MapReduce任务状态上报器
   * @param interval 检查间隔，单位毫秒
   */
  public StatusReportChecker(TaskReporter reporter, long interval) {
    this.reporter = reporter;
    this.interval = interval;
  }

  @Override
  public void run() {
    while (true) {
      try {
        // 等待指定间隔后进行下一次检查
        Thread.sleep(interval);
      } catch (final InterruptedException e) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("StatusUpdater thread exiting " + "since it got interrupted");
        }
        // 线程被中断，退出循环
        break;
      }
      try {
        // 从原生运行时获取状态并上报给MapReduce框架
        NativeRuntime.reportStatus(reporter);
      } catch (final IOException e) {
        LOG.warn("Update native status got exception", e);
        // 将异常信息设置为任务状态，方便排查问题
        reporter.setStatus(e.toString());
        // 状态上报出错，退出循环
        break;
      }
    }
  }

  /**
   * 预初始化原生任务会使用的所有计数器
   * 提前注册计数器保证计数器能在UI中正确显示名称，避免原生任务动态创建导致显示异常
   */
  protected void initUsedCounters() {
    reporter.getCounter(TaskCounter.MAP_INPUT_RECORDS);
    reporter.getCounter(TaskCounter.MAP_OUTPUT_RECORDS);
    reporter.getCounter(FileInputFormatCounter.BYTES_READ);
    reporter.getCounter(TaskCounter.MAP_OUTPUT_BYTES);
    reporter.getCounter(TaskCounter.MAP_OUTPUT_MATERIALIZED_BYTES);
    reporter.getCounter(TaskCounter.COMBINE_INPUT_RECORDS);
    reporter.getCounter(TaskCounter.COMBINE_OUTPUT_RECORDS);
    reporter.getCounter(TaskCounter.SPILLED_RECORDS);
  }

  /**
   * 启动后台状态检查线程
   */
  public synchronized void start() {
    if (checker == null) {
      // 预初始化原生任务使用的计数器，保证UI正确显示名称
      initUsedCounters();
      // 创建继承访问主体的后台线程，保证安全上下文正确传递
      checker = new SubjectInheritingThread(this);
      // 设置为守护线程，任务结束后自动退出
      checker.setDaemon(true);
      // 启动线程开始周期性检查
      checker.start();
    }
  }

  /**
   * 停止后台状态检查线程
   * @throws InterruptedException 线程等待中断异常
   */
  public synchronized void stop() throws InterruptedException {
    if (checker != null) {
      // 中断后台线程
      checker.interrupt();
      // 等待线程完全退出
      checker.join();
    }
  }
}