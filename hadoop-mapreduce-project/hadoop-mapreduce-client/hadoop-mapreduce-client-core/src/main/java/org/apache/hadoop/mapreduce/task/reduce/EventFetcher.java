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
package org.apache.hadoop.mapreduce.task.reduce;

import java.io.IOException;

import org.apache.hadoop.mapred.MapTaskCompletionEventsUpdate;
import org.apache.hadoop.mapred.TaskCompletionEvent;
import org.apache.hadoop.mapred.TaskUmbilicalProtocol;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.util.concurrent.SubjectInheritingThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reduce阶段Map完成事件拉取线程，负责持续从ApplicationMaster拉取Map任务完成事件，
 * 通知Shuffle调度器处理已完成的Map输出，为Shuffle阶段拉取Map输出做准备。
 */
class EventFetcher<K,V> extends SubjectInheritingThread {
  // 正常拉取间隔睡眠时间，单位毫秒
  private static final long SLEEP_TIME = 1000;
  // 拉取失败最大重试次数
  private static final int MAX_RETRIES = 10;
  // 拉取失败重试间隔，单位毫秒
  private static final int RETRY_PERIOD = 5000;
  private static final Logger LOG = LoggerFactory.getLogger(EventFetcher.class);

  // 当前Reduce任务尝试ID
  private final TaskAttemptID reduce;
  // 与ApplicationMaster通信的RPC协议
  private final TaskUmbilicalProtocol umbilical;
  // Shuffle阶段调度器，用于处理Map完成事件
  private final ShuffleScheduler<K,V> scheduler;
  // 下一次拉取的起始事件索引
  private int fromEventIdx = 0;
  // 单次拉取最大事件数量
  private final int maxEventsToFetch;
  // 异常报告器，用于上报线程未捕获异常
  private final ExceptionReporter exceptionReporter;
  
  // 线程停止标志，volatile保证多线程可见性
  private volatile boolean stopped = false;
  
  /**
   * 构造EventFetcher线程实例
   * @param reduce 当前Reduce任务尝试ID
   * @param umbilical 与ApplicationMaster通信的RPC协议
   * @param scheduler Shuffle调度器，用于处理Map完成事件
   * @param reporter 异常报告器
   * @param maxEventsToFetch 单次拉取最大事件数量
   */
  public EventFetcher(TaskAttemptID reduce,
                      TaskUmbilicalProtocol umbilical,
                      ShuffleScheduler<K,V> scheduler,
                      ExceptionReporter reporter,
                      int maxEventsToFetch) {
    setName("EventFetcher for fetching Map Completion Events");
    setDaemon(true);    
    this.reduce = reduce;
    this.umbilical = umbilical;
    this.scheduler = scheduler;
    exceptionReporter = reporter;
    this.maxEventsToFetch = maxEventsToFetch;
  }

  @Override
  /**
   * EventFetcher线程主工作方法，持续拉取Map完成事件
   */
  public void work() {
    // 当前连续失败次数
    int failures = 0;
    LOG.info(reduce + " Thread started: " + getName());
    
    try {
      // 循环拉取直到线程停止或被中断
      while (!stopped && !Thread.currentThread().isInterrupted()) {
        try {
          // 拉取并处理一批Map完成事件
          int numNewMaps = getMapCompletionEvents();
          // 拉取成功，重置失败计数
          failures = 0;
          if (numNewMaps > 0) {
            LOG.info(reduce + ": " + "Got " + numNewMaps + " new map-outputs");
          }
          LOG.debug("GetMapEventsThread about to sleep for " + SLEEP_TIME);
          // 未被中断则休眠，等待下一次拉取
          if (!Thread.currentThread().isInterrupted()) {
            Thread.sleep(SLEEP_TIME);
          }
        } catch (InterruptedException e) {
          LOG.info("EventFetcher is interrupted.. Returning");
          return;
        } catch (IOException ie) {
          LOG.info("Exception in getting events", ie);
          // 超过最大重试次数，抛出异常终止线程
          if (++failures >= MAX_RETRIES) {
            throw new IOException("too many failures downloading events", ie);
          }
          // 拉取失败后休眠重试
          if (!Thread.currentThread().isInterrupted()) {
            Thread.sleep(RETRY_PERIOD);
          }
        }
      }
    } catch (InterruptedException e) {
      return;
    } catch (Throwable t) {
      // 上报未捕获异常
      exceptionReporter.reportException(t);
      return;
    }
  }

  /**
   * 关闭EventFetcher线程，停止拉取事件
   */
  public void shutDown() {
    this.stopped = true;
    interrupt();
    try {
      // 等待线程退出，最多等待5秒
      join(5000);
    } catch(InterruptedException ie) {
      LOG.warn("Got interrupted while joining " + getName(), ie);
    }
  }
  
  /** 
   * 从ApplicationMaster拉取从指定索引开始的Map完成事件，并处理这些事件
   * @return 新增成功完成的Map任务数量
   * @throws IOException 拉取事件IO异常
   * @throws InterruptedException 线程中断异常
   */  
  protected int getMapCompletionEvents()
      throws IOException, InterruptedException {
    
    // 新增成功完成的Map任务计数
    int numNewMaps = 0;
    TaskCompletionEvent events[] = null;

    // 循环拉取直到拉取到的事件数少于单次最大限制，保证拉取完所有可用事件
    do {
      // RPC调用拉取Map完成事件
      MapTaskCompletionEventsUpdate update =
          umbilical.getMapCompletionEvents(
              (org.apache.hadoop.mapred.JobID)reduce.getJobID(),
              fromEventIdx,
              maxEventsToFetch,
              (org.apache.hadoop.mapred.TaskAttemptID)reduce);
      events = update.getMapTaskCompletionEvents();
      LOG.debug("Got " + events.length + " map completion events from " +
               fromEventIdx);

      assert !update.shouldReset() : "Unexpected legacy state";

      // 更新下一次拉取的起始索引
      fromEventIdx += events.length;

      // 处理每个Map完成事件，更新调度器状态
      // 处理逻辑：
      // 1. 成功完成的Map添加到已知输出列表，准备拉取数据
      // 2. 失败/被杀死/过期的Map标记为废弃，停止拉取
      // 3. TIP失败的Map直接移除，不需要其输出
      for (TaskCompletionEvent event : events) {
        scheduler.resolve(event);
        if (TaskCompletionEvent.Status.SUCCEEDED == event.getTaskStatus()) {
          ++numNewMaps;
        }
      }
    } while (events.length == maxEventsToFetch);

    return numNewMaps;
  }

}