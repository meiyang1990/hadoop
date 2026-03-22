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

package org.apache.hadoop.mapreduce.v2.app;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.util.MRJobConfUtil;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptDiagnosticsUpdateEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptEventType;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.util.concurrent.SubjectInheritingThread;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.util.Clock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * 文件说明: Task尝试心跳监控服务，负责跟踪所有已启动的任务尝试，定期检测任务是否存活，对长时间未发送心跳的任务标记为超时死亡。
 * 是MapReduce ApplicationMaster中保障任务健壮性的核心组件，及时发现挂掉卡住的任务并触发重试。
 */
@SuppressWarnings({"unchecked", "rawtypes"})
public class TaskHeartbeatHandler extends AbstractService {

  /**
   * 记录任务尝试最后一次心跳时间，以及是否已经上报过进度的状态。
   */
  static class ReportTime {
    private long lastProgress;
    private final AtomicBoolean reported;

    public ReportTime(long time) {
      setLastProgress(time);
      reported = new AtomicBoolean(false);
    }
    
    public synchronized void setLastProgress(long time) {
      lastProgress = time;
    }

    public synchronized long getLastProgress() {
      return lastProgress;
    }

    public boolean isReported(){
      return reported.get();
    }
  }
  
  private static final Logger LOG =
      LoggerFactory.getLogger(TaskHeartbeatHandler.class);
  
  // 定期检测丢失任务的后台线程
  private Thread lostTaskCheckerThread;
  // 服务停止标志
  private volatile boolean stopped;
  // 任务心跳超时时间，超时判定任务丢失
  private long taskTimeOut;
  // 已注销任务保留超时时间，超过后清理记录
  private long unregisterTimeOut;
  // 任务卡住超时时间，启动后长时间未上报进度判定为卡住
  private long taskStuckTimeOut;
  // 超时检查间隔时间，默认30秒
  private int taskTimeOutCheckInterval = 30 * 1000; // 30 seconds.

  // 事件处理器，用于发送任务超时事件到AppMaster事件系统
  private final EventHandler eventHandler;
  // 时钟工具，用于获取当前时间，方便测试模拟
  private final Clock clock;
  
  // 当前正在运行的任务尝试与其最后心跳时间的映射表
  private ConcurrentMap<TaskAttemptId, ReportTime> runningAttempts;
  // 最近已注销的任务尝试与其注销时间的映射表，用于延迟清理
  private ConcurrentMap<TaskAttemptId, ReportTime> recentlyUnregisteredAttempts;

  /**
   * 构造任务心跳处理器实例，初始化运行任务和已注销任务存储容器。
   * @param eventHandler 事件处理器，用于发送任务事件
   * @param clock 时钟工具，用于获取当前时间
   * @param numThreads 预估并发线程数，用于初始化并发容器容量
   */
  public TaskHeartbeatHandler(EventHandler eventHandler, Clock clock,
      int numThreads) {
    super("TaskHeartbeatHandler");
    this.eventHandler = eventHandler;
    this.clock = clock;
    runningAttempts =
      new ConcurrentHashMap<TaskAttemptId, ReportTime>(16, 0.75f, numThreads);
    recentlyUnregisteredAttempts =
        new ConcurrentHashMap<TaskAttemptId, ReportTime>(16, 0.75f, numThreads);
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    super.serviceInit(conf);
    // 从配置加载任务超时时间，使用默认值兜底
    taskTimeOut = conf.getLong(
        MRJobConfig.TASK_TIMEOUT, MRJobConfig.DEFAULT_TASK_TIMEOUT_MILLIS);
    // 从配置加载已注销任务清理超时时间，使用默认值兜底
    unregisterTimeOut = conf.getLong(MRJobConfig.TASK_EXIT_TIMEOUT,
        MRJobConfig.TASK_EXIT_TIMEOUT_DEFAULT);
    // 从配置加载任务卡住超时时间，使用默认值兜底
    taskStuckTimeOut = conf.getLong(MRJobConfig.TASK_STUCK_TIMEOUT_MS,
        MRJobConfig.DEFAULT_TASK_STUCK_TIMEOUT_MS);

    // 强制保证任务超时至少是任务进度上报间隔的两倍，避免误判
    long taskProgressReportIntervalMillis = MRJobConfUtil.
        getTaskProgressReportInterval(conf);
    long minimumTaskTimeoutAllowed = taskProgressReportIntervalMillis * 2;
    if(taskTimeOut < minimumTaskTimeoutAllowed) {
      taskTimeOut = minimumTaskTimeoutAllowed;
      LOG.info("Task timeout must be as least twice as long as the task " +
          "status report interval. Setting task timeout to " + taskTimeOut);
    }

    // 从配置加载检查间隔时间，默认30秒
    taskTimeOutCheckInterval =
        conf.getInt(MRJobConfig.TASK_TIMEOUT_CHECK_INTERVAL_MS, 30 * 1000);
  }

  @Override
  protected void serviceStart() throws Exception {
    // 启动后台检测线程
    lostTaskCheckerThread = new SubjectInheritingThread(new PingChecker());
    lostTaskCheckerThread.setName("TaskHeartbeatHandler PingChecker");
    lostTaskCheckerThread.start();
    super.serviceStart();
  }

  @Override
  protected void serviceStop() throws Exception {
    // 设置停止标志，中断后台检测线程
    stopped = true;
    if (lostTaskCheckerThread != null) {
      lostTaskCheckerThread.interrupt();
    }
    super.serviceStop();
  }

  /**
   * 处理任务尝试的进度上报，更新最后心跳时间。
   * @param attemptID 上报进度的任务尝试ID
   */
  public void progressing(TaskAttemptId attemptID) {
  //only put for the registered attempts
    //TODO throw an exception if the task isn't registered.
    ReportTime time = runningAttempts.get(attemptID);
    if(time != null) {
      // 标记该任务已经上报过至少一次进度
      time.reported.compareAndSet(false, true);
      // 更新最后心跳时间
      time.setLastProgress(clock.getTime());
    }
  }

  
  /**
   * 注册一个新启动的任务尝试，加入心跳监控。
   * @param attemptID 新启动的任务尝试ID
   */
  public void register(TaskAttemptId attemptID) {
    runningAttempts.put(attemptID, new ReportTime(clock.getTime()));
  }

  /**
   * 注销一个已完成的任务尝试，移出运行监控，加入延迟清理队列。
   * @param attemptID 已完成的任务尝试ID
   */
  public void unregister(TaskAttemptId attemptID) {
    runningAttempts.remove(attemptID);
    recentlyUnregisteredAttempts.put(attemptID,
        new ReportTime(clock.getTime()));
  }

  /**
   * 检查指定任务尝试是否最近刚注销。
   * @param attemptID 待检查的任务尝试ID
   * @return true表示该任务尝试最近注销，false表示不在最近注销列表中
   */
  public boolean hasRecentlyUnregistered(TaskAttemptId attemptID) {
    return recentlyUnregisteredAttempts.containsKey(attemptID);
  }

  /**
   * 后台定时检测任务心跳的Runnable实现，定期扫描运行任务和已注销任务，处理超时。
   */
  private class PingChecker implements Runnable {

    @Override
    public void run() {
      while (!stopped && !Thread.currentThread().isInterrupted()) {
        // 获取当前检测时间点
        long currentTime = clock.getTime();
        // 检测运行任务是否超时卡住
        checkRunning(currentTime);
        // 清理超时的已注销任务记录
        checkRecentlyUnregistered(currentTime);
        try {
          // 等待下一次检测周期
          Thread.sleep(taskTimeOutCheckInterval);
        } catch (InterruptedException e) {
          LOG.info("TaskHeartbeatHandler thread interrupted");
          break;
        }
      }
    }

    /**
     * 遍历所有正在运行的任务尝试，检测是否超时或者卡住，对异常任务触发超时事件。
     * @param currentTime 当前检测时间点
     */
    private void checkRunning(long currentTime) {
      Iterator<Map.Entry<TaskAttemptId, ReportTime>> iterator =
          runningAttempts.entrySet().iterator();

      while (iterator.hasNext()) {
        Map.Entry<TaskAttemptId, ReportTime> entry = iterator.next();
        // 判断任务是否超过心跳超时时间
        boolean taskTimedOut = (taskTimeOut > 0) &&
            (currentTime > (entry.getValue().getLastProgress() + taskTimeOut));
        // 当容器长时间未启动成功，从未上报过进度，则判定任务卡住
        boolean taskStuck = (taskStuckTimeOut > 0) &&
            (!entry.getValue().isReported()) &&
            (currentTime >
                (entry.getValue().getLastProgress() + taskStuckTimeOut));

        if(taskTimedOut || taskStuck) {
          // 任务丢失，从运行列表移除，发送超时诊断事件和任务超时事件
          iterator.remove();
          eventHandler.handle(new TaskAttemptDiagnosticsUpdateEvent(entry
              .getKey(), "AttemptID:" + entry.getKey().toString()
              + " task timeout set: " + taskTimeOut / 1000 + "s,"
              + " taskTimedOut: " + taskTimedOut + ";"
              + " task stuck timeout set: " + taskStuckTimeOut / 1000 + "s,"
              + " taskStuck: " + taskStuck));
          eventHandler.handle(new TaskAttemptEvent(entry.getKey(),
              TaskAttemptEventType.TA_TIMED_OUT));
        }
      }
    }

    /**
     * 遍历最近已注销的任务尝试，清理超过保留超时时间的记录，释放内存。
     * @param currentTime 当前检测时间点
     */
    private void checkRecentlyUnregistered(long currentTime) {
      Iterator<ReportTime> iterator =
          recentlyUnregisteredAttempts.values().iterator();
      while (iterator.hasNext()) {
        ReportTime unregisteredTime = iterator.next();
        if (currentTime >
            unregisteredTime.getLastProgress() + unregisterTimeOut) {
          // 超过保留时间，移除记录
          iterator.remove();
        }
      }
    }
  }

  @VisibleForTesting
  ConcurrentMap<TaskAttemptId, ReportTime> getRunningAttempts(){
    return runningAttempts;
  }

  @VisibleForTesting
  public long getTaskTimeOut() {
    return taskTimeOut;
  }
}