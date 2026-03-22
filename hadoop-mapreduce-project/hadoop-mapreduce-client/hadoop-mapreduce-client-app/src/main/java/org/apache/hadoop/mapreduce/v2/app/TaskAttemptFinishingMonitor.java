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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptEventType;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.util.AbstractLivelinessMonitor;
import org.apache.hadoop.yarn.util.SystemClock;

/**
 * 文件职责：MapReduce ApplicationMaster中监控任务尝试结束阶段的存活监视器
 * 
 * 本类负责监控处于FINISHING状态的任务尝试，如果任务尝试长时间停留在该状态未完成，
 * 会主动发送超时事件将其判定为超时失败，避免作业卡住。
 */
@SuppressWarnings({"unchecked", "rawtypes"})
public class TaskAttemptFinishingMonitor extends
    AbstractLivelinessMonitor<TaskAttemptId> {

  private EventHandler eventHandler;

  /**
   * 构造函数，初始化任务尝试结束状态监视器
   * @param eventHandler 事件处理器，用于发送超时事件
   */
  public TaskAttemptFinishingMonitor(EventHandler eventHandler) {
    super("TaskAttemptFinishingMonitor", SystemClock.getInstance());
    this.eventHandler = eventHandler;
  }

  /**
   * 从配置中初始化超时参数和检查间隔
   * @param conf MapReduce作业配置
   */
  public void init(Configuration conf) {
    super.init(conf);
    // 读取任务超时时间配置
    int expireIntvl = conf.getInt(MRJobConfig.TASK_EXIT_TIMEOUT,
        MRJobConfig.TASK_EXIT_TIMEOUT_DEFAULT);
    // 读取超时检查间隔配置
    int checkIntvl = conf.getInt(
        MRJobConfig.TASK_EXIT_TIMEOUT_CHECK_INTERVAL_MS,
        MRJobConfig.TASK_EXIT_TIMEOUT_CHECK_INTERVAL_MS_DEFAULT);

    // 设置过期时间
    setExpireInterval(expireIntvl);
    // 设置监控检查间隔
    setMonitorInterval(checkIntvl);
  }

  /**
   * 处理过期的任务尝试，发送超时事件通知AM处理
   * @param id 已超时的任务尝试ID
   */
  @Override
  protected void expire(TaskAttemptId id) {
    eventHandler.handle(
        new TaskAttemptEvent(id,
        TaskAttemptEventType.TA_TIMED_OUT));
  }
}