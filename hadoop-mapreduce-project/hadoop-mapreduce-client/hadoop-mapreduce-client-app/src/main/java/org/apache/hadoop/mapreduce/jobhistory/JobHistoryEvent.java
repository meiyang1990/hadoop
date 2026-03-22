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

package org.apache.hadoop.mapreduce.jobhistory;

import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.yarn.event.AbstractEvent;

/**
 * 作业历史事件封装类，用于将通用MapReduce作业/任务/尝试的历史事件封装为YARN事件
 * 供作业历史事件处理系统进行异步处理，继承自YARN的AbstractEvent实现事件模型
 */
public class JobHistoryEvent extends AbstractEvent<EventType>{

  private final JobId jobID;
  private final HistoryEvent historyEvent;

  /**
   * 构造作业历史事件，使用当前系统时间作为事件时间戳
   * @param jobID 所属作业的ID
   * @param historyEvent 实际的历史事件对象
   */
  public JobHistoryEvent(JobId jobID, HistoryEvent historyEvent) {
    this(jobID, historyEvent, System.currentTimeMillis());
  }

  /**
   * 构造作业历史事件，指定自定义事件时间戳
   * @param jobID 所属作业的ID
   * @param historyEvent 实际的历史事件对象
   * @param timestamp 事件发生的时间戳
   */
  public JobHistoryEvent(JobId jobID, HistoryEvent historyEvent,
          long timestamp) {
    super(historyEvent.getEventType(), timestamp);
    this.jobID = jobID;
    this.historyEvent = historyEvent;
  }

  /**
   * 获取该事件所属作业的ID
   * @return 作业ID对象
   */
  public JobId getJobID() {
    return jobID;
  }

  /**
   * 获取实际封装的历史事件对象
   * @return 具体的历史事件实例
   */
  public HistoryEvent getHistoryEvent() {
    return historyEvent;
  }
}