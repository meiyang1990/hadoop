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

package org.apache.hadoop.mapreduce.v2.api.protocolrecords;

import java.util.List;

import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptCompletionEvent;

/**
 * 获取任务尝试完成事件响应协议接口，封装了从ApplicationMaster获取任务完成事件列表的响应结果
 * 用于MR客户端查询作业中已完成任务的事件信息，支撑作业状态监控与进度追踪
 */
public interface GetTaskAttemptCompletionEventsResponse {
  /**
   * 获取所有任务尝试完成事件列表
   * @return 所有已完成任务尝试的事件列表
   */
  public abstract List<TaskAttemptCompletionEvent> getCompletionEventList();
  
  /**
   * 根据索引获取指定位置的任务尝试完成事件
   * @param index 事件索引位置
   * @return 指定索引处的任务尝试完成事件
   */
  public abstract TaskAttemptCompletionEvent getCompletionEvent(int index);
  
  /**
   * 获取任务尝试完成事件的总数量
   * @return 事件总数量
   */
  public abstract int getCompletionEventCount();
  
  /**
   * 批量添加多个任务尝试完成事件到响应结果
   * @param eventList 待添加的任务完成事件列表
   */
  public abstract void addAllCompletionEvents(List<TaskAttemptCompletionEvent> eventList);
  
  /**
   * 添加单个任务尝试完成事件到响应结果
   * @param event 待添加的单个任务完成事件
   */
  public abstract void addCompletionEvent(TaskAttemptCompletionEvent event);
  
  /**
   * 移除指定索引位置的任务尝试完成事件
   * @param index 待移除事件的索引位置
   */
  public abstract void removeCompletionEvent(int index);
  
  /**
   * 清空所有任务尝试完成事件
   */
  public abstract void clearCompletionEvents();  
}