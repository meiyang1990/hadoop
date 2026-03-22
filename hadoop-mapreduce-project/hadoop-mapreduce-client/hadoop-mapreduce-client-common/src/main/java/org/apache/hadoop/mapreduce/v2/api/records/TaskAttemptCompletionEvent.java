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

package org.apache.hadoop.mapreduce.v2.api.records;

/**
 * 任务尝试完成事件接口，承载MapReduce任务尝试执行完成后的相关信息
 * 用于ApplicationMaster向ResourceManager汇报任务尝试的完成状态和结果信息
 */
public interface TaskAttemptCompletionEvent {
  /**
   * 获取完成事件对应的任务尝试ID
   * @return 任务尝试唯一标识ID
   */
  public abstract TaskAttemptId getAttemptId();
  
  /**
   * 获取任务尝试的完成状态
   * @return 任务尝试完成状态枚举（成功/失败等）
   */
  public abstract TaskAttemptCompletionEventStatus getStatus();
  
  /**
   * 获取Map任务输出所在的服务器地址
   * @return Map输出服务地址字符串
   */
  public abstract String getMapOutputServerAddress();
  
  /**
   * 获取任务尝试的运行时长
   * @return 任务尝试运行时间，单位毫秒
   */
  public abstract int getAttemptRunTime();
  
  /**
   * 获取该完成事件的ID
   * @return 事件唯一ID
   */
  public abstract int getEventId();
  
  /**
   * 设置完成事件对应的任务尝试ID
   * @param taskAttemptId 任务尝试唯一标识ID
   */
  public abstract void setAttemptId(TaskAttemptId taskAttemptId);
  
  /**
   * 设置任务尝试的完成状态
   * @param status 任务尝试完成状态枚举
   */
  public abstract void setStatus(TaskAttemptCompletionEventStatus status);
  
  /**
   * 设置Map任务输出所在的服务器地址
   * @param address Map输出服务地址字符串
   */
  public abstract void setMapOutputServerAddress(String address);
  
  /**
   * 设置任务尝试的运行时长
   * @param runTime 任务尝试运行时间，单位毫秒
   */
  public abstract void setAttemptRunTime(int runTime);
  
  /**
   * 设置该完成事件的ID
   * @param eventId 事件唯一ID
   */
  public abstract void setEventId(int eventId);
}