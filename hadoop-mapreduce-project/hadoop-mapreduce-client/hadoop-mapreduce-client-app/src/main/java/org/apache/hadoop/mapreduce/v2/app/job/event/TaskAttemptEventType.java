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

package org.apache.hadoop.mapreduce.v2.app.job.event;

/**
 * TaskAttempt处理的事件类型枚举，定义了MapReduce应用中任务尝试所有可能的事件类型，
 * 用于驱动任务尝试状态机流转，区分不同事件来源和处理逻辑。
 */
public enum TaskAttemptEventType {

  // 事件生产者：Task，调度任务尝试执行
  TA_SCHEDULE,
  // 事件生产者：Task，重新调度任务尝试
  TA_RESCHEDULE,
  // 事件生产者：Task，恢复任务尝试执行
  TA_RECOVER,

  // 事件生产者：客户端、Task，杀死任务尝试
  TA_KILL,

  // 事件生产者：容器分配器，任务尝试已分配容器
  TA_ASSIGNED,
  // 事件生产者：容器分配器，任务尝试对应的容器已完成
  TA_CONTAINER_COMPLETED,

  // 事件生产者：容器启动器，容器已成功启动
  TA_CONTAINER_LAUNCHED,
  // 事件生产者：容器启动器，容器启动失败
  TA_CONTAINER_LAUNCH_FAILED,
  // 事件生产者：容器启动器，容器已清理完成
  TA_CONTAINER_CLEANED,

  // 事件生产者：任务尝试监听器，更新诊断信息
  TA_DIAGNOSTICS_UPDATE,
  // 事件生产者：任务尝试监听器，任务尝试提交等待中
  TA_COMMIT_PENDING, 
  // 事件生产者：任务尝试监听器，任务尝试执行完成
  TA_DONE,
  // 事件生产者：任务尝试监听器，任务尝试失败消息
  TA_FAILMSG,
  // 事件生产者：任务尝试监听器，更新任务尝试状态
  TA_UPDATE,
  // 事件生产者：任务尝试监听器，任务尝试超时
  TA_TIMED_OUT,
  // 事件生产者：任务尝试监听器，任务尝试被抢占
  TA_PREEMPTED,

  // 事件生产者：客户端，客户端发起的任务尝试失败通知
  TA_FAILMSG_BY_CLIENT,

  // 事件生产者：任务清理器，任务尝试清理完成
  TA_CLEANUP_DONE,

  // 事件生产者：Job，任务尝试存在过多获取失败，标记失败
  TA_TOO_MANY_FETCH_FAILURE,
}