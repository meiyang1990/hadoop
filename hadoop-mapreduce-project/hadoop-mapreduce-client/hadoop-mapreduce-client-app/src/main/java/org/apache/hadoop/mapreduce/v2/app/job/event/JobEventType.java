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
 * MapReduce作业处理的事件类型枚举，定义了作业生命周期中所有可能发生的事件类型，
 * 用于作业状态机驱动作业状态流转，不同事件由不同组件产生触发。
 */
public enum JobEventType {

  // 生产者：客户端，客户端请求杀死作业
  JOB_KILL,

  // 生产者：MRAppMaster，作业初始化
  JOB_INIT,
  // 生产者：MRAppMaster，作业初始化失败
  JOB_INIT_FAILED,
  // 生产者：MRAppMaster，作业开始执行
  JOB_START,

  // 生产者：Task，任务完成
  JOB_TASK_COMPLETED,
  // 生产者：Task，Map任务需要重新调度
  JOB_MAP_TASK_RESCHEDULED,
  // 生产者：Task，任务尝试执行完成
  JOB_TASK_ATTEMPT_COMPLETED,

  // 生产者：CommitterEventHandler，作业初始化完成
  JOB_SETUP_COMPLETED,
  // 生产者：CommitterEventHandler，作业初始化失败
  JOB_SETUP_FAILED,
  // 生产者：CommitterEventHandler，作业提交完成
  JOB_COMMIT_COMPLETED,
  // 生产者：CommitterEventHandler，作业提交失败
  JOB_COMMIT_FAILED,
  // 生产者：CommitterEventHandler，作业终止完成
  JOB_ABORT_COMPLETED,

  // 生产者：Job，作业整体执行完成
  JOB_COMPLETED,
  // 生产者：Job，作业失败等待超时
  JOB_FAIL_WAIT_TIMEDOUT,

  // 生产者：任意组件，作业诊断信息更新
  JOB_DIAGNOSTIC_UPDATE,
  // 生产者：任意组件，内部错误发生
  INTERNAL_ERROR,
  // 生产者：任意组件，作业计数器更新
  JOB_COUNTER_UPDATE,
  
  // 生产者：TaskAttemptListener，任务尝试获取数据失败
  JOB_TASK_ATTEMPT_FETCH_FAILURE,
  
  // 生产者：RMContainerAllocator，集群节点信息更新
  JOB_UPDATED_NODES,
  // 生产者：RMContainerAllocator，ApplicationMaster重启
  JOB_AM_REBOOT
}