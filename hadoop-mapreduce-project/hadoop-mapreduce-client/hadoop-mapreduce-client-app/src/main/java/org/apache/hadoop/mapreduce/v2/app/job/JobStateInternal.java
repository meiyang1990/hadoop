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

package org.apache.hadoop.mapreduce.v2.app.job;

/**
 * MapReduce作业内部状态枚举
 * 定义了ApplicationMaster内部维护作业完整生命周期的所有状态，
 * 用于状态机流转控制作业执行流程，比对外公开的JobStatus状态更细化
 */
public enum JobStateInternal {
  /** 作业刚创建，未初始化 */
  NEW,
  /** 作业正在执行setup初始化阶段 */
  SETUP,
  /** 作业初始化完成，等待调度任务 */
  INITED,
  /** 作业正在运行，任务执行中 */
  RUNNING,
  /** 作业执行完成，正在提交结果 */
  COMMITTING,
  /** 作业执行成功 */
  SUCCEEDED,
  /** 作业失败，等待清理任务完成 */
  FAIL_WAIT,
  /** 作业失败，正在中止未完成任务 */
  FAIL_ABORT,
  /** 作业失败，已完成清理 */
  FAILED,
  /** 作业被杀死，等待清理任务完成 */
  KILL_WAIT,
  /** 作业被杀死，正在中止未完成任务 */
  KILL_ABORT,
  /** 作业被杀死，已完成清理 */
  KILLED,
  /** 作业运行出错，发生异常 */
  ERROR,
  /** AM重启，作业需要重新处理 */
  REBOOT
}