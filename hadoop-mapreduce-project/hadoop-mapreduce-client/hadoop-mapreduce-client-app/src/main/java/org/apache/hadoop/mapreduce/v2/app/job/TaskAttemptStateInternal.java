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

import org.apache.hadoop.classification.InterfaceAudience.Private;

/**
 * TaskAttempt 内部状态机的状态枚举定义，用于MapReduce ApplicationMaster中任务尝试的内部状态管理
 * 这些状态是TaskAttemptImpl实现类私有，不对外部公开，用于内部状态流转控制
 */
@Private
public enum TaskAttemptStateInternal {
  /** 新建状态，任务尝试刚创建完成 */
  NEW, 
  /** 未分配状态，等待分配容器资源 */
  UNASSIGNED, 
  /** 已分配状态，容器资源已分配完成 */
  ASSIGNED, 
  /** 运行中状态，任务尝试正在容器中运行 */
  RUNNING, 
  /** 提交挂起状态，任务执行完成等待提交 */
  COMMIT_PENDING,

  /**
   * 成功收尾容器状态：当TaskUmbilicalProtocol认为尝试已成功完成后进入该状态
   * 给容器留出自行退出的机会，进入该状态时会立即通知任务该尝试已成功，对作业而言任务即已成功
   * <p>
   * 状态转出规则：若在超时时间内未收到容器退出通知，则转入SUCCESS_CONTAINER_CLEANUP清理容器；
   * 若收到YARN的容器退出通知，则直接转入SUCCEEDED最终成功状态
   */
  SUCCESS_FINISHING_CONTAINER,

  /**
   * 失败收尾容器状态：当TaskUmbilicalProtocol认为尝试已失败后进入该状态
   * 给容器留出自行退出的机会，进入该状态时会立即通知任务该尝试已失败，对作业而言任务即已失败
   * <p>
   * 状态转出规则：若在超时时间内未收到容器退出通知，则转入FAIL_CONTAINER_CLEANUP清理容器；
   * 若收到YARN的容器退出通知，则直接转入FAILED最终失败状态
   */
  FAIL_FINISHING_CONTAINER,

  /** 成功容器清理状态：需要清理已成功尝试的容器资源 */
  SUCCESS_CONTAINER_CLEANUP,
  /** 成功完成，任务尝试最终成功状态 */
  SUCCEEDED,
  /** 失败容器清理状态：需要清理已失败尝试的容器资源 */
  FAIL_CONTAINER_CLEANUP, 
  /** 失败任务清理状态：需要清理失败任务相关资源 */
  FAIL_TASK_CLEANUP, 
  /** 失败完成，任务尝试最终失败状态 */
  FAILED, 
  /** 杀死容器清理状态：需要清理被杀死尝试的容器资源 */
  KILL_CONTAINER_CLEANUP, 
  /** 杀死任务清理状态：需要清理被杀死任务相关资源 */
  KILL_TASK_CLEANUP, 
  /** 杀死完成，任务尝试最终被杀死状态 */
  KILLED,
}