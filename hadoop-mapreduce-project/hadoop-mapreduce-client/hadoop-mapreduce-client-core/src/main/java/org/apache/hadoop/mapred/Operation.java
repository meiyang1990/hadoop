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
package org.apache.hadoop.mapred;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.mapreduce.JobACL;

/**
 * MapReduce操作权限枚举，定义了各类MapReduce操作及其所需的队列ACL和作业ACL权限。
 * 用于操作授权检查，每个操作绑定对应的权限要求，驱动操作的访问控制。
 */
@InterfaceAudience.Private
public enum Operation {
  /** 查看作业计数器，需要队列管理作业权限或作业查看权限 */
  VIEW_JOB_COUNTERS(QueueACL.ADMINISTER_JOBS, JobACL.VIEW_JOB),
  /** 查看作业详情，需要队列管理作业权限或作业查看权限 */
  VIEW_JOB_DETAILS(QueueACL.ADMINISTER_JOBS, JobACL.VIEW_JOB),
  /** 查看任务日志，需要队列管理作业权限或作业查看权限 */
  VIEW_TASK_LOGS(QueueACL.ADMINISTER_JOBS, JobACL.VIEW_JOB),
  /** 杀死作业，需要队列管理作业权限或作业修改权限 */
  KILL_JOB(QueueACL.ADMINISTER_JOBS, JobACL.MODIFY_JOB),
  /** 标记任务失败，需要队列管理作业权限或作业修改权限 */
  FAIL_TASK(QueueACL.ADMINISTER_JOBS, JobACL.MODIFY_JOB),
  /** 杀死任务，需要队列管理作业权限或作业修改权限 */
  KILL_TASK(QueueACL.ADMINISTER_JOBS, JobACL.MODIFY_JOB),
  /** 修改作业优先级，需要队列管理作业权限或作业修改权限 */
  SET_JOB_PRIORITY(QueueACL.ADMINISTER_JOBS, JobACL.MODIFY_JOB),
  /** 提交作业，需要队列提交作业权限 */
  SUBMIT_JOB(QueueACL.SUBMIT_JOB, null);

  // 操作所需的队列级ACL权限
  private final QueueACL qACLNeeded;
  // 操作所需的作业级ACL权限
  private final JobACL jobACLNeeded;
  
  /**
   * 构造操作枚举实例，绑定所需权限
   * @param qACL 所需队列级ACL
   * @param jobACL 所需作业级ACL
   */
  Operation(QueueACL qACL, JobACL jobACL) {
    this.qACLNeeded = qACL;
    this.jobACLNeeded = jobACL;
  }

  /**
   * 获取操作所需的队列级ACL权限
   * @return 队列ACL权限
   */
  public QueueACL getqACLNeeded() {
    return qACLNeeded;
  }

  /**
   * 获取操作所需的作业级ACL权限
   * @return 作业ACL权限
   */
  public JobACL getJobACLNeeded() {
    return jobACLNeeded;
  }
}