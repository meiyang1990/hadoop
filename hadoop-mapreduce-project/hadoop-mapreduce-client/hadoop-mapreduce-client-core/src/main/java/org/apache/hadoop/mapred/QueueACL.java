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

/**
 * 队列访问控制权限枚举，定义了用户/用户组可对MapReduce调度队列执行的操作类型
 * 用于YARN队列级别的权限控制，不同权限对应不同操作的访问许可
 */
@InterfaceAudience.Private
public enum QueueACL {
  /** 提交作业到队列的权限 */
  SUBMIT_JOB ("acl-submit-job"),
  /** 管理队列中已有作业的权限，包含杀死任务/作业、修改作业优先级、查看作业信息等操作 */
  ADMINISTER_JOBS ("acl-administer-jobs");
  // Currently this ACL acl-administer-jobs is checked for the operations
  // FAIL_TASK, KILL_TASK, KILL_JOB, SET_JOB_PRIORITY and VIEW_JOB.

  // TODO: Add ACL for LIST_JOBS when we have ability to authenticate
  //       users in UI
  // TODO: Add ACL for CHANGE_ACL when we have an admin tool for
  //       configuring queues.

  /** ACL配置项名称，对应配置文件中的配置键 */
  private final String aclName;

  /**
   * 构造队列ACL枚举实例
   * @param aclName ACL配置项名称
   */
  QueueACL(String aclName) {
    this.aclName = aclName;
  }

  /**
   * 获取当前ACL对应的配置项名称
   * @return 配置项名称字符串
   */
  public final String getAclName() {
    return aclName;
  }
}