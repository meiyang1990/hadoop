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
 * MapReduce Application 中Task模块处理的事件类型枚举
 * 定义了所有Task可以接收的事件类型，以及事件的产生来源
 */
public enum TaskEventType {

  // 事件产生方：客户端、Job
  T_KILL,

  // 事件产生方：Job
  T_SCHEDULE,
  T_RECOVER,

  // 事件产生方：推测执行器
  T_ADD_SPEC_ATTEMPT,

  // 事件产生方：TaskAttempt
  T_ATTEMPT_LAUNCHED,
  T_ATTEMPT_COMMIT_PENDING,
  T_ATTEMPT_FAILED,
  T_ATTEMPT_SUCCEEDED,
  T_ATTEMPT_KILLED
}