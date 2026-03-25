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

package org.apache.hadoop.mapreduce.v2.app.commit;

/**
 * 作业提交器事件类型枚举，定义MapReduce应用提交过程中所有可能的事件类型
 */
public enum CommitterEventType {
  /** 作业初始化设置事件 */
  JOB_SETUP,
  /** 作业提交完成事件 */
  JOB_COMMIT,
  /** 作业终止事件 */
  JOB_ABORT,
  /** 任务终止事件 */
  TASK_ABORT
}