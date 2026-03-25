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

package org.apache.hadoop.mapreduce.v2.app.speculate;

import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.app.job.Task;

/**
 * 任务推测执行判断基类，用于判断一个任务是否满足开启推测执行的基础条件。
 * 子类可以继承该类扩展更严格的推测执行判断逻辑，核心职责是提供基础的合法性检查。
 */
public class TaskSpeculationPredicate {
  /**
   * 判断指定任务是否满足开启推测执行的基础条件。
   * 基础规则：仅允许当前只有1个运行中尝试（未启动过推测）的任务开启推测，拒绝已有推测或未运行的任务。
   * @param context 应用上下文，可从中获取作业和任务信息
   * @param taskID 待判断的任务ID
   * @return true表示允许开启推测执行，false表示禁止
   */
  boolean canSpeculate(AppContext context, TaskId taskID) {
    // This class rejects speculating any task that already has speculations,
    //  or isn't running.
    //  Subclasses should call TaskSpeculationPredicate.canSpeculate(...) , but
    //  can be even more restrictive.
    JobId jobID = taskID.getJobId();
    Job job = context.getJob(jobID);
    Task task = job.getTask(taskID);
    return task.getAttempts().size() == 1;
  }
}