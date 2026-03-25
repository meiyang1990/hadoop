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

package org.apache.hadoop.mapreduce.v2.app.webapp;

import javax.inject.Inject;
import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.app.job.Task;

/**
 * MapReduce Application Web UI 上下文持有类，用于在Web页面渲染过程中共享当前请求关联的应用、作业和任务信息
 */
public class App {
  final AppContext context;
  private Job job;
  private Task task;

  /**
   * 构造函数，注入应用上下文
   * @param ctx MapReduce应用上下文，包含整个应用的全局信息
   */
  @Inject
  public App(AppContext ctx) {
    context = ctx;
  }

  /**
   * 设置当前请求关联的作业对象
   * @param job 作业实例对象
   */
  void setJob(Job job) {
    this.job = job;
  }

  /**
   * 获取当前请求关联的作业对象
   * @return 当前作业实例
   */
  public Job getJob() {
    return job;
  }

  /**
   * 设置当前请求关联的任务对象
   * @param task 任务实例对象
   */
  void setTask(Task task) {
    this.task = task;
  }

  /**
   * 获取当前请求关联的任务对象
   * @return 当前任务实例
   */
  public Task getTask() {
    return task;
  }
}