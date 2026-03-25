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

import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;

/**
 * 作业初始化提交事件类，封装作业初始化阶段所需的上下文信息
 * 用于触发输出提交器完成作业运行前的初始化工作，属于MapReduce ApplicationMaster提交器事件体系
 */
public class CommitterJobSetupEvent extends CommitterEvent {

  private JobId jobID;
  private JobContext jobContext;

  /**
   * 构造作业初始化提交事件
   * @param jobID 作业ID标识
   * @param jobContext 作业运行上下文对象，包含作业配置等信息
   */
  public CommitterJobSetupEvent(JobId jobID, JobContext jobContext) {
    super(CommitterEventType.JOB_SETUP);
    this.jobID = jobID;
    this.jobContext = jobContext;
  }

  /**
   * 获取当前作业的ID标识
   * @return 作业ID对象
   */
  public JobId getJobID() {
    return jobID;
  }

  /**
   * 获取当前作业的运行上下文
   * @return 作业上下文对象，包含作业配置等信息
   */
  public JobContext getJobContext() {
    return jobContext;
  }
}