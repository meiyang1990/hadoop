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
package org.apache.hadoop.mapreduce.v2.app.rm.preemption;

import org.apache.hadoop.mapreduce.checkpoint.TaskCheckpointID;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.yarn.api.records.PreemptionMessage;

/**
 * 空实现抢占策略，完全忽略所有来自ResourceManager的容器抢占请求。
 * 用于不需要容器抢占功能的场景，提供AMPreemptionPolicy接口的默认空实现。
 */
public class NoopAMPreemptionPolicy implements AMPreemptionPolicy {

  /**
   * 初始化空抢占策略，不执行任何操作。
   * @param context 应用上下文
   */
  @Override
  public void init(AppContext context){
   // do nothing
  }

  /**
   * 处理抢占请求，空实现，忽略所有抢占请求。
   * @param ctxt 策略上下文
   * @param preemptionRequests RM下发的抢占请求
   */
  @Override
  public void preempt(Context ctxt, PreemptionMessage preemptionRequests) {
    // do nothing, ignore all requeusts
  }

  /**
   * 处理抢占失败的容器，空实现，不执行任何操作。
   * @param attemptID 任务尝试ID
   */
  @Override
  public void handleFailedContainer(TaskAttemptId attemptID) {
    // do nothing
  }

  /**
   * 判断指定任务尝试是否已被抢占，始终返回false。
   * @param yarnAttemptID 任务尝试ID
   * @return 永远返回false，表示没有任务被抢占
   */
  @Override
  public boolean isPreempted(TaskAttemptId yarnAttemptID) {
    return false;
  }

  /**
   * 处理成功抢占的任务报告，空实现，忽略报告。
   * @param taskAttemptID 任务尝试ID
   */
  @Override
  public void reportSuccessfulPreemption(TaskAttemptId taskAttemptID) {
    // ignore
  }

  /**
   * 获取指定任务的检查点ID，空实现始终返回null。
   * @param taskId 任务ID
   * @return 永远返回null，表示无检查点
   */
  @Override
  public TaskCheckpointID getCheckpointID(TaskId taskId) {
    return null;
  }

  /**
   * 设置指定任务的检查点ID，空实现，忽略操作。
   * @param taskId 任务ID
   * @param cid 检查点ID
   */
  @Override
  public void setCheckpointID(TaskId taskId, TaskCheckpointID cid) {
    // ignore
  }

  /**
   * 处理已完成的容器，空实现，忽略操作。
   * @param attemptID 任务尝试ID
   */
  @Override
  public void handleCompletedContainer(TaskAttemptId attemptID) {
    // ignore
  }

}