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

import org.apache.hadoop.mapreduce.JobCounter;
import org.apache.hadoop.mapreduce.checkpoint.TaskCheckpointID;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobCounterUpdateEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptEventType;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.PreemptionContainer;
import org.apache.hadoop.yarn.api.records.PreemptionContract;
import org.apache.hadoop.yarn.api.records.PreemptionMessage;
import org.apache.hadoop.yarn.api.records.StrictPreemptionContract;
import org.apache.hadoop.yarn.event.EventHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 强制抢占式杀死容器的抢占策略实现类，收到RM抢占请求后直接杀死对应任务容器。
 * 这是一种激进的抢占策略，只要资源管理器要求抢占就立即杀死对应任务容器释放资源。
 */
public class KillAMPreemptionPolicy implements AMPreemptionPolicy {

  private static final Logger LOG =
      LoggerFactory.getLogger(KillAMPreemptionPolicy.class);

  @SuppressWarnings("rawtypes")
  private EventHandler dispatcher = null;

  /**
   * 初始化抢占策略，获取应用上下文的事件分发器。
   * @param context 应用上下文
   */
  @Override
  public void init(AppContext context) {
    dispatcher = context.getEventHandler();
  }

  /**
   * 处理资源管理器发来的抢占请求，对所有要求抢占的容器执行杀死操作。
   * @param ctxt 抢占策略上下文，用于查询容器对应的任务尝试
   * @param preemptionRequests 资源管理器发来的抢占请求，包含强制和可协商两种抢占契约
   */
  @Override
  public void preempt(Context ctxt, PreemptionMessage preemptionRequests) {
    // 获取强制抢占契约，必须释放这些容器
    StrictPreemptionContract strictContract = preemptionRequests
        .getStrictContract();
    if (strictContract != null) {
      for (PreemptionContainer c : strictContract.getContainers()) {
        killContainer(ctxt, c);
      }
    }
    // 获取可协商抢占契约，也需要释放这些容器
    PreemptionContract contract = preemptionRequests.getContract();
    if (contract != null) {
      for (PreemptionContainer c : contract.getContainers()) {
        killContainer(ctxt, c);
      }
    }
  }

  /**
   * 杀死指定容器对应的任务尝试，并更新作业计数器。
   * @param ctxt 抢占策略上下文
   * @param c 待抢占的容器信息
   */
  @SuppressWarnings("unchecked")
  private void killContainer(Context ctxt, PreemptionContainer c){
    // 获取待抢占容器ID
    ContainerId reqCont = c.getId();
    // 查询容器对应的任务尝试ID
    TaskAttemptId reqTask = ctxt.getTaskAttempt(reqCont);
    LOG.info("Evicting " + reqTask);
    // 发送任务尝试杀死事件
    dispatcher.handle(new TaskAttemptEvent(reqTask,
        TaskAttemptEventType.TA_KILL));

    // 更新作业计数器统计抢占任务数量
    JobCounterUpdateEvent jce = new JobCounterUpdateEvent(reqTask
            .getTaskId().getJobId());
        jce.addCounterUpdate(JobCounter.TASKS_REQ_PREEMPT, 1);
        dispatcher.handle(jce);
  }

  /**
   * 处理抢占容器失败的情况，本策略不处理该事件。
   * @param attemptID 失败的任务尝试ID
   */
  @Override
  public void handleFailedContainer(TaskAttemptId attemptID) {
    // ignore
  }

  /**
   * 查询任务尝试是否已经被抢占，本策略不保存抢占状态始终返回false。
   * @param yarnAttemptID 任务尝试ID
   * @return 始终返回false
   */
  @Override
  public boolean isPreempted(TaskAttemptId yarnAttemptID) {
    return false;
  }

  /**
   * 报告抢占成功，本策略不处理该事件。
   * @param taskAttemptID 抢占成功的任务尝试ID
   */
  @Override
  public void reportSuccessfulPreemption(TaskAttemptId taskAttemptID) {
    // ignore
  }

  /**
   * 获取任务检查点ID，本策略不支持检查点始终返回null。
   * @param taskId 任务ID
   * @return 始终返回null
   */
  @Override
  public TaskCheckpointID getCheckpointID(TaskId taskId) {
    return null;
  }

  /**
   * 设置任务检查点ID，本策略不支持检查点不处理该请求。
   * @param taskId 任务ID
   * @param cid 检查点ID
   */
  @Override
  public void setCheckpointID(TaskId taskId, TaskCheckpointID cid) {
    // ignore
  }

  /**
   * 处理已完成容器，本策略不处理该事件。
   * @param attemptID 已完成任务尝试ID
   */
  @Override
  public void handleCompletedContainer(TaskAttemptId attemptID) {
    // ignore
  }

}