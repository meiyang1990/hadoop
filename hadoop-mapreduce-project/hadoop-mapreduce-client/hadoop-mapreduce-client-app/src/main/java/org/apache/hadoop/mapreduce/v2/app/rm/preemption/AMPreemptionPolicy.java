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

import java.util.List;

import org.apache.hadoop.mapreduce.checkpoint.TaskCheckpointID;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.PreemptionMessage;

/**
 * 应用Master抢占策略接口，定义了MRAppMaster响应ResourceManager容器抢占请求的处理规范
 * 不同实现可以提供不同的抢占策略，决定如何响应YARN的资源回收请求
 * @see org.apache.hadoop.mapreduce.v2.app.rm.RMContainerAllocator
 */
public interface AMPreemptionPolicy {

  /**
   * 应用抢占策略上下文，提供查询当前运行容器和任务关联信息的接口
   * 供抢占策略获取应用当前运行状态，用于决策需要抢占哪些容器
   */
  public abstract class Context {

    /**
     * 根据容器ID获取关联的任务尝试ID
     * @param container 待抢占容器ID
     * @return 运行在此容器上的任务尝试ID，如果无任务绑定则返回<code>null</code>
     */
    public abstract TaskAttemptId getTaskAttempt(ContainerId container);

    /**
     * 获取当前AM上所有指定类型任务正在使用的容器列表
     * @param t 任务类型（Map/Reduce）
     * @return 指定类型的所有正在运行容器列表
     */
    public abstract List<Container> getContainers(TaskType t);

  }

  /**
   * 初始化抢占策略，传入应用上下文
   * @param context 应用运行上下文
   */
  public void init(AppContext context);

  /**
   * 处理来自ResourceManager的抢占请求回调，策略可根据请求选择检查点保存、主动归还容器或忽略请求
   * 如果超时未主动归还，RM会强制杀死未归还的容器
   * @param context 当前运行容器的状态上下文
   * @param preemptionRequests RM发出的资源抢占请求
   */
  public void preempt(Context context, PreemptionMessage preemptionRequests);

  /**
   * 查询指定任务尝试是否正在被抢占
   * @param attemptID 待查询的任务尝试ID
   * @return true if 该任务尝试正在被抢占
   */
  public boolean isPreempted(TaskAttemptId attemptID);

  /**
   * 上报任务抢占成功，供策略进行记账、更新统计等后续处理
   * @param attemptID 成功被抢占的任务尝试ID
   */
  public void reportSuccessfulPreemption(TaskAttemptId attemptID);

  /**
   * 处理容器失败退出事件，允许策略执行清理或补偿操作
   * @param attemptID 失败容器绑定的任务尝试ID
   */
  public void handleFailedContainer(TaskAttemptId attemptID);

  /**
   * 处理容器正常完成退出事件，供策略进行记账处理
   * @param attemptID 正常完成的任务尝试ID
   */
  public void handleCompletedContainer(TaskAttemptId attemptID);

  /**
   * 获取指定任务最新的检查点ID
   * @param taskId 任务ID
   * @return 该任务关联的检查点ID，如果无检查点则返回null
   */
  public TaskCheckpointID getCheckpointID(TaskId taskId);

  /**
   * 设置指定任务的最新检查点ID，传入null会清除该任务所有已有检查点
   * @param taskId 任务ID
   * @param cid 要分配的检查点ID，传入<code>null</code>清除该任务所有检查点
   */
  public void setCheckpointID(TaskId taskId, TaskCheckpointID cid);

}