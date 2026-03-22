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

package org.apache.hadoop.mapreduce.v2.app;

import java.net.InetSocketAddress;

import org.apache.hadoop.mapred.Task;
import org.apache.hadoop.mapred.WrappedJvmID;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;

/**
 * @file TaskAttemptListener.java
 * @brief MapReduce ApplicationMaster中监听任务尝试状态变更的接口
 * 
 * 该接口定义了任务尝试生命周期各个阶段的回调注册契约，
 * 用于管理任务尝试对应的JVM生命周期，接收任务执行过程中的状态更新。
 */
public interface TaskAttemptListener {

  /**
   * 获取监听器服务的监听地址
   * @return 监听器绑定的网络地址
   */
  InetSocketAddress getAddress();

  /**
   * 注册待启动的任务到JVM
   * 当任务尝试分配到JVM ID后、JVM启动前调用，记录待启动的任务关联关系
   * @param task 待执行的任务对象
   * @param jvmID 分配给该任务的JVM ID
   */
  void registerPendingTask(Task task, WrappedJvmID jvmID);
  
  /**
   * 注册已启动的任务尝试
   * JVM启动完成后调用，标记该JVM上的任务尝试已经成功启动
   * @param attemptID 任务尝试ID
   * @param jvmID 运行该任务尝试的JVM ID
   */
  void registerLaunchedTask(TaskAttemptId attemptID, WrappedJvmID jvmID);

  /**
   * 注销已完成的任务尝试和关联的JVM
   * 任务尝试执行完成、资源清理阶段调用，移除相关注册信息
   * @param attemptID 已完成的任务尝试ID
   * @param jvmID 运行该任务尝试的JVM ID
   */
  void unregister(TaskAttemptId attemptID, WrappedJvmID jvmID);

}