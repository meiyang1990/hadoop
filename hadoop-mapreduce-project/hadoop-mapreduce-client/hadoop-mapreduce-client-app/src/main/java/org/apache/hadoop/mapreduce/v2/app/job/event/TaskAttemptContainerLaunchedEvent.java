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

import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;

/**
 * 任务尝试容器启动完成事件，用于通知应用Master任务尝试的容器已启动并提供Shuffle服务端口
 * 当NodeManager启动Map任务尝试的容器后，发送该事件告知AppMaster Shuffle服务监听端口
 */
public class TaskAttemptContainerLaunchedEvent extends TaskAttemptEvent {
  // Shuffle服务监听端口
  private int shufflePort;

  /**
   * 构造任务尝试容器启动事件
   * @param id 任务尝试ID
   * @param shufflePort Shuffle服务监听端口
   */
  public TaskAttemptContainerLaunchedEvent(TaskAttemptId id, int shufflePort) {
    super(id, TaskAttemptEventType.TA_CONTAINER_LAUNCHED);
    this.shufflePort = shufflePort;
  }

  
  /**
   * 获取Shuffle服务监听端口，仅当事件类型为TA_CONTAINER_LAUNCHED时有效
   * @return Shuffle服务监听端口
   */
  public int getShufflePort() {
    return shufflePort;
  }
}