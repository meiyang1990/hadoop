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

package org.apache.hadoop.mapreduce.v2.app.launcher;

import org.apache.hadoop.mapred.Task;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.util.StringInterner;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;

/**
 * 容器远程启动事件，承载YARN容器远程启动所需的全部上下文信息
 * 用于向容器启动器发送远程启动任务尝试的事件通知
 */
public class ContainerRemoteLaunchEvent extends ContainerLauncherEvent {

  private final Container allocatedContainer;
  private final ContainerLaunchContext containerLaunchContext;
  private final Task task;

  /**
   * 构造容器远程启动事件，初始化所有启动所需信息
   * @param taskAttemptID 任务尝试ID，标识本次要启动的任务尝试
   * @param containerLaunchContext YARN容器启动上下文，包含启动命令、环境变量等信息
   * @param allocatedContainer YARN分配给本次任务的容器对象
   * @param remoteTask 要远程执行的MapReduce任务对象
   */
  public ContainerRemoteLaunchEvent(TaskAttemptId taskAttemptID,
      ContainerLaunchContext containerLaunchContext,
      Container allocatedContainer, Task remoteTask) {
    super(taskAttemptID, allocatedContainer.getId(), StringInterner
      .weakIntern(allocatedContainer.getNodeId().toString()),
      allocatedContainer.getContainerToken(),
      ContainerLauncher.EventType.CONTAINER_REMOTE_LAUNCH);
    this.allocatedContainer = allocatedContainer;
    this.containerLaunchContext = containerLaunchContext;
    this.task = remoteTask;
  }

  /**
   * 获取YARN容器启动上下文
   * @return 容器启动上下文对象，包含启动参数信息
   */
  public ContainerLaunchContext getContainerLaunchContext() {
    return this.containerLaunchContext;
  }

  /**
   * 获取YARN分配给本次任务的容器对象
   * @return 已分配的YARN容器对象
   */
  public Container getAllocatedContainer() {
    return this.allocatedContainer;
  }

  /**
   * 获取需要远程执行的MapReduce任务对象
   * @return 远程执行的任务对象
   */
  public Task getRemoteTask() {
    return this.task;
  }
  
  @Override
  public int hashCode() {
    return super.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    return super.equals(obj);
  }
}