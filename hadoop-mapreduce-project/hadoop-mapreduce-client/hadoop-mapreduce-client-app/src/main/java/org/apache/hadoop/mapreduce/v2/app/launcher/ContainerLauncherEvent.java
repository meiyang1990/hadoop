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

import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Token;
import org.apache.hadoop.yarn.event.AbstractEvent;

/**
 * 容器启动器事件，封装MapReduce任务尝试容器启动相关的事件信息
 * 用于在MapReduce ApplicationMaster内部传递容器调度事件，继承YARN的通用事件模型
 */
public class ContainerLauncherEvent 
    extends AbstractEvent<ContainerLauncher.EventType> {

  // 关联的任务尝试ID
  private TaskAttemptId taskAttemptID;
  // YARN容器ID
  private ContainerId containerID;
  // NodeManager容器管理服务地址
  private String containerMgrAddress;
  // 容器访问令牌，用于身份认证
  private Token containerToken;
  // 是否需要转储容器线程堆栈（用于故障诊断）
  private boolean dumpContainerThreads;

  /**
   * 构造容器启动事件，默认不转储容器线程
   * @param taskAttemptID 关联的任务尝试ID
   * @param containerID YARN容器ID
   * @param containerMgrAddress NodeManager容器管理服务地址
   * @param containerToken 容器访问令牌
   * @param type 事件类型
   */
  public ContainerLauncherEvent(TaskAttemptId taskAttemptID, 
      ContainerId containerID,
      String containerMgrAddress,
      Token containerToken,
      ContainerLauncher.EventType type) {
    this(taskAttemptID, containerID, containerMgrAddress, containerToken, type,
        false);
  }

  /**
   * 构造容器启动事件，完整参数构造
   * @param taskAttemptID 关联的任务尝试ID
   * @param containerID YARN容器ID
   * @param containerMgrAddress NodeManager容器管理服务地址
   * @param containerToken 容器访问令牌
   * @param type 事件类型
   * @param dumpContainerThreads 是否转储容器线程堆栈用于故障诊断
   */
  public ContainerLauncherEvent(TaskAttemptId taskAttemptID,
      ContainerId containerID,
      String containerMgrAddress,
      Token containerToken,
      ContainerLauncher.EventType type,
      boolean dumpContainerThreads) {
    super(type);
    this.taskAttemptID = taskAttemptID;
    this.containerID = containerID;
    this.containerMgrAddress = containerMgrAddress;
    this.containerToken = containerToken;
    this.dumpContainerThreads = dumpContainerThreads;
  }

  /**
   * 获取事件关联的任务尝试ID
   * @return 任务尝试ID
   */
  public TaskAttemptId getTaskAttemptID() {
    return this.taskAttemptID;
  }

  /**
   * 获取事件关联的YARN容器ID
   * @return YARN容器ID
   */
  public ContainerId getContainerID() {
    return containerID;
  }

  /**
   * 获取NodeManager容器管理服务地址
   * @return NodeManager服务地址
   */
  public String getContainerMgrAddress() {
    return containerMgrAddress;
  }

  /**
   * 获取容器访问令牌
   * @return 容器访问令牌
   */
  public Token getContainerToken() {
    return containerToken;
  }

  /**
   * 获取是否需要转储容器线程堆栈标志
   * @return true表示需要转储用于故障诊断，false不需要
   */
  public boolean getDumpContainerThreads() {
    return dumpContainerThreads;
  }

  @Override
  public String toString() {
    return super.toString() + " for container " + containerID + " taskAttempt "
        + taskAttemptID;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result
        + ((containerID == null) ? 0 : containerID.hashCode());
    result = prime * result
        + ((containerMgrAddress == null) ? 0 : containerMgrAddress.hashCode());
    result = prime * result
        + ((containerToken == null) ? 0 : containerToken.hashCode());
    result = prime * result
        + ((taskAttemptID == null) ? 0 : taskAttemptID.hashCode());
    result = prime * result
        + (dumpContainerThreads ? 1 : 0);
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    ContainerLauncherEvent other = (ContainerLauncherEvent) obj;
    if (containerID == null) {
      if (other.containerID != null)
        return false;
    } else if (!containerID.equals(other.containerID))
      return false;
    if (containerMgrAddress == null) {
      if (other.containerMgrAddress != null)
        return false;
    } else if (!containerMgrAddress.equals(other.containerMgrAddress))
      return false;
    if (containerToken == null) {
      if (other.containerToken != null)
        return false;
    } else if (!containerToken.equals(other.containerToken))
      return false;
    if (taskAttemptID == null) {
      if (other.taskAttemptID != null)
        return false;
    } else if (!taskAttemptID.equals(other.taskAttemptID))
      return false;

    return dumpContainerThreads == other.dumpContainerThreads;
  }

}