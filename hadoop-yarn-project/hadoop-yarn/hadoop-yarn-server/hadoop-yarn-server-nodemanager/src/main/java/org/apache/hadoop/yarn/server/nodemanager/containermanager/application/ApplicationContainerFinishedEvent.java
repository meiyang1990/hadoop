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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.application;

import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;

/**
 * 应用容器完成事件，用于通知应用程序所属容器已执行完毕。
 * 携带容器结束状态和启动时间等信息，供应用状态机处理后续逻辑。
 */
public class ApplicationContainerFinishedEvent extends ApplicationEvent {
  private ContainerStatus containerStatus;
  // 时间线发布器需要该字段，保留容器启动时间
  private long containerStartTime;

  /**
   * 构造容器完成事件实例。
   * @param containerStatus 容器完成后的状态信息
   * @param containerStartTs 容器启动时间戳
   */
  public ApplicationContainerFinishedEvent(ContainerStatus containerStatus,
      long containerStartTs) {
    super(containerStatus.getContainerId().getApplicationAttemptId().
        getApplicationId(),
        ApplicationEventType.APPLICATION_CONTAINER_FINISHED);
    this.containerStatus = containerStatus;
    this.containerStartTime = containerStartTs;
  }

  /**
   * 获取已完成容器的ID。
   * @return 容器ID
   */
  public ContainerId getContainerID() {
    return containerStatus.getContainerId();
  }

  /**
   * 获取已完成容器的最终状态。
   * @return 容器状态对象
   */
  public ContainerStatus getContainerStatus() {
    return containerStatus;
  }

  /**
   * 获取容器启动时间戳。
   * @return 容器启动时间戳，单位毫秒
   */
  public long getContainerStartTime() {
    return containerStartTime;
  }
}