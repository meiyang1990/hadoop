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

package org.apache.hadoop.yarn.server.resourcemanager.rmcontainer;

import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;

/**
 * RM容器完成事件，封装容器执行结束后的相关状态信息，用于RM容器状态机流转。
 */
public class RMContainerFinishedEvent extends RMContainerEvent {

  // NodeManager上报的容器远程状态信息
  private final ContainerStatus remoteContainerStatus;

  /**
   * 构造容器完成事件。
   * @param containerId 容器ID
   * @param containerStatus NodeManager上报的容器状态
   * @param event 事件类型
   */
  public RMContainerFinishedEvent(ContainerId containerId,
      ContainerStatus containerStatus, RMContainerEventType event) {
    super(containerId, event);
    this.remoteContainerStatus = containerStatus;
  }

  /**
   * 获取NodeManager上报的容器最终状态。
   * @return 容器远程状态信息
   */
  public ContainerStatus getRemoteContainerStatus() {
    return this.remoteContainerStatus;
  }
}