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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.loghandler.event;

import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.server.api.ContainerType;

/**
 * 容器完成事件，通知日志处理器对已结束容器的日志进行后续处理（聚合、归档等）
 */
public class LogHandlerContainerFinishedEvent extends LogHandlerEvent {

  private final ContainerId containerId;
  private final ContainerType containerType;
  private final int exitCode;

  /**
   * 构造容器完成日志处理事件
   * @param containerId 已完成容器ID
   * @param containerType 容器类型
   * @param exitCode 容器退出码
   */
  public LogHandlerContainerFinishedEvent(ContainerId containerId,
      ContainerType containerType, int exitCode) {
    super(LogHandlerEventType.CONTAINER_FINISHED);
    this.containerId = containerId;
    this.containerType = containerType;
    this.exitCode = exitCode;
  }

  public ContainerId getContainerId() {
    return this.containerId;
  }

  public ContainerType getContainerType() {
    return containerType;
  }

  public int getExitCode() {
    return this.exitCode;
  }

}