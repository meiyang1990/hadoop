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

package org.apache.hadoop.yarn.server.resourcemanager.ahs;

import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.server.applicationhistoryservice.records.ContainerStartData;

/**
 * 容器启动事件写入应用历史服务的事件类，用于RM将容器启动事件转储到应用历史服务。
 */
public class WritingContainerStartEvent extends WritingApplicationHistoryEvent {

  private ContainerId containerId;
  private ContainerStartData containerStart;

  /**
   * 构造容器启动写入事件。
   * @param containerId 容器ID
   * @param containerStart 容器启动数据
   */
  public WritingContainerStartEvent(ContainerId containerId,
      ContainerStartData containerStart) {
    super(WritingHistoryEventType.CONTAINER_START);
    this.containerId = containerId;
    this.containerStart = containerStart;
  }

  @Override
  public int hashCode() {
    // 基于所属应用ID计算哈希值
    return containerId.getApplicationAttemptId().getApplicationId().hashCode();
  }

  /**
   * 获取事件对应的容器ID。
   * @return 容器ID
   */
  public ContainerId getContainerId() {
    return containerId;
  }

  /**
   * 获取容器启动数据。
   * @return 容器启动数据
   */
  public ContainerStartData getContainerStartData() {
    return containerStart;
  }

}