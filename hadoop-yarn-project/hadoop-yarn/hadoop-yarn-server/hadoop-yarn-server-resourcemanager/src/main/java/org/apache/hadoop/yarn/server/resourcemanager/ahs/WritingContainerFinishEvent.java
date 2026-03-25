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
import org.apache.hadoop.yarn.server.applicationhistoryservice.records.ContainerFinishData;

/**
 * 容器结束事件写入应用历史事件，用于资源管理器向应用历史服务写入容器结束信息。
 */
public class WritingContainerFinishEvent extends WritingApplicationHistoryEvent {

  private ContainerId containerId;
  private ContainerFinishData containerFinish;

  /**
   * 构造容器结束写入事件。
   * @param containerId 容器ID
   * @param containerFinish 容器结束数据
   */
  public WritingContainerFinishEvent(ContainerId containerId,
      ContainerFinishData containerFinish) {
    super(WritingHistoryEventType.CONTAINER_FINISH);
    this.containerId = containerId;
    this.containerFinish = containerFinish;
  }

  @Override
  public int hashCode() {
    return containerId.getApplicationAttemptId().getApplicationId().hashCode();
  }

  /**
   * 获取当前事件对应的容器ID。
   * @return 容器ID
   */
  public ContainerId getContainerId() {
    return containerId;
  }

  /**
   * 获取容器结束数据。
   * @return 容器结束数据
   */
  public ContainerFinishData getContainerFinishData() {
    return containerFinish;
  }

}