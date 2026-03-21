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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.event;

import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.ContainerAllocationExpirer;

/**
 * 容器过期调度事件，由{@link ContainerAllocationExpirer}发送，通知调度器指定容器已过期
 *
 */
public class ContainerExpiredSchedulerEvent extends SchedulerEvent {

  /** 过期容器ID */
  private final ContainerId containerId;
  /** 是否为递增容器分配过期 */
  private final boolean increase;

  /**
   * 构造非递增容器分配过期事件
   * @param containerId 过期容器ID
   */
  public ContainerExpiredSchedulerEvent(ContainerId containerId) {
    this(containerId, false);
  }

  /**
   * 构造容器过期事件
   * @param containerId 过期容器ID
   * @param increase 是否为递增容器分配过期
   */
  public ContainerExpiredSchedulerEvent(
      ContainerId containerId, boolean increase) {
    super(SchedulerEventType.CONTAINER_EXPIRED);
    this.containerId = containerId;
    this.increase = increase;
  }

  /**
   * 获取过期容器ID
   * @return 过期容器ID
   */
  public ContainerId getContainerId() {
    return containerId;
  }

  /**
   * 获取是否为递增容器分配过期
   * @return true表示是递增容器分配过期，false表示普通容器分配过期
   */
  public boolean isIncrease() {
    return increase;
  }
}