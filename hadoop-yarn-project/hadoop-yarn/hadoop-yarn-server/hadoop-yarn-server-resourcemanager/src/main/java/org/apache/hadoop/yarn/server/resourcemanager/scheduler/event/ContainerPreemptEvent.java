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

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;

/**
 * 容器抢占事件，用于资源调度器中传递抢占相关指令，支持杀死预留容器、标记容器待抢占、杀死已标记抢占容器三种场景。
 */
public class ContainerPreemptEvent extends SchedulerEvent {

  private final ApplicationAttemptId aid;
  private final RMContainer container;

  /**
   * 构造容器抢占事件。
   * @param aid 应用尝试ID
   * @param container 待处理的RM容器对象
   * @param type 调度事件类型
   */
  public ContainerPreemptEvent(ApplicationAttemptId aid, RMContainer container,
      SchedulerEventType type) {
    super(type);
    this.aid = aid;
    this.container = container;
  }

  /**
   * 获取待抢占处理的RM容器对象。
   * @return 待处理RM容器
   */
  public RMContainer getContainer(){
    return this.container;
  }

  /**
   * 获取容器所属的应用尝试ID。
   * @return 应用尝试ID
   */
  public ApplicationAttemptId getAppId() {
    return aid;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder(super.toString());
    sb.append(" ").append(getAppId())
        .append(" ").append(getContainer().getContainerId());
    return sb.toString();
  }

}