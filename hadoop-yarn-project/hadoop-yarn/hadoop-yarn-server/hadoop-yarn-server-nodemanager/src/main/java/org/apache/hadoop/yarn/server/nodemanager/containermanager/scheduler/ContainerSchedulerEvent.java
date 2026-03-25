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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.scheduler;

import org.apache.hadoop.yarn.event.AbstractEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container
    .Container;

/**
 * 容器调度器事件基类，所有容器调度相关事件都继承此类，供{@link ContainerScheduler}消费处理
 */
public class ContainerSchedulerEvent extends
    AbstractEvent<ContainerSchedulerEventType> {

  // 该事件关联的目标容器
  private final Container container;

  /**
   * 构造容器调度事件实例
   * @param container 关联的容器
   * @param eventType 事件类型
   */
  public ContainerSchedulerEvent(Container container,
      ContainerSchedulerEventType eventType) {
    super(eventType);
    this.container = container;
  }

  /**
   * 获取该事件关联的容器
   * @return 关联的容器实例
   */
  public Container getContainer() {
    return container;
  }
}