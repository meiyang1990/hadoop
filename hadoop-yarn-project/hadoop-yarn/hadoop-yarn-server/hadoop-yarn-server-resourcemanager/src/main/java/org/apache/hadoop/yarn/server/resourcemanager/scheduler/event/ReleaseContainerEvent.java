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

import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;

/**
 * 容器释放事件，用于通知调度器释放指定容器的资源
 * 是YARN调度器事件体系中处理容器资源回收的核心事件类
 */
public class ReleaseContainerEvent extends SchedulerEvent {

  // 需要被释放的容器对象
  private final RMContainer container;

  /**
   * 构造释放容器事件
   * @param rmContainer 待释放的RMContainer对象
   */
  public ReleaseContainerEvent(RMContainer rmContainer) {
    super(SchedulerEventType.RELEASE_CONTAINER);
    this.container = rmContainer;
  }

  /**
   * 获取待释放的RMContainer对象
   * @return 待释放的RMContainer
   */
  public RMContainer getContainer() {
    return container;
  }
}