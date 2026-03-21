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

import org.apache.hadoop.yarn.server.nodemanager.containermanager.ContainerManagerImpl;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerInitEvent;

/**
 * 容器初始化事件，由ContainerManagerImpl发送给ApplicationImpl
 * 用于请求初始化应用所属容器，通过应用层中转可以检查应用生命周期，
 * 容器启动会被延迟到应用完成初始化之后。
 * 应用初始化完成后，InitContainerTransition会将本事件转换为ContainerInitEvent转发给容器。
 */
public class ApplicationContainerInitEvent extends ApplicationEvent {
  // 待初始化的容器实例
  final Container container;
  
  /**
   * 构造应用容器初始化事件
   * @param container 待初始化的容器
   */
  public ApplicationContainerInitEvent(Container container) {
    super(container.getContainerId().getApplicationAttemptId()
        .getApplicationId(), ApplicationEventType.INIT_CONTAINER);
    this.container = container;
  }

  /**
   * 获取待初始化的容器实例
   * @return 待初始化容器
   */
  Container getContainer() {
    return container;
  }
}