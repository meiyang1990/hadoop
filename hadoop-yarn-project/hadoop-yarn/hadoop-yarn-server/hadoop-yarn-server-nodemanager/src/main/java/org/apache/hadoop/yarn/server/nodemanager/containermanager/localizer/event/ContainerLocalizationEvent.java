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
package org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event;

import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;

/**
 * 容器本地化事件，封装容器资源本地化操作相关的事件信息
 */
public class ContainerLocalizationEvent extends LocalizationEvent {

  // 关联的目标容器实例
  final Container container;

  /**
   * 构造容器本地化事件
   * @param event 本地化事件类型
   * @param c 关联的目标容器
   */
  public ContainerLocalizationEvent(LocalizationEventType event, Container c) {
    super(event);
    this.container = c;
  }

  /**
   * 获取事件关联的容器实例
   * @return 关联的容器对象
   */
  public Container getContainer() {
    return container;
  }

}