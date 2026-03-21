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
package org.apache.hadoop.yarn.server.nodemanager.containermanager.container;

import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.LocalResourceRequest;

/**
 * 容器资源相关事件，用于通知容器资源请求/释放等资源操作
 */
public class ContainerResourceEvent extends ContainerEvent {

  // 关联的本地资源请求
  private final LocalResourceRequest rsrc;

  /**
   * 构造容器资源事件
   * @param container 容器ID
   * @param type 容器事件类型
   * @param rsrc 本地资源请求
   */
  public ContainerResourceEvent(ContainerId container,
      ContainerEventType type, LocalResourceRequest rsrc) {
    super(container, type);
    this.rsrc = rsrc;
  }

  /**
   * 获取事件关联的资源请求
   * @return 本地资源请求
   */
  public LocalResourceRequest getResource() {
    return rsrc;
  }

}