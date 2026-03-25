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

import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.LocalResourceRequest;

/**
 * 资源释放事件，通知本地资源本地化模块释放指定容器使用的本地化资源
 */
public class ResourceReleaseEvent extends ResourceEvent {

  // 需要释放资源所属的容器ID
  private final ContainerId container;

  /**
   * 构造资源释放事件
   * @param rsrc 需要释放的本地资源请求
   * @param container 请求该资源的容器ID
   */
  public ResourceReleaseEvent(LocalResourceRequest rsrc, 
      ContainerId container) {
    super(rsrc, ResourceEventType.RELEASE);
    this.container = container;
  }

  /**
   * 获取需要释放资源所属的容器ID
   * @return 容器ID
   */
  public ContainerId getContainer() {
    return container;
  }

}