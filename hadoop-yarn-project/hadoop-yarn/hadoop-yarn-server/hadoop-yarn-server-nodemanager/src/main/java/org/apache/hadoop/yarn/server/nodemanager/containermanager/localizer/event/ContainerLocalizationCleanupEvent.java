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

import java.util.Collection;
import java.util.Map;

import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.LocalResourceRequest;

/**
 * 容器本地化资源清理事件，用于通知本地化组件清理容器不再需要的本地化资源。
 * 当容器完成后触发，按资源可见性分组存储需要清理的资源请求。
 */
public class ContainerLocalizationCleanupEvent extends
    ContainerLocalizationEvent {

  // 按可见性分组存储需要清理的本地资源请求
  private final Map<LocalResourceVisibility, Collection<LocalResourceRequest>> 
    rsrc;

  /**
   * 构造容器资源清理事件。
   * @param c 目标容器
   * @param rsrc 按可见性分组的待清理资源请求集合
   */
  public ContainerLocalizationCleanupEvent(Container c,
      Map<LocalResourceVisibility, Collection<LocalResourceRequest>> rsrc) {
    super(LocalizationEventType.CLEANUP_CONTAINER_RESOURCES, c);
    this.rsrc = rsrc;
  }

  /**
   * 获取按可见性分组的待清理资源请求集合。
   * @return 分组后的待清理资源请求
   */
  public
      Map<LocalResourceVisibility, Collection<LocalResourceRequest>>
      getResources() {
    return rsrc;
  }
}