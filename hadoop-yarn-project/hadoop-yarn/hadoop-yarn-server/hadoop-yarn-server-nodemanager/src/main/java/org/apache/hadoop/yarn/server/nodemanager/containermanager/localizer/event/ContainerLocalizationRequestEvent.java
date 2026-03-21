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
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerImpl;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.LocalResourceRequest;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.ResourceLocalizationService;

/**
 * 容器资源本地化请求事件，通知资源本地化服务为指定容器下载本地化所需资源
 * 该事件由容器初始化过程中的{@link ContainerImpl}生成，触发资源本地化流程
 */
public class ContainerLocalizationRequestEvent extends
    ContainerLocalizationEvent {

  // 按可见性分类的待本地化资源请求集合
  private final Map<LocalResourceVisibility, Collection<LocalResourceRequest>> 
    rsrc;

  /**
   * 构造容器资源本地化请求事件
   * @param c 目标容器
   * @param rsrc 按可见性分组的本地化资源请求集合
   */
  public ContainerLocalizationRequestEvent(Container c,
      Map<LocalResourceVisibility, Collection<LocalResourceRequest>> rsrc) {
    super(LocalizationEventType.LOCALIZE_CONTAINER_RESOURCES, c);
    this.rsrc = rsrc;
  }

  /**
   * 获取所有请求本地化的资源（按可见性分组）
   * @return 按可见性分组的本地化资源请求集合
   */
  public
      Map<LocalResourceVisibility, Collection<LocalResourceRequest>>
      getRequestedResources() {
    return rsrc;
  }
}