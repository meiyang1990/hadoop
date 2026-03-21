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

import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.LocalResourceRequest;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.LocalizerContext;

/**
 * 资源本地化请求事件，封装容器资源本地化请求信息
 * 用于通知本地化服务发起指定资源的下载本地化流程
 */
public class ResourceRequestEvent extends ResourceEvent {

  // 本地化上下文，包含应用等上下文信息
  private final LocalizerContext context;
  // 资源可见性（PUBLIC/PRIVATE/APPLICATION）
  private final LocalResourceVisibility vis;

  /**
   * 构造资源请求事件
   * @param resource 待本地化的资源请求
   * @param vis 资源可见性
   * @param context 本地化上下文
   */
  public ResourceRequestEvent(LocalResourceRequest resource,
      LocalResourceVisibility vis, LocalizerContext context) {
    super(resource, ResourceEventType.REQUEST);
    this.vis = vis;
    this.context = context;
  }

  /**
   * 获取本地化上下文
   * @return 本地化上下文
   */
  public LocalizerContext getContext() {
    return context;
  }

  /**
   * 获取资源可见性
   * @return 资源可见性枚举
   */
  public LocalResourceVisibility getVisibility() {
    return vis;
  }

}