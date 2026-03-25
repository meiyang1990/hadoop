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
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.LocalizedResource;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.LocalizerContext;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.ResourceLocalizationService;

/**
 * 资源本地化请求事件，通知 {@link ResourceLocalizationService} 下载本地化指定资源
 */
public class LocalizerResourceRequestEvent extends LocalizerEvent {

  private final LocalizerContext context;
  private final LocalizedResource resource;
  private final LocalResourceVisibility vis;
  private final String pattern;

  /**
   * 构造资源本地化请求事件
   * @param resource 待本地化的资源对象
   * @param vis 资源可见性
   * @param context 本地化器上下文，包含容器相关信息
   * @param pattern 资源解压模式（若为归档资源）
   */
  public LocalizerResourceRequestEvent(LocalizedResource resource,
      LocalResourceVisibility vis, LocalizerContext context, String pattern) {
    super(LocalizerEventType.REQUEST_RESOURCE_LOCALIZATION,
        context.getContainerId().toString());
    this.vis = vis;
    this.context = context;
    this.resource = resource;
    this.pattern = pattern;
  }

  public LocalizedResource getResource() {
    return resource;
  }

  public LocalizerContext getContext() {
    return context;
  }

  public LocalResourceVisibility getVisibility() {
    return vis;
  }

  public String getPattern() {
    return pattern;
  }

}