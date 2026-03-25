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

import org.apache.hadoop.yarn.event.AbstractEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.ResourceLocalizationService;

/**
 * 文件级注释：本地化器事件基类，用于NodeManager资源本地化服务的事件驱动模型中，
 * 封装所有发往ResourceLocalizationService的事件通用属性。
 * 
 * Events delivered to the {@link ResourceLocalizationService}
 */
public class LocalizerEvent extends AbstractEvent<LocalizerEventType> {

  // 对应本地化器的唯一标识
  private final String localizerId;

  /**
   * 构造本地化器事件实例
   * @param type 事件类型
   * @param localizerId 目标本地化器ID
   */
  public LocalizerEvent(LocalizerEventType type, String localizerId) {
    super(type);
    this.localizerId = localizerId;
  }

  /**
   * 获取事件目标本地化器ID
   * @return 本地化器唯一标识
   */
  public String getLocalizerId() {
    return localizerId;
  }

}