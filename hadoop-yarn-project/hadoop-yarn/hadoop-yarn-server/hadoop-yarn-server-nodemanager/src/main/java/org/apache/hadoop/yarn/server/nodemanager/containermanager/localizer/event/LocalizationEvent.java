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
 * 文件说明：资源本地化事件基类，所有资源本地化相关事件的父类
 * 供 {@link ResourceLocalizationService} 资源本地化服务处理各类本地化事件
 */
public class LocalizationEvent extends AbstractEvent<LocalizationEventType> {

  /**
   * 构造本地化事件，自动记录当前时间戳
   * @param event 本地化事件类型
   */
  public LocalizationEvent(LocalizationEventType event) {
    super(event, System.currentTimeMillis());
  }

}