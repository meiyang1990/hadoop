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

import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.Application;

/**
 * 应用本地化事件，封装应用级别的本地化操作相关事件
 * 用于NodeManager本地资源本地化流程中传递应用上下文信息
 */
public class ApplicationLocalizationEvent extends LocalizationEvent {

  // 关联的应用实例
  final Application app;

  /**
   * 构造应用本地化事件
   * @param type 本地化事件类型
   * @param app 关联的应用实例
   */
  public ApplicationLocalizationEvent(LocalizationEventType type, Application app) {
    super(type);
    this.app = app;
  }

  /**
   * 获取事件关联的应用实例
   * @return 关联的应用实例
   */
  public Application getApplication() {
    return app;
  }

}