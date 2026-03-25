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

import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.LocalizedResource;

/**
 * 资源本地化事件类型枚举，定义了所有发往{@link LocalizedResource}的事件类型
 * 所有具体事件都是{@link ResourceEvent}的子类
 */
public enum ResourceEventType {
  /** 资源本地化请求事件，对应{@link ResourceRequestEvent} */
  REQUEST,
  /** 资源本地化完成事件，对应{@link ResourceLocalizedEvent} */ 
  LOCALIZED,
  /** 资源释放事件，对应{@link ResourceReleaseEvent} */
  RELEASE,
  /** 资源本地化失败事件，对应{@link ResourceFailedLocalizationEvent} */
  LOCALIZATION_FAILED,
  /** 资源恢复完成事件，对应{@link ResourceRecoveredEvent} */
  RECOVERED
}