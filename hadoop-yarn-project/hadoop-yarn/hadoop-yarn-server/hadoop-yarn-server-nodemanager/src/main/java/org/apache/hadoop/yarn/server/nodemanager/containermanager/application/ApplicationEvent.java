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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.application;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.event.AbstractEvent;

/**
 * 节点管理器应用管理模块基础事件类，封装应用相关事件的通用属性
 * 所有应用状态相关事件都继承此类，用于驱动应用状态机流转
 */
public class ApplicationEvent extends AbstractEvent<ApplicationEventType> {

  // 关联应用的全局唯一ID
  private final ApplicationId applicationID;

  /**
   * 构造应用事件实例
   * @param appID 事件所属应用ID
   * @param appEventType 事件类型
   */
  public ApplicationEvent(ApplicationId appID,
      ApplicationEventType appEventType) {
    super(appEventType, System.currentTimeMillis());
    this.applicationID = appID;
  }

  /**
   * 获取本事件所属应用的ID
   * @return 应用全局唯一ID
   */
  public ApplicationId getApplicationID() {
    return this.applicationID;
  }

}