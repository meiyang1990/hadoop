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

/**
 * 应用初始化完成事件，由NodeManager容器管理器分发，
 * 用于触发应用初始化完成后的后续处理流程。
 */
public class ApplicationInitedEvent extends ApplicationEvent {

  /**
   * 创建应用初始化完成事件实例。
   * @param appID 已完成初始化的应用ID
   */
  public ApplicationInitedEvent(ApplicationId appID) {
    super(appID, ApplicationEventType.APPLICATION_INITED);
  }

}