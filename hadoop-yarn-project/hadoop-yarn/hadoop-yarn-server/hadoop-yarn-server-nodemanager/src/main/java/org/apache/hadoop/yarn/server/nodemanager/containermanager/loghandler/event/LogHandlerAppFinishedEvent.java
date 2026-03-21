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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.loghandler.event;

import org.apache.hadoop.yarn.api.records.ApplicationId;

/**
 * 应用程序结束日志处理事件，通知日志处理器完成对应应用的日志收尾工作。
 */
public class LogHandlerAppFinishedEvent extends LogHandlerEvent {

  private final ApplicationId applicationId;

  /**
   * 构造应用结束日志处理事件。
   * @param appId 已结束的应用程序ID
   */
  public LogHandlerAppFinishedEvent(ApplicationId appId) {
    super(LogHandlerEventType.APPLICATION_FINISHED);
    this.applicationId = appId;
  }

  /**
   * 获取已结束应用的ID。
   * @return 应用程序ID
   */
  public ApplicationId getApplicationId() {
    return this.applicationId;
  }

}