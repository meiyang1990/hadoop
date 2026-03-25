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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.loghandler;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.loghandler.event.LogHandlerEvent;


import java.util.Set;

/**
 * YARN NodeManager 容器日志处理器接口，定义日志事件处理的核心规范。
 * 负责处理容器日志的收集、聚合、滚动等生命周期管理。
 */
public interface LogHandler extends EventHandler<LogHandlerEvent> {
  @Override
  public void handle(LogHandlerEvent event);

  /**
   * 获取所有携带无效令牌的应用集合，用于日志处理权限校验。
   * @return 包含无效令牌的应用ID集合
   */
  public Set<ApplicationId> getInvalidTokenApps();
}