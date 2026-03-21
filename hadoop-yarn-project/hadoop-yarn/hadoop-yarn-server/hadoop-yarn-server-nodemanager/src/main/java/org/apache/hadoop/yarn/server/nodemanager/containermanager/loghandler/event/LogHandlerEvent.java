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

import org.apache.hadoop.yarn.event.AbstractEvent;

/**
 * 日志处理器事件基类，封装日志处理相关事件的通用结构
 * 用于NodeManager容器日志处理模块的事件驱动模型
 */
public class LogHandlerEvent extends AbstractEvent<LogHandlerEventType>{

  /**
   * 构造指定类型的日志处理器事件
   * @param type 日志处理器事件类型
   */
  public LogHandlerEvent(LogHandlerEventType type) {
    super(type);
  }

}