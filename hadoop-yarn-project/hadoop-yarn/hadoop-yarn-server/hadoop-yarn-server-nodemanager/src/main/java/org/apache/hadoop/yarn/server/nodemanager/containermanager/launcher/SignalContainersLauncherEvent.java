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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.launcher;

import org.apache.hadoop.yarn.api.records.SignalContainerCommand;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;

/**
 * 向容器发送信号的事件，由容器启动器处理
 * 可通过两种流程触发：
 * 1. WebUI -> 容器
 * 2. CLI -> ResourceManager -> NodeManager
 */
public class SignalContainersLauncherEvent extends ContainersLauncherEvent{

  // 待执行的容器信号命令
  private final SignalContainerCommand command;

  /**
   * 构造信号容器启动事件
   * @param container 目标容器
   * @param command 信号命令
   */
  public SignalContainersLauncherEvent(Container container,
      SignalContainerCommand command) {
    super(container, ContainersLauncherEventType.SIGNAL_CONTAINER);
    this.command = command;
  }

  /**
   * 获取信号命令
   * @return 信号容器命令
   */
  public SignalContainerCommand getCommand() {
    return command;
  }
}