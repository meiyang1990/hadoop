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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.container;

import org.apache.hadoop.yarn.api.records.ContainerId;

/**
 * 容器销毁事件，用于向容器状态机传递杀死容器的请求，携带退出信息和诊断信息
 */
public class ContainerKillEvent extends ContainerEvent {

  // 诊断信息，描述容器被杀死的原因
  private final String diagnostic;
  // 容器退出状态码
  private final int exitStatus;

  /**
   * 构造容器杀死事件
   * @param cID 目标容器ID
   * @param exitStatus 容器退出状态码
   * @param diagnostic 杀死原因诊断信息
   */
  public ContainerKillEvent(ContainerId cID,
      int exitStatus, String diagnostic) {
    super(cID, ContainerEventType.KILL_CONTAINER);
    this.exitStatus = exitStatus;
    this.diagnostic = diagnostic;
  }

  /**
   * 获取容器杀死的诊断信息
   * @return 诊断信息字符串
   */
  public String getDiagnostic() {
    return this.diagnostic;
  }

  /**
   * 获取容器退出状态码
   * @return 退出状态码
   */
  public int getContainerExitStatus() {
    return this.exitStatus;
  }

}