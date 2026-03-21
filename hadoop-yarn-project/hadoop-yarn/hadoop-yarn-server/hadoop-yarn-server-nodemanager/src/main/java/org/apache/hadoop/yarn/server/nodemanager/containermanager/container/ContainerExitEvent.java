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
 * 容器退出事件，封装容器退出时的相关信息
 * 用于容器状态机处理容器退出流程，携带退出码和诊断信息
 */
public class ContainerExitEvent extends ContainerEvent {
  private int exitCode;
  private final String diagnosticInfo;

  /**
   * 构造容器退出事件
   * @param cID 容器ID
   * @param eventType 容器事件类型
   * @param exitCode 容器退出码
   * @param diagnosticInfo 容器退出诊断信息
   */
  public ContainerExitEvent(ContainerId cID, ContainerEventType eventType,
      int exitCode, String diagnosticInfo) {
    super(cID, eventType);
    this.exitCode = exitCode;
    this.diagnosticInfo = diagnosticInfo;
  }

  /**
   * 获取容器退出码
   * @return 容器退出码
   */
  public int getExitCode() {
    return this.exitCode;
  }

  /**
   * 获取容器退出诊断信息
   * @return 诊断信息字符串
   */
  public String getDiagnosticInfo() {
    return diagnosticInfo;
  }
}