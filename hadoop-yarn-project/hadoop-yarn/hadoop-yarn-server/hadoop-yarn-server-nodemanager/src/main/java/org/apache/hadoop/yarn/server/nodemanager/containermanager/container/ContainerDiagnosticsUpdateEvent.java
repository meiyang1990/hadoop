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
 * 容器诊断信息更新事件，用于向容器状态机传递诊断信息更新请求
 * 当容器运行过程中产生新的诊断日志/错误信息时，发布此事件更新容器诊断信息
 */
public class ContainerDiagnosticsUpdateEvent extends ContainerEvent {

  private final String diagnosticsUpdate;

  /**
   * 构造容器诊断信息更新事件
   * @param cID 目标容器ID
   * @param update 新增的诊断信息内容
   */
  public ContainerDiagnosticsUpdateEvent(ContainerId cID, String update) {
    super(cID, ContainerEventType.UPDATE_DIAGNOSTICS_MSG);
    this.diagnosticsUpdate = update;
  }

  /**
   * 获取本次更新的诊断信息内容
   * @return 新增诊断信息字符串
   */
  public String getDiagnosticsUpdate() {
    return this.diagnosticsUpdate;
  }
}