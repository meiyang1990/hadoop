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
 * 容器暂停事件，对应容器事件类型PAUSE_CONTAINER，用于通知容器管理器暂停指定容器
 */
public class ContainerPauseEvent extends ContainerEvent {

  // 暂停原因诊断信息
  private final String diagnostic;

  /**
   * 构造容器暂停事件
   * @param cId 目标容器ID
   * @param diagnostic 暂停原因诊断信息
   */
  public ContainerPauseEvent(ContainerId cId,
      String diagnostic) {
    super(cId, ContainerEventType.PAUSE_CONTAINER);
    this.diagnostic = diagnostic;
  }

  /**
   * 获取暂停原因诊断信息
   * @return 暂停诊断信息
   */
  public String getDiagnostic() {
    return this.diagnostic;
  }
}