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
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.LocalResourceRequest;

/**
 * 容器资源本地化失败事件，用于通知容器管理器资源下载或本地化失败
 */
public class ContainerResourceFailedEvent extends ContainerResourceEvent {

  // 资源失败诊断信息，用于记录失败原因
  private final String diagnosticMesage;

  /**
   * 构造容器资源失败事件
   * @param container 目标容器ID
   * @param rsrc 失败的资源请求
   * @param diagnosticMesage 失败诊断信息
   */
  public ContainerResourceFailedEvent(ContainerId container,
      LocalResourceRequest rsrc, String diagnosticMesage) {
    super(container, ContainerEventType.RESOURCE_FAILED, rsrc);
    this.diagnosticMesage = diagnosticMesage;
  }

  /**
   * 获取资源失败的诊断信息
   * @return 失败诊断信息
   */
  public String getDiagnosticMessage() {
    return diagnosticMesage;
  }
}