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
package org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event;

import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.LocalResourceRequest;

/**
 * 资源本地化失败事件，当本地化器处理请求资源时发生错误，会发送该事件通知上游模块。
 */
public class ResourceFailedLocalizationEvent extends ResourceEvent {

  // 本地化失败诊断信息
  private final String diagnosticMesage;

  /**
   * 构造资源本地化失败事件
   * @param rsrc 本地化失败的资源请求
   * @param diagnosticMesage 失败诊断信息
   */
  public ResourceFailedLocalizationEvent(LocalResourceRequest rsrc,
      String diagnosticMesage) {
    super(rsrc, ResourceEventType.LOCALIZATION_FAILED);
    this.diagnosticMesage = diagnosticMesage;
  }

  /**
   * 获取本地化失败的诊断信息
   * @return 失败诊断信息字符串
   */
  public String getDiagnosticMessage() {
    return diagnosticMesage;
  }
}