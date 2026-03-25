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
 * YARN NodeManager 容器恢复事件，对应容器事件类型 RESUME_CONTAINER，
 * 用于通知容器管理器恢复之前被暂停的容器运行。
 */
public class ContainerResumeEvent extends ContainerEvent {

  // 恢复操作的诊断信息，用于记录恢复原因或异常描述
  private final String diagnostic;

  /**
   * 构造容器恢复事件
   * @param cId 要恢复的容器ID
   * @param diagnostic 恢复操作的诊断信息
   */
  public ContainerResumeEvent(ContainerId cId,
      String diagnostic) {
    super(cId, ContainerEventType.RESUME_CONTAINER);
    this.diagnostic = diagnostic;
  }

  /**
   * 获取恢复操作的诊断信息
   * @return 诊断信息字符串
   */
  public String getDiagnostic() {
    return this.diagnostic;
  }
}