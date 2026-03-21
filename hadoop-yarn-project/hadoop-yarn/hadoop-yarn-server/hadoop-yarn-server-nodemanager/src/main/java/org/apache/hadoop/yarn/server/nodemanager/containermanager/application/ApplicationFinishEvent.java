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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.application;

import org.apache.hadoop.yarn.api.records.ApplicationId;

/**
 * 应用完成/终止事件，用于通知NodeManager上的应用实例需要结束清理
 */
public class ApplicationFinishEvent extends ApplicationEvent {
  // 应用终止的诊断信息，用于日志和问题排查
  private final String diagnostic;

  /**
   * 构造应用终止事件，用于触发应用所有容器的中止清理
   * @param appId 要终止的应用ID
   * @param diagnostic 应用终止的原因信息
   */
  public ApplicationFinishEvent(ApplicationId appId, String diagnostic) {
    super(appId, ApplicationEventType.FINISH_APPLICATION);
    this.diagnostic = diagnostic;
  }

  /**
   * 获取应用终止的诊断原因信息
   * @return 诊断信息字符串
   */
  public String getDiagnostic() {
    return diagnostic;
  }
}