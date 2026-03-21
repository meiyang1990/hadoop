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

package org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt;

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.event.AbstractEvent;

/**
 * YARN ResourceManager 中应用尝试 attempt 相关事件的基类，
 * 封装应用尝试 attempt 事件的通用信息，用于 RM 状态机驱动事件处理。
 */
public class RMAppAttemptEvent extends AbstractEvent<RMAppAttemptEventType> {

  // 关联的应用尝试 attempt ID
  private final ApplicationAttemptId appAttemptId;
  // 诊断信息，用于异常场景说明
  private final String diagnosticMsg;

  /**
   * 构造函数，不携带诊断信息。
   * @param appAttemptId 关联的应用尝试 attempt ID
   * @param type 事件类型
   */
  public RMAppAttemptEvent(ApplicationAttemptId appAttemptId,
      RMAppAttemptEventType type) {
    this(appAttemptId, type, "");
  }

  /**
   * 构造函数，携带诊断信息。
   * @param appAttemptId 关联的应用尝试 attempt ID
   * @param type 事件类型
   * @param diagnostics 诊断信息
   */
  public RMAppAttemptEvent(ApplicationAttemptId appAttemptId,
      RMAppAttemptEventType type, String diagnostics) {
    super(type);
    this.appAttemptId = appAttemptId;
    this.diagnosticMsg = diagnostics;
  }

  /**
   * 构造函数，指定时间戳，不携带诊断信息。
   * @param appAttemptId 关联的应用尝试 attempt ID
   * @param type 事件类型
   * @param timeStamp 事件时间戳
   */
  public RMAppAttemptEvent(ApplicationAttemptId appAttemptId,
                           RMAppAttemptEventType type, long timeStamp) {
    super(type, timeStamp);
    this.appAttemptId = appAttemptId;
    this.diagnosticMsg = "";
  }

  /**
   * 获取当前事件关联的应用尝试 attempt ID。
   * @return 应用尝试 attempt ID
   */
  public ApplicationAttemptId getApplicationAttemptId() {
    return this.appAttemptId;
  }

  /**
   * 获取事件关联的诊断信息。
   * @return 诊断信息字符串
   */
  public String getDiagnosticMsg() {
    return diagnosticMsg;
  }
}