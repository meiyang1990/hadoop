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

package org.apache.hadoop.yarn.server.resourcemanager.rmapp;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.event.AbstractEvent;

/**
 * ResourceManager中应用程序状态变更事件基类，
 * 封装应用程序相关事件的通用信息，用于RM内部状态机驱动应用生命周期流转
 */
public class RMAppEvent extends AbstractEvent<RMAppEventType>{

  // 关联的应用程序ID
  private final ApplicationId appId;
  // 诊断信息，用于描述事件原因/错误信息
  private final String diagnosticMsg;

  /**
   * 构造RMAppEvent，指定应用ID和事件类型，诊断信息为空
   * @param appId 关联应用ID
   * @param type 事件类型
   */
  public RMAppEvent(ApplicationId appId, RMAppEventType type) {
    this(appId, type, "");
  }

  /**
   * 构造RMAppEvent，指定应用ID、事件类型和诊断信息
   * @param appId 关联应用ID
   * @param type 事件类型
   * @param diagnostic 诊断信息
   */
  public RMAppEvent(ApplicationId appId, RMAppEventType type,
      String diagnostic) {
    super(type);
    this.appId = appId;
    this.diagnosticMsg = diagnostic;
  }

  /**
   * 构造RMAppEvent，指定应用ID、事件类型和事件时间戳
   * @param appId 关联应用ID
   * @param type 事件类型
   * @param timeStamp 事件发生时间戳
   */
  public RMAppEvent(ApplicationId appId, RMAppEventType type, long timeStamp) {
    super(type, timeStamp);
    this.appId = appId;
    this.diagnosticMsg = "";
  }

  /**
   * 获取本事件关联的应用ID
   * @return 应用程序ID
   */
  public ApplicationId getApplicationId() {
    return this.appId;
  }

  /**
   * 获取事件诊断信息
   * @return 诊断信息字符串
   */
  public String getDiagnosticMsg() {
    return this.diagnosticMsg;
  }

}