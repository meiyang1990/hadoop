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

package org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.event;

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEventType;

/**
 * 应用尝试注册事件，用于ApplicationMaster向ResourceManager完成注册后通知RM状态机处理。
 * 携带了ApplicationMaster的网络地址和追踪页面URL等注册信息。
 */
public class RMAppAttemptRegistrationEvent extends RMAppAttemptEvent {

  // ApplicationMaster所在主机地址
  private final String host;
  // ApplicationMaster RPC服务端口
  private int rpcport;
  // ApplicationMaster追踪页面URL
  private String trackingurl;

  /**
   * 构造应用尝试注册事件。
   * @param appAttemptId 应用尝试ID
   * @param host ApplicationMaster主机地址
   * @param rpcPort ApplicationMaster RPC端口
   * @param trackingUrl ApplicationMaster追踪页面URL
   */
  public RMAppAttemptRegistrationEvent(ApplicationAttemptId appAttemptId,
      String host, int rpcPort, String trackingUrl) {
    super(appAttemptId, RMAppAttemptEventType.REGISTERED);
    this.host = host;
    this.rpcport = rpcPort;
    this.trackingurl = trackingUrl;
  }

  public String getHost() {
    return this.host;
  }

  public int getRpcport() {
    return this.rpcport;
  }

  public String getTrackingurl() {
    return this.trackingurl;
  }
}