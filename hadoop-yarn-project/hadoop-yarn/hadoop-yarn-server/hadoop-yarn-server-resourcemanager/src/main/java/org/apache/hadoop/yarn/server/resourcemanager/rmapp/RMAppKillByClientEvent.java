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

import java.net.InetAddress;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationId;

/**
 * 客户端触发杀死应用事件，封装杀死请求的调用者信息用于审计日志记录
 * 当客户端主动请求杀死YARN应用时，生成该事件携带调用者身份与网络信息
 */
public class RMAppKillByClientEvent extends RMAppEvent {

  // 调用者的用户与组信息
  private final UserGroupInformation callerUGI;
  // 调用者的远程IP地址
  private final InetAddress ip;

  /**
   * 构造客户端杀死应用事件，封装请求相关信息
   * @param appId 目标应用ID
   * @param diagnostics 杀死事件的诊断信息
   * @param callerUGI 调用者的用户组信息
   * @param remoteIP 调用者的远程IP地址
   */
  public RMAppKillByClientEvent(ApplicationId appId, String diagnostics,
      UserGroupInformation callerUGI, InetAddress remoteIP) {
    super(appId, RMAppEventType.KILL, diagnostics);
    this.callerUGI = callerUGI;
    this.ip = remoteIP;
  }

  /**
   * 获取事件发起者的用户组信息
   * @return 调用者用户组信息
   */
  public final UserGroupInformation getCallerUGI() {
    return callerUGI;
  }

  /**
   * 获取事件发起者的远程IP地址
   * @return 调用者远程IP地址
   */
  public final InetAddress getIp() {
    return ip;
  }
}