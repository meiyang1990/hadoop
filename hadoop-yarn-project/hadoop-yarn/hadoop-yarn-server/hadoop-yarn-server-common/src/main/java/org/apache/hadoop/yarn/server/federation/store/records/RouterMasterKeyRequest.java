// 这个文件已经全部加上中文注释
/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.yarn.server.federation.store.records;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.util.Records;

/**
 * YARN联邦存储服务中更新路由器密钥的请求封装类，用于向联邦状态存储传递路由器密钥信息。
 */
@Private
@Unstable
public abstract class RouterMasterKeyRequest {

  /**
   * 创建新的路由器密钥请求实例。
   * @param routerMasterKey 路由器密钥信息
   * @return 初始化完成的请求实例
   */
  @Private
  @Unstable
  public static RouterMasterKeyRequest newInstance(RouterMasterKey routerMasterKey) {
    RouterMasterKeyRequest request = Records.newRecord(RouterMasterKeyRequest.class);
    request.setRouterMasterKey(routerMasterKey);
    return request;
  }

  /**
   * 获取请求中携带的路由器密钥信息。
   * @return 路由器密钥
   */
  @Public
  @Unstable
  public abstract RouterMasterKey getRouterMasterKey();

  /**
   * 设置请求中携带的路由器密钥信息。
   * @param routerMasterKey 路由器密钥
   */
  @Private
  @Unstable
  public abstract void setRouterMasterKey(RouterMasterKey routerMasterKey);
}