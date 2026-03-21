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
package org.apache.hadoop.yarn.server.federation.store.records;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.util.Records;

/**
 * YARN联邦状态存储获取Router RMToken的请求实体类，
 * 用于封装从联邦状态存储查询Router RM令牌所需的请求参数。
 */
@Private
@Unstable
public abstract class RouterRMTokenRequest {

  /**
   * 创建一个新的RouterRMTokenRequest实例，封装传入的存储令牌信息。
   * @param routerStoreToken Router存储令牌信息
   * @return 初始化完成的请求实例
   */
  @Private
  @Unstable
  public static RouterRMTokenRequest newInstance(RouterStoreToken routerStoreToken) {
    RouterRMTokenRequest request = Records.newRecord(RouterRMTokenRequest.class);
    request.setRouterStoreToken(routerStoreToken);
    return request;
  }

  /**
   * 获取请求中携带的Router存储令牌信息。
   * @return Router存储令牌
   */
  @Public
  @Unstable
  public abstract RouterStoreToken getRouterStoreToken();

  /**
   * 设置请求中的Router存储令牌信息。
   * @param routerStoreToken Router存储令牌
   */
  @Private
  @Unstable
  public abstract void setRouterStoreToken(RouterStoreToken routerStoreToken);
}