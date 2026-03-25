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
 * YARN联邦集群RouterRMToken响应实体，封装从联邦状态存储获取的Router存储令牌信息。
 */
@Private
@Unstable
public abstract class RouterRMTokenResponse {

  /**
   * 创建新的RouterRMTokenResponse实例，封装传入的Router存储令牌。
   * @param routerStoreToken Router存储令牌对象
   * @return 初始化完成的RouterRMTokenResponse实例
   */
  @Private
  @Unstable
  public static RouterRMTokenResponse newInstance(RouterStoreToken routerStoreToken) {
    RouterRMTokenResponse request = Records.newRecord(RouterRMTokenResponse.class);
    request.setRouterStoreToken(routerStoreToken);
    return request;
  }

  /**
   * 获取封装的Router存储令牌。
   * @return Router存储令牌对象
   */
  @Public
  @Unstable
  public abstract RouterStoreToken getRouterStoreToken();

  /**
   * 设置封装的Router存储令牌。
   * @param routerStoreToken 要设置的Router存储令牌对象
   */
  @Private
  @Unstable
  public abstract void setRouterStoreToken(RouterStoreToken routerStoreToken);
}