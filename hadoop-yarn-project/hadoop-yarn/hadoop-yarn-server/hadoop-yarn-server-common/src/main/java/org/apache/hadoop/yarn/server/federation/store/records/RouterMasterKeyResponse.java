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
 * YARN联邦状态存储查询路由主密钥的响应类，封装查询到的路由主密钥信息。
 */
@Private
@Unstable
public abstract class RouterMasterKeyResponse {

  /**
   * 创建新的RouterMasterKeyResponse实例，初始化设置路由主密钥。
   * @param masterKey 路由主密钥对象
   * @return 初始化完成的响应实例
   */
  @Private
  @Unstable
  public static RouterMasterKeyResponse newInstance(RouterMasterKey masterKey) {
    RouterMasterKeyResponse request = Records.newRecord(RouterMasterKeyResponse.class);
    request.setRouterMasterKey(masterKey);
    return request;
  }

  /**
   * 获取查询到的路由主密钥。
   * @return 路由主密钥对象
   */
  @Public
  @Unstable
  public abstract RouterMasterKey getRouterMasterKey();

  /**
   * 设置路由主密钥。
   * @param masterKey 路由主密钥对象
   */
  @Private
  @Unstable
  public abstract void setRouterMasterKey(RouterMasterKey masterKey);
}