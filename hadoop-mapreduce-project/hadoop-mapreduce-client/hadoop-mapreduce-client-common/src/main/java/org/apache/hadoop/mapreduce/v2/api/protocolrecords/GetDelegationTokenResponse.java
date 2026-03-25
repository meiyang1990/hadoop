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
package org.apache.hadoop.mapreduce.v2.api.protocolrecords;

import org.apache.hadoop.yarn.api.records.Token;

/**
 * 获取委派令牌响应接口，封装MapReduce获取委派令牌操作的返回结果
 * 用于MapReduce客户端与服务端之间的安全认证通信，传递获取到的委派令牌
 */
public interface GetDelegationTokenResponse {
  /**
   * 设置获取到的委派令牌
   * @param clientDToken 客户端使用的委派令牌实例
   */
  void setDelegationToken(Token clientDToken);
  
  /**
   * 获取响应中的委派令牌
   * @return 客户端用于身份认证的委派令牌
   */
  Token getDelegationToken();
}