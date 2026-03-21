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

package org.apache.hadoop.yarn.server.nodemanager.amrmproxy;

import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;

/**
 * 封装AMRM令牌和ApplicationMaster注册响应的数据容器，用于AMRM代理中转注册结果
 */
public class TokenAndRegisterResponse {
  private Token<AMRMTokenIdentifier> token;
  private RegisterApplicationMasterResponse response;

  /**
   * 构造令牌与注册响应的容器对象
   * @param pToken AMRM身份认证令牌
   * @param pResponse ApplicationMaster注册响应
   */
  public TokenAndRegisterResponse(Token<AMRMTokenIdentifier> pToken,
      RegisterApplicationMasterResponse pResponse) {
    this.token = pToken;
    this.response = pResponse;
  }

  /**
   * 获取AMRM身份认证令牌
   * @return AMRM令牌对象
   */
  public Token<AMRMTokenIdentifier> getToken() {
    return token;
  }

  /**
   * 获取ApplicationMaster注册响应
   * @return 注册响应对象
   */
  public RegisterApplicationMasterResponse getResponse() {
    return response;
  }
}