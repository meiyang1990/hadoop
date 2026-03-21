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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.security;

import javax.crypto.SecretKey;

import org.apache.hadoop.security.token.SecretManager;

/**
 * 本地化令牌密钥管理器，负责NodeManager本地化服务令牌的生成与验证
 * 用于保护容器本地化过程中资源下载的安全认证
 */
public class LocalizerTokenSecretManager extends
    SecretManager<LocalizerTokenIdentifier> {

  private final SecretKey secretKey;
  
  /**
   * 构造函数，生成本地化令牌使用的根密钥
   */
  public LocalizerTokenSecretManager() {
    this.secretKey = generateSecret();
  }
  
  @Override
  protected byte[] createPassword(LocalizerTokenIdentifier identifier) {
    // 根据令牌标识符和根密钥生成令牌密码
    return createPassword(identifier.getBytes(), secretKey);
  }

  @Override
  public byte[] retrievePassword(LocalizerTokenIdentifier identifier)
      throws org.apache.hadoop.security.token.SecretManager.InvalidToken {
    // 重新计算令牌密码用于验证，创建和验证使用相同计算逻辑
    return createPassword(identifier.getBytes(), secretKey);
  }

  @Override
  public LocalizerTokenIdentifier createIdentifier() {
    // 创建空的本地化令牌标识符实例
    return new LocalizerTokenIdentifier();
  }

}