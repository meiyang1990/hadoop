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

package org.apache.hadoop.yarn.server.nodemanager.security;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;

/**
 * NodeManager 代理令牌管理器，负责处理NodeManager上代理令牌的更新操作。
 * 提供以登录用户身份代理续期 delegation token 的能力。
 */
public class NMDelegationTokenManager {

  private final Configuration conf;

  /**
   * 构造方法，传入配置对象。
   * @param conf Hadoop配置对象
   */
  public NMDelegationTokenManager(Configuration conf) {
    this.conf = conf;
  }

  /**
   * 以当前登录用户身份续期指定令牌。
   * @param token 需要续期的代理令牌
   * @return 续期后令牌的过期时间戳
   * @throws IOException IO操作异常
   * @throws InterruptedException 线程中断异常
   */
  public Long renewToken(Token<? extends TokenIdentifier> token)
      throws IOException, InterruptedException {
    UserGroupInformation ugi = UserGroupInformation.getLoginUser();
    return ugi.doAs((PrivilegedExceptionAction<Long>) () -> token.renew(conf));
  }
}