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

package org.apache.hadoop.mapreduce.v2.app;

import java.lang.annotation.Annotation;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.v2.api.MRClientProtocolPB;
import org.apache.hadoop.security.KerberosInfo;
import org.apache.hadoop.security.SecurityInfo;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.security.token.TokenInfo;
import org.apache.hadoop.security.token.TokenSelector;
import org.apache.hadoop.yarn.security.client.ClientToAMTokenSelector;

/**
 * MapReduce客户端到ApplicationMaster安全认证信息配置类
 * 负责为MR客户端与AM之间的RPC通信配置令牌选择器，支持YARN ClientToAM令牌认证机制
 */
public class MRClientSecurityInfo extends SecurityInfo {

  @Override
  /**
   * 获取指定协议的Kerberos认证信息
   * 本场景不使用Kerberos认证，直接返回null
   * @param protocol RPC协议类
   * @param conf 配置对象
   * @return 始终返回null，表示不使用Kerberos认证
   */
  public KerberosInfo getKerberosInfo(Class<?> protocol, Configuration conf) {
    return null;
  }

  @Override
  /**
   * 获取指定协议的令牌认证信息
   * 为MRClientProtocolPB协议配置ClientToAM令牌选择器，实现基于YARN令牌的身份认证
   * @param protocol RPC协议类
   * @param conf 配置对象
   * @return 配置好的TokenInfo对象，如果不是目标协议则返回null
   */
  public TokenInfo getTokenInfo(Class<?> protocol, Configuration conf) {
    // 仅处理MR客户端到AM的PB协议
    if (!protocol.equals(MRClientProtocolPB.class)) {
      return null;
    }
    return new TokenInfo() {

      @Override
      public Class<? extends Annotation> annotationType() {
        return null;
      }

      @Override
      public Class<? extends TokenSelector<? extends TokenIdentifier>>
          value() {
        // 使用YARN提供的ClientToAM令牌选择器
        return ClientToAMTokenSelector.class;
      }
    };
  }
}