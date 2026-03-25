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

package org.apache.hadoop.mapreduce.v2.security.client;

import java.lang.annotation.Annotation;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.v2.api.HSClientProtocolPB;
import org.apache.hadoop.mapreduce.v2.jobhistory.JHAdminConfig;
import org.apache.hadoop.security.KerberosInfo;
import org.apache.hadoop.security.SecurityInfo;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.security.token.TokenInfo;
import org.apache.hadoop.security.token.TokenSelector;

/**
 * 历史服务器客户端安全信息配置类
 * 负责为MapReduce历史服务器PB协议客户端提供Kerberos认证信息和Delegation Token选择器配置
 * 继承SecurityInfo抽象类，实现Hadoop安全认证框架的扩展点
 */
public class ClientHSSecurityInfo extends SecurityInfo {
    
  @Override
  /**
   * 获取指定协议的Kerberos认证信息
   * @param protocol 协议接口类
   * @param conf 配置对象
   * @return 封装好的Kerberos信息对象，非目标协议则返回null
   */
  public KerberosInfo getKerberosInfo(Class<?> protocol, Configuration conf) {
    // 仅处理历史服务器客户端PB协议
    if (!protocol
        .equals(HSClientProtocolPB.class)) {
      return null;
    }
    return new KerberosInfo() {

      @Override
      public Class<? extends Annotation> annotationType() {
        return null;
      }

      @Override
      // 从配置中获取历史服务器服务端的Kerberos principal配置项
      public String serverPrincipal() {
        return JHAdminConfig.MR_HISTORY_PRINCIPAL;
      }

      @Override
      public String clientPrincipal() {
        return null;
      }
    };
  }

  @Override
  /**
   * 获取指定协议的Token信息和选择器
   * @param protocol 协议接口类
   * @param conf 配置对象
   * @return 封装好的Token信息，指定对应的Token选择器，非目标协议则返回null
   */
  public TokenInfo getTokenInfo(Class<?> protocol, Configuration conf) {
    // 仅处理历史服务器客户端PB协议
    if (!protocol
        .equals(HSClientProtocolPB.class)) {
      return null;
    }
    return new TokenInfo() {

      @Override
      public Class<? extends Annotation> annotationType() {
        return null;
      }

      @Override
      // 使用历史服务器专用的Token选择器，用于选择正确的Delegation Token
      public Class<? extends TokenSelector<? extends TokenIdentifier>>
          value() {
        return ClientHSTokenSelector.class;
      }
    };  }

}