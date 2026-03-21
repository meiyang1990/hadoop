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

package org.apache.hadoop.yarn.server;

import java.lang.annotation.Annotation;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.KerberosInfo;
import org.apache.hadoop.security.SecurityInfo;
import org.apache.hadoop.security.token.TokenInfo;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.api.ResourceTrackerPB;

/**
 * ResourceManager与NodeManager之间的Kerberos安全信息配置类
 * 为RM-NM之间的RPC通信提供安全认证元信息
 */
public class RMNMSecurityInfoClass extends SecurityInfo {

  /**
   * 获取ResourceTrackerPB协议的Kerberos认证信息
   * @param protocol 待查询的协议类
   * @param conf YARN配置对象
   * @return 配置好的Kerberos信息对象，非ResourceTrackerPB协议返回null
   */
  @Override
  public KerberosInfo getKerberosInfo(Class<?> protocol, Configuration conf) {
    // 仅处理ResourceTrackerPB协议，其他协议不提供安全信息
    if (!protocol.equals(ResourceTrackerPB.class)) {
      return null;
    }
    // 实现匿名内部类，指定RM服务端和NM客户端的Kerberos主体配置项
    return new KerberosInfo() {

      @Override
      public Class<? extends Annotation> annotationType() {
        return null;
      }

      @Override
      public String serverPrincipal() {
        // 返回ResourceManager的Kerberos主体配置键
        return YarnConfiguration.RM_PRINCIPAL;
      }

      @Override
      public String clientPrincipal() {
        // 返回NodeManager的Kerberos主体配置键
        return YarnConfiguration.NM_PRINCIPAL;
      }
    };
  }

  /**
   * 获取令牌认证信息，RM-NM通信不使用令牌认证，返回null
   * @param protocol 待查询的协议类
   * @param conf YARN配置对象
   * @return 始终返回null
   */
  @Override
  public TokenInfo getTokenInfo(Class<?> protocol, Configuration conf) {
    return null;
  }

}