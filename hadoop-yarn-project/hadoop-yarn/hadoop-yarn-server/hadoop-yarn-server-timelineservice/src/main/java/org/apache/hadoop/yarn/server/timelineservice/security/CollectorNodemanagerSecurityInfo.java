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

package org.apache.hadoop.yarn.server.timelineservice.security;

import java.lang.annotation.Annotation;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.KerberosInfo;
import org.apache.hadoop.security.SecurityInfo;
import org.apache.hadoop.security.token.TokenInfo;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.api.CollectorNodemanagerProtocolPB;

/**
 * CollectorNodemanager 协议的安全信息实现类，为时间线采集器和NodeManager之间的RPC通信提供Kerberos认证配置。
 */
@Public
@Evolving
public class CollectorNodemanagerSecurityInfo extends SecurityInfo {

  @Override
  public KerberosInfo getKerberosInfo(Class<?> protocol, Configuration conf) {
    // 仅针对CollectorNodemanagerProtocolPB协议返回安全信息，其他协议返回空
    if (!protocol
        .equals(CollectorNodemanagerProtocolPB.class)) {
      return null;
    }
    // 匿名内部类实现KerberosInfo，提供服务端principal配置
    return new KerberosInfo() {

      @Override
      public Class<? extends Annotation> annotationType() {
        return null;
      }

      @Override
      public String serverPrincipal() {
        // 返回NodeManager的Kerberos principal配置项键
        return YarnConfiguration.NM_PRINCIPAL;
      }

      @Override
      public String clientPrincipal() {
        return null;
      }
    };
  }

  @Override
  public TokenInfo getTokenInfo(Class<?> protocol, Configuration conf) {
    // 不使用delegation token认证，返回null
    return null;
  }
}