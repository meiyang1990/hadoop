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

import java.lang.annotation.Annotation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.KerberosInfo;
import org.apache.hadoop.security.SecurityInfo;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.security.token.TokenInfo;
import org.apache.hadoop.security.token.TokenSelector;
import org.apache.hadoop.yarn.server.nodemanager.api.LocalizationProtocolPB;

/**
 * 本地化协议RPC安全信息实现，负责为NodeManager本地化协议提供Token选择器
 */
public class LocalizerSecurityInfo extends SecurityInfo {

  private static final Logger LOG =
       LoggerFactory.getLogger(LocalizerSecurityInfo.class);

  @Override
  public KerberosInfo getKerberosInfo(Class<?> protocol, Configuration conf) {
    return null;
  }

  @Override
  public TokenInfo getTokenInfo(Class<?> protocol, Configuration conf) {
    // 仅处理本地化PB协议，非目标协议返回null
    if (!protocol
        .equals(LocalizationProtocolPB.class)) {
      return null;
    }
    // 返回自定义TokenInfo，提供本地化Token选择器
    return new TokenInfo() {

      @Override
      public Class<? extends Annotation> annotationType() {
        return null;
      }

      @Override
      public Class<? extends TokenSelector<? extends TokenIdentifier>>
          value() {
        LOG.debug("Using localizerTokenSecurityInfo");
        return LocalizerTokenSelector.class;
      }
    };
  }
}