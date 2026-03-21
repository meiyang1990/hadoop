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

import java.util.Collection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.security.token.TokenSelector;

/**
 * 本地化服务令牌选择器，从用户凭据集合中挑选出匹配的本地化服务安全令牌。
 * 用于NodeManager拉取容器本地化资源时的RPC身份认证。
 */
public class LocalizerTokenSelector implements
    TokenSelector<LocalizerTokenIdentifier> {

  private static final Logger LOG =
       LoggerFactory.getLogger(LocalizerTokenSelector.class);

  @SuppressWarnings("unchecked")
  @Override
  public Token<LocalizerTokenIdentifier> selectToken(Text service,
      Collection<Token<? extends TokenIdentifier>> tokens) {

    LOG.debug("Using localizerTokenSelector.");

    // 遍历所有令牌查找匹配类型
    for (Token<? extends TokenIdentifier> token : tokens) {
      LOG.debug("Token of kind {} is found", token.getKind());
      // 匹配令牌类型，找到则返回匹配结果
      if (LocalizerTokenIdentifier.KIND.equals(token.getKind())) {
        return (Token<LocalizerTokenIdentifier>) token;
      }
    }
    LOG.debug("Returning null.");
    return null;
  }
}