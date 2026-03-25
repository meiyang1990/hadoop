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

/**
 * MapReduce历史服务客户端代理令牌选择器，属于Hadoop MapReduce客户端安全模块
 * 负责从用户持有的令牌集合中选择匹配MapReduce历史服务的MR代理令牌
 */
package org.apache.hadoop.mapreduce.v2.security.client;

import java.util.Collection;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.v2.api.MRDelegationTokenIdentifier;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.security.token.TokenSelector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MapReduce历史服务代理令牌选择器实现类
 * 实现Hadoop通用TokenSelector接口，从当前获取到的所有令牌中筛选出
 * 用于访问MapReduce历史服务（JobHistoryServer）的MR委托令牌
 */
public class ClientHSTokenSelector implements
    TokenSelector<MRDelegationTokenIdentifier> {

  private static final Logger LOG = LoggerFactory
      .getLogger(ClientHSTokenSelector.class);

  /**
   * 从给定令牌集合中选择匹配目标服务的MR委托令牌
   * @param service 目标服务标识
   * @param tokens 客户端持有的所有令牌集合
   * @return 匹配的MR委托令牌，未找到则返回null
   */
  @SuppressWarnings("unchecked")
  public Token<MRDelegationTokenIdentifier> selectToken(Text service,
      Collection<Token<? extends TokenIdentifier>> tokens) {
    // 服务标识为空，直接返回null
    if (service == null) {
      return null;
    }
    // 调试日志：记录正在查找指定服务的令牌
    LOG.debug("Looking for a token with service " + service.toString());
    // 遍历所有令牌查找匹配项
    for (Token<? extends TokenIdentifier> token : tokens) {
      // 开启调试日志时输出当前令牌信息
      if (LOG.isDebugEnabled()) {
        LOG.debug("Token kind is " + token.getKind().toString()
            + " and the token's service name is " + token.getService());
      }
      // 匹配令牌类型和服务名称，找到匹配项直接返回
      if (MRDelegationTokenIdentifier.KIND_NAME.equals(token.getKind())
          && service.equals(token.getService())) {
        return (Token<MRDelegationTokenIdentifier>) token;
      }
    }
    // 未找到匹配令牌返回null
    return null;
  }
}