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

package org.apache.hadoop.yarn.server.federation.store.sql;

import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.security.client.YARNDelegationTokenIdentifier;
import org.apache.hadoop.yarn.server.federation.store.records.RouterStoreToken;
import org.apache.hadoop.yarn.util.Records;

import java.io.IOException;
import java.sql.SQLException;

import static org.apache.hadoop.yarn.server.federation.store.utils.FederationStateStoreUtils.decodeWritable;

/**
 * 路由器存储令牌处理器，用于将SQL存储过程输出参数结果解析为RouterStoreToken对象。
 * 供YARN联邦状态存储SQL实现使用，处理存储委托令牌的查询结果。
 */
public class RouterStoreTokenHandler implements ResultSetHandler<RouterStoreToken> {

  // 令牌标识符输出参数名常量
  private final static String TOKENIDENT_OUT = "tokenIdent_OUT";
  // 令牌信息输出参数名常量
  private final static String TOKEN_OUT = "token_OUT";
  // 更新日期输出参数名常量
  private final static String RENEWDATE_OUT = "renewDate_OUT";

  @Override
  /**
   * 处理SQL存储过程输出参数，转换为RouterStoreToken对象。
   * @param params 存储过程输出参数数组
   * @return 解析完成的RouterStoreToken对象
   * @throws SQLException 参数解析或转换异常
   */
  public RouterStoreToken handle(Object... params) throws SQLException {
    // 创建新的RouterStoreToken记录实例
    RouterStoreToken storeToken = Records.newRecord(RouterStoreToken.class);
    // 遍历所有输出参数
    for (Object param : params) {
      // 仅处理FederationSQLOutParameter类型的输出参数
      if (param instanceof FederationSQLOutParameter) {
        FederationSQLOutParameter parameter = (FederationSQLOutParameter) param;
        // 获取参数名称
        String paramName = parameter.getParamName();
        // 获取参数值
        Object parmaValue = parameter.getValue();
        // 匹配令牌标识符参数，设置到结果对象
        if (StringUtils.equalsIgnoreCase(paramName, TOKENIDENT_OUT)) {
          YARNDelegationTokenIdentifier identifier = getYARNDelegationTokenIdentifier(parmaValue);
          storeToken.setIdentifier(identifier);
        // 匹配令牌信息参数，设置到结果对象
        } else if (StringUtils.equalsIgnoreCase(paramName, TOKEN_OUT)) {
          String tokenInfo = getTokenInfo(parmaValue);
          storeToken.setTokenInfo(tokenInfo);
        // 匹配更新日期参数，设置到结果对象
        } else if(StringUtils.equalsIgnoreCase(paramName, RENEWDATE_OUT)){
          Long renewDate = getRenewDate(parmaValue);
          storeToken.setRenewDate(renewDate);
        }
      }
    }
    return storeToken;
  }

  /**
   * 从字符串参数解析YARN委托令牌标识符。
   * @param tokenIdent 编码后的令牌标识符字符串
   * @return 解析完成的YARNDelegationTokenIdentifier对象
   * @throws SQLException 解码IO异常包装
   */
  private YARNDelegationTokenIdentifier getYARNDelegationTokenIdentifier(Object tokenIdent)
      throws SQLException {
    try {
      YARNDelegationTokenIdentifier resultIdentifier =
          Records.newRecord(YARNDelegationTokenIdentifier.class);
      // 对字符串进行Writable解码，得到标识符对象
      decodeWritable(resultIdentifier, String.valueOf(tokenIdent));
      return resultIdentifier;
    } catch (IOException e) {
      throw new SQLException(e);
    }
  }

  /**
   * 获取令牌信息字符串。
   * @param tokenInfo 原始参数值
   * @return 字符串形式的令牌信息
   */
  private String getTokenInfo(Object tokenInfo) {
    return String.valueOf(tokenInfo);
  }

  /**
   * 从字符串参数解析更新日期长整型值。
   * @param renewDate 原始参数值
   * @return 长整型的更新日期时间戳
   */
  private Long getRenewDate(Object renewDate) {
    return Long.parseLong(String.valueOf(renewDate));
  }
}