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

import org.apache.hadoop.security.token.delegation.DelegationKey;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.server.federation.store.records.RouterMasterKey;
import org.apache.hadoop.yarn.server.federation.store.utils.FederationStateStoreUtils;
import org.apache.hadoop.yarn.util.Records;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.SQLException;

/**
 * 联邦状态存储SQL路由器主密钥结果处理器
 * 将SQL存储过程输出参数的结果转换为RouterMasterKey类型对象。
 */
public class RouterMasterKeyHandler implements ResultSetHandler<RouterMasterKey> {

  // 主密钥输出参数名称常量
  private final static String MASTERKEY_OUT = "masterKey_OUT";

  @Override
  public RouterMasterKey handle(Object... params) throws SQLException {
    // 创建空的RouterMasterKey记录对象
    RouterMasterKey routerMasterKey = Records.newRecord(RouterMasterKey.class);
    // 遍历所有输入参数
    for (Object param : params) {
      // 判断参数是否为联邦SQL输出参数类型
      if (param instanceof FederationSQLOutParameter) {
        FederationSQLOutParameter parameter = (FederationSQLOutParameter) param;
        // 获取参数名称
        String paramName = parameter.getParamName();
        // 获取参数值
        Object parmaValue = parameter.getValue();
        // 匹配到主密钥输出参数
        if (StringUtils.equalsIgnoreCase(paramName, MASTERKEY_OUT)) {
          // 解析参数值得到DelegationKey对象
          DelegationKey key = getDelegationKey(parmaValue);
          // 将DelegationKey属性填充到RouterMasterKey中
          routerMasterKey.setKeyId(key.getKeyId());
          routerMasterKey.setKeyBytes(ByteBuffer.wrap(key.getEncodedKey()));
          routerMasterKey.setExpiryDate(key.getExpiryDate());
        }
      }
    }
    return routerMasterKey;
  }

  /**
   * 从SQL输出参数解析得到DelegationKey对象。
   * @param paramMasterKey SQL输出的主密钥字符串
   * @return 解析完成的DelegationKey对象
   * @throws SQLException 解析失败时抛出SQL异常
   */
  private DelegationKey getDelegationKey(Object paramMasterKey) throws SQLException {
    try {
      // 创建空DelegationKey对象
      DelegationKey key = new DelegationKey();
      // 将参数转换为字符串
      String masterKey = String.valueOf(paramMasterKey);
      // 对字符串解码得到DelegationKey对象
      FederationStateStoreUtils.decodeWritable(key, masterKey);
      return key;
    } catch (IOException e) {
      throw new SQLException(e);
    }
  }
}