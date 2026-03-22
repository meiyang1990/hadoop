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

package org.apache.hadoop.mapreduce.v2.api.protocolrecords;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.yarn.api.records.Token;

/**
 * 文件说明：MapReduce协议记录模块，定义更新委托令牌请求接口
 * 
 * 更新MapReduce委托令牌的请求接口，由客户端发送给ResourceManager，用于延长委托令牌的有效期
 * 委托令牌用于MapReduce作业执行过程中的身份认证，过期前需要更新延长生命周期
 */
@Public
@Evolving
public interface RenewDelegationTokenRequest {
  /**
   * 获取需要更新有效期的委托令牌
   * @return 待更新的委托令牌对象
   */
  Token getDelegationToken();

  /**
   * 设置需要更新有效期的委托令牌
   * @param dToken 待更新的委托令牌对象
   */
  void setDelegationToken(Token dToken);
}