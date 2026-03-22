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
 * 文件说明：MapReduce V2 客户端取消委托令牌请求协议记录
 * 功能描述：定义客户端向ResourceManager请求取消MapReduce委托令牌的请求结构
 */
/**
 * 客户端向ResourceManager发起取消委托令牌的请求接口
 * 核心职责：封装取消委托令牌请求所需的参数，用于MapReduce服务端与客户端之间的RPC通信
 * 使用场景：当应用不再需要委托令牌时，客户端主动发起取消请求，回收令牌资源
 */
@Public
@Evolving
public interface CancelDelegationTokenRequest {
  /**
   * 获取需要取消的委托令牌
   * @return 待取消的委托令牌实例
   */
  Token getDelegationToken();
  
  /**
   * 设置需要取消的委托令牌
   * @param dToken 待取消的委托令牌实例
   */
  void setDelegationToken(Token dToken);
}