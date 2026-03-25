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

package org.apache.hadoop.mapreduce.security.token.delegation;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenIdentifier;

/**
 * MapReduce框架特定的代理令牌标识符，用于标识MapReduce服务发放的代理令牌
 * 继承通用抽象代理令牌标识符，实现MapReduce场景下的令牌类型标识
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class DelegationTokenIdentifier 
    extends AbstractDelegationTokenIdentifier {
  // MapReduce代理令牌的类型标识常量
  public static final Text MAPREDUCE_DELEGATION_KIND = 
    new Text("MAPREDUCE_DELEGATION_TOKEN");

  /**
   * 构造空的代理令牌标识符，用于反序列化场景
   */
  public DelegationTokenIdentifier() {
  }

  /**
   * 构造完整的MapReduce代理令牌标识符
   * @param owner 令牌所有者的有效用户名
   * @param renewer 可更新该令牌的用户名
   * @param realUser 令牌所有者的真实用户名（用于代理用户场景）
   */
  public DelegationTokenIdentifier(Text owner, Text renewer, Text realUser) {
    super(owner, renewer, realUser);
  }

  @Override
  public Text getKind() {
    // 返回MapReduce代理令牌的类型标识
    return MAPREDUCE_DELEGATION_KIND;
  }

}