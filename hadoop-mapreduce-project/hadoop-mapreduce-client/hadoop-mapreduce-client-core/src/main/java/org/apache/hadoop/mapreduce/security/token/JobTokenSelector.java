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

package org.apache.hadoop.mapreduce.security.token;

import java.util.Collection;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.security.token.TokenSelector;

/**
 * MapReduce作业令牌选择器，用于从令牌集合中查找匹配指定服务的作业令牌。
 * 为MapReduce作业认证流程提供匹配对应服务的作业令牌选择能力。
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class JobTokenSelector implements TokenSelector<JobTokenIdentifier> {

  /**
   * 从令牌集合中选择匹配指定服务的作业令牌
   * @param service 目标服务标识
   * @param tokens 可用令牌集合
   * @return 匹配的作业令牌，无匹配返回null
   */
  @SuppressWarnings("unchecked")
  @Override
  public Token<JobTokenIdentifier> selectToken(Text service,
      Collection<Token<? extends TokenIdentifier>> tokens) {
    // 服务标识为空直接返回null
    if (service == null) {
      return null;
    }
    // 遍历所有令牌查找匹配项
    for (Token<? extends TokenIdentifier> token : tokens) {
      // 匹配令牌类型为作业令牌且服务标识一致
      if (JobTokenIdentifier.KIND_NAME.equals(token.getKind())
          && service.equals(token.getService())) {
        return (Token<JobTokenIdentifier>) token;
      }
    }
    // 未找到匹配令牌
    return null;
  }
}