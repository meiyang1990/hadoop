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
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenSelector;

/**
 * MapReduce 场景专用的代理令牌选择器，从凭据中筛选出对应MapReduce服务的代理令牌
 * 继承通用抽象选择器实现，专门适配MapReduce的代理令牌类型
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class DelegationTokenSelector
    extends AbstractDelegationTokenSelector<DelegationTokenIdentifier>{

  /**
   * 构造MapReduce专用的代理令牌选择器，传入MapReduce代理令牌类型标识
   */
  public DelegationTokenSelector() {
    super(DelegationTokenIdentifier.MAPREDUCE_DELEGATION_KIND);
  }
}