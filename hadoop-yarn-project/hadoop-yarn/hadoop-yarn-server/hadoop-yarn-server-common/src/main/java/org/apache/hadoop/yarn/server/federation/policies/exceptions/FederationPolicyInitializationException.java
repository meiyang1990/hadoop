// 这个文件已经全部加上中文注释
/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.yarn.server.federation.policies.exceptions;

/**
 * YARN联邦路由策略初始化失败时抛出的异常。
 */
public class FederationPolicyInitializationException
    extends FederationPolicyException {
  /**
   * 带错误信息的构造方法。
   * @param message 错误描述信息
   */
  public FederationPolicyInitializationException(String message) {
    super(message);
  }

  /**
   * 包装底层异常的构造方法。
   * @param j 原始异常
   */
  public FederationPolicyInitializationException(Throwable j) {
    super(j);
  }
}