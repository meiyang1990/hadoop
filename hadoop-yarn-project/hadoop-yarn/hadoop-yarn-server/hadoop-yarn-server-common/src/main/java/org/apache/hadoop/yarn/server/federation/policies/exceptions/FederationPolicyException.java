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

import org.apache.hadoop.yarn.exceptions.YarnException;

/**
 * YARN联邦路由策略异常基类，用于表示联邦策略执行过程中出现的各类错误。
 */
public class FederationPolicyException extends YarnException {
  /**
   * 构造带有错误信息的联邦策略异常。
   * @param s 错误描述信息
   */
  public FederationPolicyException(String s) {
    super(s);
  }

  /**
   * 构造包装底层异常的联邦策略异常。
   * @param t 原始异常
   */
  public FederationPolicyException(Throwable t) {
    super(t);
  }
}