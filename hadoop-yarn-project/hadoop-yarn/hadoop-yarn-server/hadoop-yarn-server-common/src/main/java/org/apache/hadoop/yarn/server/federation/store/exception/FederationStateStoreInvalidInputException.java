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

package org.apache.hadoop.yarn.server.federation.store.exception;

import org.apache.hadoop.yarn.exceptions.YarnException;

/**
 * YARN联邦状态存储输入校验失败时抛出的异常。
 * 当联邦状态存储的各个输入校验器发现请求参数非法时，会抛出此异常。
 * 涉及校验器包括：{@code FederationMembershipStateStoreInputValidator}、
 * {@code FederationApplicationHomeSubClusterStoreInputValidator}、
 * {@code FederationPolicyStoreInputValidator}
 *
 */
public class FederationStateStoreInvalidInputException extends YarnException {

  /**
   * 序列化版本ID，由IDE自动生成。
   */
  private static final long serialVersionUID = -7352144682711430801L;

  /**
   * 使用指定根异常构造输入无效异常。
   * @param cause 根异常原因
   */
  public FederationStateStoreInvalidInputException(Throwable cause) {
    super(cause);
  }

  /**
   * 使用指定消息构造输入无效异常。
   * @param message 异常描述信息
   */
  public FederationStateStoreInvalidInputException(String message) {
    super(message);
  }

  /**
   * 使用指定消息和根异常构造输入无效异常。
   * @param message 异常描述信息
   * @param cause 根异常原因
   */
  public FederationStateStoreInvalidInputException(String message,
      Throwable cause) {
    super(message, cause);
  }
}