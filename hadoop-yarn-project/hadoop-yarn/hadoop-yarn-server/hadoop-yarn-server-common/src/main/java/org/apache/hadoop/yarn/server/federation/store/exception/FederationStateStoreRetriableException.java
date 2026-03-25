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
 * YARN联邦状态存储可重试异常，由FederationStateStore在抛出可重试操作失败时抛出。
 *
 */
public class FederationStateStoreRetriableException extends YarnException {

  private static final long serialVersionUID = 1L;

  /**
   * 使用异常原因构造可重试异常。
   * @param cause 原始异常原因
   */
  public FederationStateStoreRetriableException(Throwable cause) {
    super(cause);
  }

  /**
   * 使用错误信息构造可重试异常。
   * @param message 错误描述信息
   */
  public FederationStateStoreRetriableException(String message) {
    super(message);
  }

  /**
   * 使用错误信息和原始异常构造可重试异常。
   * @param message 错误描述信息
   * @param cause 原始异常原因
   */
  public FederationStateStoreRetriableException(String message,
      Throwable cause) {
    super(message, cause);
  }
}