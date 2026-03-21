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
 * YARN联邦状态存储层抛出的通用异常，用于封装状态存储操作过程中出现的错误。
 *
 */
public class FederationStateStoreException extends YarnException {

  /**
   * 序列化版本ID，由IDE自动生成。
   */
  private static final long serialVersionUID = -6453353714832159296L;

  /**
   * 构造无消息无原因的联邦状态存储异常。
   */
  public FederationStateStoreException() {
    super();
  }

  /**
   * 构造带指定错误消息的联邦状态存储异常。
   * @param message 错误消息文本
   */
  public FederationStateStoreException(String message) {
    super(message);
  }

  /**
   * 构造包装原始异常的联邦状态存储异常。
   * @param cause 原始异常原因
   */
  public FederationStateStoreException(Throwable cause) {
    super(cause);
  }

  /**
   * 构造带错误消息并包装原始异常的联邦状态存储异常。
   * @param message 错误消息文本
   * @param cause 原始异常原因
   */
  public FederationStateStoreException(String message, Throwable cause) {
    super(message, cause);
  }

}