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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.conf;

import org.apache.hadoop.yarn.exceptions.YarnException;

/**
 * 容量调度器配置存储版本不兼容异常，当{@link YarnConfigurationStore}加载持久化配置时，
 * 如果检测到当前存储的schema版本与当前服务不兼容，就会抛出此异常
 */
public class YarnConfStoreVersionIncompatibleException extends
    YarnException {
  private static final long serialVersionUID = -28298582253579013629L;

  /**
   * 根据异常原因构造版本不兼容异常
   * @param cause 原始异常原因
   */
  public YarnConfStoreVersionIncompatibleException(Throwable cause) {
    super(cause);
  }

  /**
   * 根据错误消息构造版本不兼容异常
   * @param message 错误描述信息
   */
  public YarnConfStoreVersionIncompatibleException(String message) {
    super(message);
  }

  /**
   * 根据错误消息和原始异常构造版本不兼容异常
   * @param message 错误描述信息
   * @param cause 原始异常原因
   */
  public YarnConfStoreVersionIncompatibleException(
      String message, Throwable cause) {
    super(message, cause);
  }
}