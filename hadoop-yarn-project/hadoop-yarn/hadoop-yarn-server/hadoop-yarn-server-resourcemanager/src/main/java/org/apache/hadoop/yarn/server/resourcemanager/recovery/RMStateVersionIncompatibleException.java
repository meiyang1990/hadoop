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

package org.apache.hadoop.yarn.server.resourcemanager.recovery;

import org.apache.hadoop.yarn.exceptions.YarnException;

/**
 * ResourceManager 恢复状态时，若加载的状态存储版本不兼容，则抛出该异常。
 * 用于标识恢复过程中版本不兼容错误，帮助用户识别存储格式版本 mismatch 问题。
 */
public class RMStateVersionIncompatibleException extends YarnException {

  private static final long serialVersionUID = 1364408L;

  /**
   * 通过根因异常构造版本不兼容异常。
   * @param cause 根因异常对象
   */
  public RMStateVersionIncompatibleException(Throwable cause) {
    super(cause);
  }

  /**
   * 通过错误消息构造版本不兼容异常。
   * @param message 错误描述信息
   */
  public RMStateVersionIncompatibleException(String message) {
    super(message);
  }

  /**
   * 通过错误消息和根因异常构造版本不兼容异常。
   * @param message 错误描述信息
   * @param cause 根因异常对象
   */
  public RMStateVersionIncompatibleException(String message, Throwable cause) {
    super(message, cause);
  }
}