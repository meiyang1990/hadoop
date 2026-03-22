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
package org.apache.hadoop.hdfs.server.namenode;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;

/**
 * 从不支持保留路径的版本升级到支持保留路径的版本时，如果Fsimage中存在和保留路径冲突的路径名称，则抛出该异常。
 * 用于在升级过程中提前拦截非法路径，避免破坏HDFS保留路径的语义规则。
 */
@InterfaceAudience.Private
public class IllegalReservedPathException extends IOException {
  private static final long serialVersionUID = 1L;

  /**
   * 构造包含指定错误信息和根因异常的异常实例
   * @param message 错误描述信息
   * @param cause 根因异常
   */
  public IllegalReservedPathException(String message, Throwable cause) {
    super(message, cause);
  }

  /**
   * 构造包含指定错误信息的异常实例
   * @param message 错误描述信息
   */
  public IllegalReservedPathException(String message) {
    super(message);
  }
}