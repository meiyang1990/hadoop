// 这个文件已经全部加上中文注释
/*
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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.converter;

/**
 * 公平调度器配置转换为容量调度器配置时，验证转换后配置失败抛出的异常。
 * 当容量调度器使用转换后的配置无法正常启动时抛出此异常。
 */
public class VerificationException extends RuntimeException {
  private static final long serialVersionUID = -7697926560416349141L;

  /**
   * 构造带错误信息和根异常的验证异常实例。
   * @param message 错误描述信息
   * @param cause 根异常
   */
  public VerificationException(String message, Throwable cause) {
    super(message, cause);
  }

  /**
   * 构造空的验证异常实例。
   */
  public VerificationException() {
  }
}