// 这个文件已经全部加上中文注释
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.converter;

import org.apache.commons.cli.MissingArgumentException;

/**
 * 公平调度器(FS)转容量调度器(CS)配置转换的前置条件检查异常
 * 表示在执行FS->CS配置转换前，部分前置条件未满足要求
 *
 */
public class PreconditionException extends RuntimeException {
  private static final long serialVersionUID = 7976747724949372164L;

  /**
   * 构造带错误信息的前置条件异常
   * @param message 错误描述信息
   */
  public PreconditionException(String message) {
    super(message);
  }

  /**
   * 构造带错误信息和根异常的前置条件异常
   * @param message 错误描述信息
   * @param cause 根异常
   */
  public PreconditionException(String message, Throwable cause) {
    super(message, cause);
  }

  /**
   * 构造带错误信息和命令行参数缺失异常的前置条件异常
   * @param message 错误描述信息
   * @param ex 命令行参数缺失异常
   */
  public PreconditionException(String message, MissingArgumentException ex) {
    super(message, ex);
  }
}