// 这个文件已经全部加上中文注释
/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements. See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership. The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License. You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged;

import org.apache.hadoop.yarn.exceptions.YarnException;

/**
 * 特权操作执行异常，用于表示NodeManager上需要提升权限执行的操作失败
 * 继承自YarnException，支持保存命令退出码、标准输出和标准错误信息
 */
public class PrivilegedOperationException extends YarnException {
  private static final long serialVersionUID = 1L;
  // 特权命令退出码
  private int exitCode = -1;
  // 特权命令标准输出内容
  private String output;
  // 特权命令标准错误输出内容
  private String errorOutput;

  /**
   * 空构造函数
   */
  public PrivilegedOperationException() {
    super();
  }

  /**
   * 带错误消息的构造函数
   * @param message 错误消息
   */
  public PrivilegedOperationException(String message) {
    super(message);
  }

  /**
   * 带完整执行信息的构造函数
   * @param message 错误消息
   * @param exitCode 命令退出码
   * @param output 命令标准输出
   * @param errorOutput 命令标准错误输出
   */
  public PrivilegedOperationException(String message, int exitCode,
      String output, String errorOutput) {
    super(message);
    this.exitCode = exitCode;
    this.output = output;
    this.errorOutput = errorOutput;
  }

  /**
   * 带底层异常的构造函数
   * @param cause 原始异常
   */
  public PrivilegedOperationException(Throwable cause) {
    super(cause);
  }

  /**
   * 带底层异常和完整执行信息的构造函数
   * @param cause 原始异常
   * @param exitCode 命令退出码
   * @param output 命令标准输出
   * @param errorOutput 命令标准错误输出
   */
  public PrivilegedOperationException(Throwable cause, int exitCode,
      String output, String errorOutput) {
    super(cause);
    this.exitCode = exitCode;
    this.output = output;
    this.errorOutput = errorOutput;
  }

  /**
   * 带错误消息和底层异常的构造函数
   * @param message 错误消息
   * @param cause 原始异常
   */
  public PrivilegedOperationException(String message, Throwable cause) {
    super(message, cause);
  }

  /**
   * 获取特权命令退出码
   * @return 命令退出码
   */
  public int getExitCode() {
    return exitCode;
  }

  /**
   * 获取特权命令标准输出
   * @return 标准输出字符串
   */
  public String getOutput() {
    return output;
  }

  /**
   * 获取特权命令标准错误输出
   * @return 标准错误输出字符串
   */
  public String getErrorOutput() { return errorOutput; }
}