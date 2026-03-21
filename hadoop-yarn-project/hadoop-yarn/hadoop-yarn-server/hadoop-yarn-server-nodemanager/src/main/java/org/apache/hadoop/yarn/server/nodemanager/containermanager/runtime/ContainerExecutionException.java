// 这个文件已经全部加上中文注释
/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.yarn.server.nodemanager.containermanager.runtime;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.yarn.exceptions.YarnException;

/**
 * 容器执行器执行过程中抛出的异常，类名不使用Runtime后缀以避免和Java RuntimeException混淆
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class ContainerExecutionException extends YarnException {
  private static final long serialVersionUID = 1L;
  /** 未设置退出码的默认值 */
  private static final int EXIT_CODE_UNSET = -1;
  /** 未设置输出的默认值 */
  private static final String OUTPUT_UNSET = "<unknown>";

  /** 容器退出码 */
  private int exitCode;
  /** 容器标准输出内容 */
  private String output;
  /** 容器错误输出内容 */
  private String errorOutput;

  /**
   * 仅包含错误消息的构造方法
   * @param message 错误消息
   */
  public ContainerExecutionException(String message) {
    super(message);
    exitCode = EXIT_CODE_UNSET;
    output = OUTPUT_UNSET;
    errorOutput = OUTPUT_UNSET;
  }

  /**
   * 包装底层异常的构造方法
   * @param throwable 原始异常
   */
  public ContainerExecutionException(Throwable throwable) {
    super(throwable);
    exitCode = EXIT_CODE_UNSET;
    output = OUTPUT_UNSET;
    errorOutput = OUTPUT_UNSET;
  }

  /**
   * 包含错误消息和退出码的构造方法
   * @param message 错误消息
   * @param exitCode 容器退出码
   */
  public ContainerExecutionException(String message, int exitCode) {
    super(message);
    this.exitCode = exitCode;
    this.output = OUTPUT_UNSET;
    this.errorOutput = OUTPUT_UNSET;
  }

  /**
   * 完整信息构造方法，包含错误消息、退出码和输出内容
   * @param message 错误消息
   * @param exitCode 容器退出码
   * @param output 容器标准输出
   * @param errorOutput 容器错误输出
   */
  public ContainerExecutionException(String message, int exitCode, String
      output, String errorOutput) {
    super(message);
    this.exitCode = exitCode;
    this.output = output;
    this.errorOutput = errorOutput;
  }

  /**
   * 包装原始异常并包含退出码和输出内容的构造方法
   * @param cause 原始异常
   * @param exitCode 容器退出码
   * @param output 容器标准输出
   * @param errorOutput 容器错误输出
   */
  public ContainerExecutionException(Throwable cause, int exitCode, String
      output, String errorOutput) {
    super(cause);
    this.exitCode = exitCode;
    this.output = output;
    this.errorOutput = errorOutput;
  }

  /** 获取容器退出码 */
  public int getExitCode() {
    return exitCode;
  }

  /** 获取容器标准输出 */
  public String getOutput() {
    return output;
  }

  /** 获取容器错误输出 */
  public String getErrorOutput() {
    return errorOutput;
  }

  /** 获取默认未设置的退出码 */
  public static int getDefaultExitCode() {
    return EXIT_CODE_UNSET;
  }

}