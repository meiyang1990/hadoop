// 这个文件已经全部加上中文注释
/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.hdfs.server.namenode;

import java.io.IOException;
import org.apache.hadoop.classification.InterfaceAudience;

/**
 * 文件整体说明：NameNode格式化失败时抛出的异常，用于标识HDFS元数据格式化过程中的错误
 * 当NameNode格式化流程出错时，抛出此异常通知上层调用者
 */
@InterfaceAudience.Private
public class NameNodeFormatException extends IOException {

  private static final long serialVersionUID = 1L;

  /**
   * 构造方法，带有异常信息和原始异常原因
   * @param message 异常描述信息
   * @param cause 原始异常栈
   */
  public NameNodeFormatException(String message, Throwable cause) {
    super(message, cause);
  }

  /**
   * 构造方法，仅带有异常信息
   * @param message 异常描述信息
   */
  public NameNodeFormatException(String message) {
    super(message);
  }
}