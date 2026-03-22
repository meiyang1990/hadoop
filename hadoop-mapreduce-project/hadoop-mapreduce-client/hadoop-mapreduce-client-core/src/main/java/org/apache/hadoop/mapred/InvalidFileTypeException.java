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

package org.apache.hadoop.mapred;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * 文件类型不匹配时抛出的异常，例如期望目录但获取到文件、或者文件类型不正确的场景。
 * 在MapReduce任务读取输入时，用于标识输入路径类型不符合要求的错误。
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class InvalidFileTypeException
    extends IOException {

  private static final long serialVersionUID = 1L;

  /**
   * 构造无消息的文件类型异常对象
   */
  public InvalidFileTypeException() {
    super();
  }

  /**
   * 构造带错误消息的文件类型异常对象
   * @param msg 异常描述信息
   */
  public InvalidFileTypeException(String msg) {
    super(msg);
  }

}