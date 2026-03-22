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
 * 文件已存在异常，当目标文件已经存在且未配置允许覆盖时抛出
 * 用于MapReduce作业输出阶段处理文件冲突场景
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class FileAlreadyExistsException
    extends IOException {

  private static final long serialVersionUID = 1L;

  /**
   * 构造无异常信息的文件已存在异常
   */
  public FileAlreadyExistsException() {
    super();
  }

  /**
   * 构造带指定错误信息的文件已存在异常
   * @param msg 异常描述信息
   */
  public FileAlreadyExistsException(String msg) {
    super(msg);
  }
}