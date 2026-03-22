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

package org.apache.hadoop.mapreduce.lib.output.committer.manifest.impl;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathIOException;

/**
 * 输出路径验证过程抛出的异常类，用于标识输出提交阶段的验证失败，
 * 可以与其他类型的输出错误做区分处理。
 * 继承自PathIOException保留了出错路径信息，便于问题定位。
 */
@InterfaceAudience.Private
public class OutputValidationException extends PathIOException {

  /**
   * 构造输出验证异常实例。
   * @param path 验证失败的输出路径
   * @param error 错误描述信息
   */
  public OutputValidationException(Path path, String error) {
    super(path.toUri().toString(), error);
  }

  /**
   * 构造带根因的输出验证异常实例。
   * @param path 验证失败的输出路径
   * @param error 错误描述信息
   * @param cause 原始异常根因
   */
  public OutputValidationException(Path path,
      String error,
      Throwable cause) {
    super(path.toUri().toString(), error, cause);
  }
}