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

package org.apache.hadoop.mapred.uploader;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * MapReduce框架Jar包上传器自定义异常，用于表示上传过程中发生的错误。
 */
@InterfaceAudience.Private
@InterfaceStability.Stable
/**
 * MapReduce作业框架Jar包上传异常类，封装上传过程中的错误信息
 */
class UploaderException extends Exception {

  private static final long serialVersionUID = 1L;

  /**
   * 构造带错误信息的上传异常对象
   * @param message 错误描述信息
   */
  UploaderException(String message) {
    super(message);
  }
}