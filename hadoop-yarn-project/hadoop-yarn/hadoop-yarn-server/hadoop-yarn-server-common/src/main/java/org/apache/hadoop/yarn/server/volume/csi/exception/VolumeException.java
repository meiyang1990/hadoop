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
package org.apache.hadoop.yarn.server.volume.csi.exception;

import org.apache.hadoop.yarn.exceptions.YarnException;

/**
 * CSI卷相关异常的基类，所有YARN CSI卷管理相关异常都继承此类
 */
public class VolumeException extends YarnException {

  /**
   * 构造只包含错误消息的异常
   * @param message 错误消息
   */
  public VolumeException(String message) {
    super(message);
  }

  /**
   * 构造包含错误消息和根异常的异常
   * @param message 错误消息
   * @param e 根异常
   */
  public VolumeException(String message, Exception e) {
    super(message, e);
  }
}