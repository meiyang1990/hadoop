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

/**
 * CSI卷供应失败时抛出的异常，属于YARN CSI存储卷集成模块的自定义异常类。
 * 当动态创建、供应存储卷过程中发生错误时抛出该异常。
 */
public class VolumeProvisioningException extends VolumeException {

  /**
   * 构造带错误消息的卷供应异常。
   * @param message 错误描述信息
   */
  public VolumeProvisioningException(String message) {
    super(message);
  }

  /**
   * 构造带错误消息和原始异常的卷供应异常。
   * @param message 错误描述信息
   * @param e 原始异常
   */
  public VolumeProvisioningException(String message, Exception e) {
    super(message, e);
  }
}