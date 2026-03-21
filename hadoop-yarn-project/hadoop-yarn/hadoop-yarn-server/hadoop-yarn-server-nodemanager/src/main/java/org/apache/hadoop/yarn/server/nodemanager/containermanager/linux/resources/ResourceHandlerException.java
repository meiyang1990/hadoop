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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.yarn.exceptions.YarnException;

/**
 * Linux容器资源处理器异常，用于封装资源处理过程中发生的错误
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class ResourceHandlerException extends YarnException {
  private static final long serialVersionUID = 1L;

  /**
   * 构造无参资源处理器异常
   */
  public ResourceHandlerException() {
    super();
  }

  /**
   * 构造带消息的资源处理器异常
   * @param message 异常消息
   */
  public ResourceHandlerException(String message) {
    super(message);
  }

  /**
   * 构造带根源异常的资源处理器异常
   * @param cause 根源异常
   */
  public ResourceHandlerException(Throwable cause) {
    super(cause);
  }

  /**
   * 构造带消息和根源异常的资源处理器异常
   * @param message 异常消息
   * @param cause 根源异常
   */
  public ResourceHandlerException(String message, Throwable cause) {
    super(message, cause);
  }
}