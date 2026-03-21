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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;

/**
 * 公平调度器中当队列名称格式非法时抛出的异常
 */
@Private
@Unstable
public class InvalidQueueNameException extends IllegalArgumentException {
  private static final long serialVersionUID = -7306320927804540011L;

  /**
   * 构造带有错误消息的非法队列名称异常
   * @param message 错误描述消息
   */
  public InvalidQueueNameException(String message) {
    super(message);
  }

  /**
   * 构造带有错误消息和根异常的非法队列名称异常
   * @param message 错误描述消息
   * @param t 根异常
   */
  public InvalidQueueNameException(String message, Throwable t) {
    super(message, t);
  }
}