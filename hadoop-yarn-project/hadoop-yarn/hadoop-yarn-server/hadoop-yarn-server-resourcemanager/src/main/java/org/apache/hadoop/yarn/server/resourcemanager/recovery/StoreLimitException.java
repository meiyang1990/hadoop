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
package org.apache.hadoop.yarn.server.resourcemanager.recovery;

import org.apache.hadoop.yarn.exceptions.YarnException;

/**
 * RM状态恢复存储异常，当应用数据大小超过RM状态存储限制时抛出该异常
 * 用于保护状态存储不会因数据过大导致性能问题或存储溢出
 */
public class StoreLimitException extends YarnException {
  private static final long serialVersionUID = 1L;

  /**
   * 构造方法，使用指定错误信息创建异常
   * @param message 错误描述信息
   */
  public StoreLimitException(String message) {
    super(message);
  }
}