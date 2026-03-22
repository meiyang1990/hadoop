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

package org.apache.hadoop.mapreduce.v2.app.rm;

/**
 * 从ResourceManager申请容器发生致命失败时抛出的异常
 */
public class RMContainerAllocationException extends Exception {
  private static final long serialVersionUID = 1L;

  /**
   * 构造方法，使用指定异常原因创建异常对象
   * @param cause 异常原因
   */
  public RMContainerAllocationException(Throwable cause) { super(cause); }

  /**
   * 构造方法，使用指定错误信息创建异常对象
   * @param message 错误描述信息
   */
  public RMContainerAllocationException(String message) { super(message); }

  /**
   * 构造方法，使用指定错误信息和异常原因创建异常对象
   * @param message 错误描述信息
   * @param cause 异常原因
   */
  public RMContainerAllocationException(String message, Throwable cause) {
    super(message, cause);
  }
}