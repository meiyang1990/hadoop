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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.converter;

/**
 * 公平调度器(FS)配置转容量调度器(CS)配置过程中，遇到无法恢复错误时抛出的异常
 * 例如遇到不支持的配置项等场景
 *
 */
public class ConversionException extends RuntimeException {
  private static final long serialVersionUID = 4161836727287317835L;

  /**
   * 构造带错误信息的转换异常
   * @param message 错误描述信息
   */
  public ConversionException(String message) {
    super(message);
  }

  /**
   * 构造带错误信息和根因的转换异常
   * @param message 错误描述信息
   * @param cause 原始异常根因
   */
  public ConversionException(String message, Throwable cause) {
    super(message, cause);
  }
}