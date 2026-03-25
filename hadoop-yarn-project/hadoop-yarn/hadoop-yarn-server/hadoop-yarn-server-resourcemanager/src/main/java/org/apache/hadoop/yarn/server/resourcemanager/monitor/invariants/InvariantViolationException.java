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
package org.apache.hadoop.yarn.server.resourcemanager.monitor.invariants;


import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;

/**
 * YARN ResourceManager 不变量检查异常，表示内部系统不变量被违反。
 * 用于资源监控模块中，当检测到不符合集群资源约束的非法状态时抛出。
 */
public class InvariantViolationException extends YarnRuntimeException {

  /**
   * 构造带错误信息的异常实例
   * @param s 错误描述信息
   */
  public InvariantViolationException(String s) {
    super(s);
  }

  /**
   * 构造带错误信息和根因异常的异常实例
   * @param s 错误描述信息
   * @param e 根因异常
   */
  public InvariantViolationException(String s, Exception e) {
    super(s, e);
  }
}