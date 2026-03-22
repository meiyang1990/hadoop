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

package org.apache.hadoop.hdfs.server.namenode;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * 租约过期异常，当创建文件时使用的租约已经过期时抛出此异常
 * 用于表示HDFS租约管理中，客户端持有的文件写租约超时失效
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class LeaseExpiredException extends IOException {
  private static final long serialVersionUID = 1L;

  /**
   * 构造带有指定错误消息的租约过期异常
   * @param msg 异常错误信息
   */
  public LeaseExpiredException(String msg) {
    super(msg);
  }
}