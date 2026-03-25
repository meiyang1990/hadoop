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

package org.apache.hadoop.hdfs.server.datanode;

import java.io.IOException;

/**
 * HDFS数据节点中，数据块副本已存在且不允许覆盖/恢复时抛出的异常
 * 当尝试创建一个已存在且未标记为可恢复的副本时触发该异常
 */
public class ReplicaAlreadyExistsException extends IOException {
  private static final long serialVersionUID = 1L;

  /**
   * 构造无消息的副本已存在异常
   */
  public ReplicaAlreadyExistsException() {
    super();
  }

  /**
   * 构造带指定错误信息的副本已存在异常
   * @param msg 异常描述信息
   */
  public ReplicaAlreadyExistsException(String msg) {
    super(msg);
  }
}