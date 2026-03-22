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
package org.apache.hadoop.hdfs.protocol;

import java.io.IOException;

/**
 * HDFS快照操作异常类，封装所有与HDFS快照相关的操作异常。
 * 当快照创建、删除、重命名等操作出现错误时抛出该异常。
 */
public class SnapshotException extends IOException {
  private static final long serialVersionUID = 1L;

  /**
   * 构造带错误消息的快照异常
   * @param message 异常描述信息
   */
  public SnapshotException(final String message) {
    super(message);
  }

  /**
   * 构造包装底层异常的快照异常
   * @param cause 原始异常对象
   */
  public SnapshotException(final Throwable cause) {
    super(cause);
  }

  /**
   * 构造带错误消息且包装底层异常的快照异常
   * @param message 异常描述信息
   * @param cause 原始异常对象
   */
  public SnapshotException(final String message, final Throwable cause) {
    super(message, cause);
  }
}