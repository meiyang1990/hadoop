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
 * BPServiceActor执行操作异常类
 * 用于表示DataNode与NameNode心跳/注册等服务交互过程中执行操作失败的异常情况
 */
public class BPServiceActorActionException extends IOException {

/**
 * An exception for BPSerivceActor call related issues
 */
  private static final long serialVersionUID = 1L;

  /**
   * 构造带错误信息的异常实例
   * @param message 错误信息
   */
  public BPServiceActorActionException(String message) {
    super(message);
  }

  /**
   * 构造带错误信息和根因的异常实例
   * @param message 错误信息
   * @param cause 原始异常根因
   */
  public BPServiceActorActionException(String message, Throwable cause) {
    super(message, cause);
  }

}