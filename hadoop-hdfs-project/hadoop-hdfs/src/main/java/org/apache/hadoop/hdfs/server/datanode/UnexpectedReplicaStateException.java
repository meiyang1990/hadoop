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

import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.ReplicaState;

/**
 * 数据节点副本状态异常类，当数据块副本处于预期之外的状态时抛出该异常
 * 用于在数据节点副本管理过程中标记状态不匹配错误
 */
public class UnexpectedReplicaStateException extends IOException {
  private static final long serialVersionUID = 1L;

  /**
   * 构造无信息的异常实例
   */
  public UnexpectedReplicaStateException() {
    super();
  }

  /**
   * 构造包含块信息和预期状态的异常实例
   * @param b 发生状态异常的数据块
   * @param expectedState 预期的副本状态
   */
  public UnexpectedReplicaStateException(ExtendedBlock b,
      ReplicaState expectedState) {
    super("Replica " + b + " is not in expected state " + expectedState);
  }
  
  /**
   * 构造包含自定义错误信息的异常实例
   * @param msg 自定义错误信息
   */
  public UnexpectedReplicaStateException(String msg) {
    super(msg);
  }
}