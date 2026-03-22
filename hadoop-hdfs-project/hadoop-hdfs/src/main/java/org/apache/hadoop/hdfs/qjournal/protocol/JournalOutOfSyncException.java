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
package org.apache.hadoop.hdfs.qjournal.protocol;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;

/**
 * QJournal日志不同步异常类
 * 当共享编辑日志节点的日志状态与请求要求的状态不一致时，抛出此异常
 * 用于HDFS QJournal联邦协议中标识日志同步失败的错误场景
 */
@InterfaceAudience.Private
public class JournalOutOfSyncException extends IOException {
  private static final long serialVersionUID = 1L;
  
  /**
   * 构造函数，创建带错误信息的异常实例
   * @param msg 错误描述信息
   */
  public JournalOutOfSyncException(String msg) {
    super(msg);
  }

}