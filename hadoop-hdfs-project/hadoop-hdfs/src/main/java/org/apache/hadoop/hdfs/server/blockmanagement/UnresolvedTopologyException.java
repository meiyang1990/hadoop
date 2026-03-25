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

package org.apache.hadoop.hdfs.server.blockmanagement;

import java.io.IOException;

/**
 * HDFS块管理模块中，节点网络拓扑路径解析失败时抛出的异常
 * 当无法根据节点地址解析出对应的网络拓扑位置时抛出此异常
 */
public class UnresolvedTopologyException extends IOException {
  /** 序列化版本ID，用于Java序列化机制 */
  private static final long serialVersionUID = 1L;
  
  /**
   * 构造一个包含指定错误信息的UnresolvedTopologyException异常
   * @param text 异常的错误描述信息
   */
  public UnresolvedTopologyException(String text) {
    super(text);
  }
}