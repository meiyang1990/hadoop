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
 * 文件路径：hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/datanode/DiskFileCorruptException.java
 * <p>
 * 磁盘文件损坏异常，当操作系统内核返回IO错误时抛出，
 * 用于标识数据节点磁盘文件发生了物理损坏（比如坏道、磁盘损坏等情况）
 * </p>
 * When kernel report a "Input/output error", we use this exception to
 * represents some corruption(e.g. bad disk track) happened on some disk file.
 */
public class DiskFileCorruptException extends IOException {
  /**
   * 构造磁盘文件损坏异常实例
   * @param msg 异常描述信息
   * @param cause 底层原始异常
   */
  public DiskFileCorruptException(String msg, Throwable cause) {
    super(msg, cause);
  }

  /**
   * 构造仅带消息的磁盘文件损坏异常实例
   * @param msg 异常描述信息
   */
  public DiskFileCorruptException(String msg) {
    super(msg);
  }
}