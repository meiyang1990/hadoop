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

/**
 * HDFS命名空间内容统计类型枚举，定义了名称节点空间统计需要计算的不同统计维度。
 * 用于目录容量、文件数量等空间统计场景，支持对不同类型的对象分别计数。
 */
public enum Content {
  /** 文件数量统计 */
  FILE,
  /** 目录数量统计 */
  DIRECTORY,
  /** 符号链接数量统计 */
  SYMLINK,

  /** 文件总长度统计，单位：字节 */
  LENGTH,
  /** 磁盘空间总使用量统计，包含副本占用空间，单位：字节 */
  DISKSPACE,

  /** 快照数量统计 */
  SNAPSHOT,
  /** 可快照目录数量统计 */
  SNAPSHOTTABLE_DIRECTORY;
}