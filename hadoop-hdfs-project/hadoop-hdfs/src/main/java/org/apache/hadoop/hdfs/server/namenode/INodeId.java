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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.util.SequentialNumber;

/**
 * HDFS INode ID生成器，负责生成全局唯一的INode标识。
 * 1~16384为保留ID预留给未来使用，ID不会回收重复使用，长时间运行也不会产生溢出问题。
 * 根目录INode ID固定为16385，0用于向后兼容旧版本数据。
 */
@InterfaceAudience.Private
public class INodeId extends SequentialNumber {
  /**
   * 最后一个保留的INode ID，正式分配的INode ID从LAST_RESERVED_ID + 1开始。
   */
  public static final long LAST_RESERVED_ID = 1 << 14; // 16384
  public static final long ROOT_INODE_ID = LAST_RESERVED_ID + 1; // 16385
  public static final long INVALID_INODE_ID = -1;

  /**
   * 构造INode ID生成器，从根INode ID开始顺序生成。
   */
  INodeId() {
    super(ROOT_INODE_ID);
  }
}