// 这个文件已经全部加上中文注释
/*
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

package org.apache.hadoop.mapreduce;

import org.apache.hadoop.classification.InterfaceAudience;

/**
 * 文件系统操作计数器枚举，定义了MapReduce任务中统计文件系统I/O操作的各类指标
 * 用于收集和监控MapReduce作业运行过程中文件系统的读写流量和操作次数
 */
@InterfaceAudience.Private
public enum FileSystemCounter {
  /** 总读取字节数 */
  BYTES_READ,
  /** 总写入字节数 */
  BYTES_WRITTEN,
  /** 总读操作次数 */
  READ_OPS,
  /** 大规模读操作次数 */
  LARGE_READ_OPS,
  /** 总写操作次数 */
  WRITE_OPS,
  /** 纠删码模式下总读取字节数 */
  BYTES_READ_EC,
}