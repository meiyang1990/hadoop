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
package org.apache.hadoop.mapreduce.checkpoint;

/**
 * MapReduce任务检查点机制的统计计数器枚举
 * 用于定义检查点相关的各类统计指标，帮助监控检查点操作的性能和数据量
 */
public enum EnumCounter {
  /** 输入Key计数 */
  INPUTKEY,
  /** 输入Value计数 */
  INPUTVALUE,
  /** 输出记录计数 */
  OUTPUTRECORDS,
  /** 检查点写入字节数 */
  CHECKPOINT_BYTES,
  /** 检查点操作耗时（毫秒） */
  CHECKPOINT_MS
}