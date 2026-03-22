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

package org.apache.hadoop.mapred.nativetask.buffer;

import org.apache.hadoop.classification.InterfaceAudience;

/**
 * 本地任务缓冲区类型枚举，定义了MapReduce本地任务可用的内存缓冲区类型
 */
@InterfaceAudience.Private
public enum BufferType {
  /** 直接内存缓冲区，使用堆外内存分配 */
  DIRECT_BUFFER,
  /** 堆缓冲区，使用JVM堆内存分配 */
  HEAP_BUFFER
};