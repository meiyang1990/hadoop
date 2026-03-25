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

package org.apache.hadoop.mapred.nativetask;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * 本地任务排序中需要在C++本地层进行比较的键类型，必须实现该接口。
 * 
 * 本地比较函数需要满足如下函数签名格式：
 * <code>
 *   typedef int (*ComparatorPtr)(const char * src, uint32_t srcLength,
 *   const char * dest,  uint32_t destLength);
 * </code>
 * 本地层中键以序列化格式存储，比较函数会传入两个键的内存地址和长度，
 * 可以实现与Java层比较逻辑一致的比较操作。
 * 
 * 例如HiveKey序列化为：int字段（存储原始字节长度） + 原始字节。
 * 比较两个HiveKey时，先读取长度字段，再调用库提供的BytesComparator比较原始字节，
 * 将原始字节的地址和长度传入BytesComparator完成比较。
 * 
 * <code>
 *   int HivePlatform::HiveKeyComparator(const char * src, uint32_t srcLength,
 *   const char * dest, uint32_t destLength) {
 *     uint32_t sl = bswap(*(uint32_t*)src);
 *     uint32_t dl = bswap(*(uint32_t*)dest);
 *     return NativeObjectFactory::BytesComparator(src + 4, sl, dest + 4, dl);
 *   }
 * </code>
 * 
 * 该接口用于标识可被本地任务框架直接在native层进行排序比较的键类型，
 * 支持nativetask加速排序过程，避免Java与native层之间的反序列化开销。
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface INativeComparable {
}