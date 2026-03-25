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
package org.apache.hadoop.yarn.server.timelineservice.storage.common;

/**
 * HBase行键前缀接口，用于支持范围扫描查询。
 * 需要精确匹配单条结果时使用完整行键，需要扫描某一范围行键时使用行键前缀（行键的起始部分）。
 * 实现类通过使用更少参数的构造器生成不同长度的行键前缀。
 *
 * @param <R> 该前缀对应的完整行键类型
 */
public interface RowKeyPrefix<R> {

  /**
   * 生成用于范围扫描的行键前缀字节数组。
   * 前缀包含的字段由构造实例时使用的构造器决定，输出格式为 {@code first!second!...!last!}。
   * @return 行键前缀字节数组
   */
  byte[] getRowKeyPrefix();

}