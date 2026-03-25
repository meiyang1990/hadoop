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
package org.apache.hadoop.mapred;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * MapReduce输入分片索引记录，存储分片在文件中的位置和长度信息
 * 用于旧版MapReduce API中定位分片数据的物理存储位置
 */
@InterfaceAudience.LimitedPrivate({"MapReduce"})
@InterfaceStability.Unstable
public class IndexRecord {
  // 分片在文件中的起始偏移量
  public long startOffset;
  // 未压缩的原始分片长度
  public long rawLength;
  // 实际存储的分片长度（压缩后为压缩大小，未压缩与rawLength相同）
  public long partLength;

  /**
   * 构造空索引记录
   */
  public IndexRecord() { }

  /**
   * 构造带完整信息的索引记录
   * @param startOffset 分片起始偏移量
   * @param rawLength 原始未压缩分片长度
   * @param partLength 实际存储分片长度
   */
  public IndexRecord(long startOffset, long rawLength, long partLength) {
    this.startOffset = startOffset;
    this.rawLength = rawLength;
    this.partLength = partLength;
  }
}