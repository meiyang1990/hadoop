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
package org.apache.hadoop.hdfs.server.common;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.util.SequentialNumber;

/**
 * HDFS文件系统核心基础原语，用于生成单调递增的世代时间戳（Generation Stamp）
 * 该时间戳用于标识HDFS中数据块、inode等对象的版本变化，支持版本识别和冲突检测
 */
@InterfaceAudience.Private
public class GenerationStamp extends SequentialNumber {
  /**
   * 最后一个被保留的预定义世代时间戳，所有实际使用的时间戳都大于该值
   */
  public static final long LAST_RESERVED_STAMP = 1000L;

  /**
   * 构造函数，初始化GenerationStamp，起始值为保留的最大预定义时间戳
   * 后续每次递增都会生成新的可用世代时间戳
   */
  public GenerationStamp() {
    super(LAST_RESERVED_STAMP);
  }
}