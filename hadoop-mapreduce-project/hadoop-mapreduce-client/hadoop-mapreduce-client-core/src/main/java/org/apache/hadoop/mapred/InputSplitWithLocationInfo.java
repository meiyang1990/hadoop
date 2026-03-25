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

/**
 * @file InputSplitWithLocationInfo.java
 * Hadoop MapReduce 输入分片位置信息扩展接口，提供分片存储位置的详细信息
 */
package org.apache.hadoop.mapred;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;

/**
 * 带存储位置详细信息的输入分片接口
 * 扩展基础InputSplit，允许获取每个存储位置上分片数据的存储详情，支持优化任务本地化调度
 * 用于MapReduce任务调度时，判断数据存储介质（内存/磁盘），帮助调度器做出更优的本地化决策
 */
@Public
@Evolving
public interface InputSplitWithLocationInfo extends InputSplit {
  /**
   * 获取输入分片在各个节点的存储位置详情
   * @return 包含每个位置分片存储信息的SplitLocationInfo数组，null表示所有位置数据都存储在磁盘
   * @throws IOException 获取位置信息失败时抛出IO异常
   */
  SplitLocationInfo[] getLocationInfo() throws IOException;
}