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
import org.apache.hadoop.mapreduce.JobCounter;

/**
 * Hadoop MapReduce旧版本API中的作业进度信息类，保留旧版作业计数器枚举以保持向后兼容性
 * 核心职责：为老版本客户端提供兼容支持，新代码应使用org.apache.hadoop.mapreduce.JobCounter
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class JobInProgress {
  
  /**
   * 已废弃的作业计数器枚举，用于兼容旧版API，新代码请使用{@link JobCounter}
   * 定义了MapReduce作业运行过程中各类统计指标的计数器类型
   */
  @Deprecated
  public enum Counter {
    NUM_FAILED_MAPS, 
    NUM_FAILED_REDUCES,
    TOTAL_LAUNCHED_MAPS,
    TOTAL_LAUNCHED_REDUCES,
    OTHER_LOCAL_MAPS,
    DATA_LOCAL_MAPS,
    RACK_LOCAL_MAPS,
    SLOTS_MILLIS_MAPS,
    SLOTS_MILLIS_REDUCES,
    FALLOW_SLOTS_MILLIS_MAPS,
    FALLOW_SLOTS_MILLIS_REDUCES
  }

}