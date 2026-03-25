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
 * Unless required by applicable law or agreed to writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.blockmanagement;

import java.util.Map;

import org.apache.hadoop.fs.StorageType;

/**
 * HDFS块管理模块JMX统计接口
 * 用于暴露块管理相关的统计信息，供JMX监控采集使用
 */
public interface BlockStatsMXBean {

  /**
   * 获取各存储类型的统计信息
   * @return 按存储类型分类的存储统计结果映射表
   */
  Map<StorageType, StorageTypeStats> getStorageTypeStats();
}