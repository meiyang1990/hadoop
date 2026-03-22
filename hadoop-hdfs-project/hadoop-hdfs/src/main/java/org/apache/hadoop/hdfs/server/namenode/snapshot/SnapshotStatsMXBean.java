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
package org.apache.hadoop.hdfs.server.namenode.snapshot;

import org.apache.hadoop.hdfs.protocol.SnapshotInfo;
import org.apache.hadoop.hdfs.protocol.SnapshottableDirectoryStatus;

/**
 * 快照统计信息的JMX管理接口，用于暴露HDFS快照相关统计信息给JMX监控
 */
public interface SnapshotStatsMXBean {

  /**
   * 获取所有支持快照功能的目录列表
   *
   * @return 支持快照的目录状态Bean数组
   */
  public SnapshottableDirectoryStatus.Bean[] getSnapshottableDirectories();

  /**
   * 获取当前所有已创建的快照列表
   *
   * @return 快照信息Bean数组
   */
  public SnapshotInfo.Bean[] getSnapshots();

}