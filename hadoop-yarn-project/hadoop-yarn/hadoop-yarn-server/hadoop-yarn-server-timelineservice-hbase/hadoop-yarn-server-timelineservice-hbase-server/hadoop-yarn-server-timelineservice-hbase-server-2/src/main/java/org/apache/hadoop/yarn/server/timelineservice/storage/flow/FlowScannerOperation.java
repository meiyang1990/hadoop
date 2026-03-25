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
package org.apache.hadoop.yarn.server.timelineservice.storage.flow;

/**
 * 流运行表FlowRunTable上的扫描器操作类型枚举，标识HBase协处理器触发扫描的场景
 */
public enum FlowScannerOperation {

  /**
   * 扫描器用于读取操作，在preGet或preScan阶段打开
   */
  READ,

  /**
   * 扫描器用于刷写操作，在preFlush阶段打开
   */
  FLUSH,

  /**
   * 扫描器用于Minor Compaction（小合并）阶段打开
   */
  MINOR_COMPACTION,

  /**
   * 扫描器用于Major Compaction（大合并）阶段打开
   */
  MAJOR_COMPACTION
}