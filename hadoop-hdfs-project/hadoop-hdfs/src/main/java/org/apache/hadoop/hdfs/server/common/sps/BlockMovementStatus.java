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

package org.apache.hadoop.hdfs.server.common.sps;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * 存储策略满足性(Storage Policy Satisfier, SPS)服务中块移动操作的状态枚举，
 * 用于标识数据块在DataNode节点间移动的执行结果状态。
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public enum BlockMovementStatus {
  /** 块移动操作成功完成 */
  DN_BLK_STORAGE_MOVEMENT_SUCCESS(0),
  /**
   * 块移动操作失败，失败原因可能包括：块时间戳不匹配、网络错误、目标节点无可用存储空间等
   */
  DN_BLK_STORAGE_MOVEMENT_FAILURE(-1);

  // TODO: need to support different type of failures. Failure due to network
  // errors, block pinned, no space available etc.

  private final int code;

  BlockMovementStatus(int code) {
    this.code = code;
  }

  /**
   * 获取当前状态对应的整型状态码
   * @return 整型状态编码
   */
  int getStatusCode() {
    return code;
  }
}