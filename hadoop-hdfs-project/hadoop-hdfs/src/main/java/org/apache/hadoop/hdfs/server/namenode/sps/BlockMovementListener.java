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

package org.apache.hadoop.hdfs.server.namenode.sps;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.protocol.Block;

/**
 * 存储策略满足器(SPS)的块移动完成监听器接口，用于接收块移动尝试完成的通知。
 * 当数据块移动尝试完成后，通过该接口通知SPS核心模块进行后续处理（重试或标记完成）。
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public interface BlockMovementListener {

  /**
   * 通知SPS模块一批数据块的移动尝试已完成，由SPS模块判断是否需要对失败块进行重试。
   *
   * @param moveAttemptFinishedBlks 已完成移动尝试的数据块列表（无论成功失败，均属于已尝试完成）
   */
  void notifyMovementTriedBlocks(Block[] moveAttemptFinishedBlks);
}