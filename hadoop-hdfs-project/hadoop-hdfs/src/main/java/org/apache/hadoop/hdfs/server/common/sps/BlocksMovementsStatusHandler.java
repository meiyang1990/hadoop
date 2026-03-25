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
 * 文件：BlocksMovementsStatusHandler.java
 * 所属模块：HDFS核心服务端，存储策略满足性(Storage Policy Satisfier, SPS)子系统
 * 核心职责：定义块移动状态处理接口，用于收集已完成的块移动操作的详细信息，支持不同实现对移动结果进行自定义处理
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public interface BlocksMovementsStatusHandler {

  /**
   * 处理单个已完成移动尝试的块，将其纳入状态收集或后续处理流程
   * 用于SPS系统完成块移动尝试后，回调通知处理移动结果
   *
   * @param moveAttemptFinishedBlk 已完成移动尝试的块信息
   */
  void handle(BlockMovementAttemptFinished moveAttemptFinishedBlk);
}