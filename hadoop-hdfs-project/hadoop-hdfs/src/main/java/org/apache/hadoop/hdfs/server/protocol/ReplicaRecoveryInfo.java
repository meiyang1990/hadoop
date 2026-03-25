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

package org.apache.hadoop.hdfs.server.protocol;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.ReplicaState;

/**
 * @file 文件说明：HDFS数据块副本恢复信息，用于恢复未完成的块写入操作
 * 核心职责：封装块恢复过程中所需的元数据，包括块基础信息和恢复前的副本状态
 * 使用场景：NameNode与DataNode之间进行块恢复（如恢复失败的写入操作）时传递信息
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class ReplicaRecoveryInfo extends Block {
  // 副本恢复前的原始状态
  private final ReplicaState originalState;

  /**
   * 构造副本恢复信息对象
   * @param blockId 数据块ID
   * @param diskLen 磁盘上存储的副本长度
   * @param gs 数据块生成时间戳（版本号）
   * @param rState 副本原始状态
   */
  public ReplicaRecoveryInfo(long blockId, long diskLen, long gs, ReplicaState rState) {
    set(blockId, diskLen, gs);
    originalState = rState;
  }

  /**
   * 获取副本恢复前的原始状态
   * @return 副本原始状态枚举值
   */
  public ReplicaState getOriginalReplicaState() {
    return originalState;
  }

  @Override
  public boolean equals(Object o) {
    return super.equals(o);
  }

  @Override
  public int hashCode() {
    return super.hashCode();
  }

  @Override
  public String toString() {
    return super.toString() + "[numBytes=" + this.getNumBytes() +
        ",originalReplicaState=" + this.originalState.name() + "]";
  }
}