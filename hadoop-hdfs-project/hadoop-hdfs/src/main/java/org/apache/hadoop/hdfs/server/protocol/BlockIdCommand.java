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

/**
 * 基于块ID的DataNode指令，用于NameNode向DataNode下发针对特定数据块的操作指令
 * 是HDFS NameNode与DataNode之间节点通信协议的核心消息类
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class BlockIdCommand extends DatanodeCommand {
  /** 块池ID，标识该指令所属的块池 */
  final String poolId;
  /** 指令操作目标的数据块ID数组 */
  final long blockIds[];

  /**
   * 构造携带块ID列表的DataNode指令
   * @param action 指令操作类型
   * @param poolId 目标块池ID
   * @param blockIds 目标数据块ID数组
   */
  public BlockIdCommand(int action, String poolId, long[] blockIds) {
    super(action);
    this.poolId = poolId;
    this.blockIds= blockIds;
  }
  
  /**
   * 获取目标块池ID
   * @return 块池ID
   */
  public String getBlockPoolId() {
    return poolId;
  }
  
  /**
   * 获取操作目标的数据块ID数组
   * @return 数据块ID数组
   */
  public long[] getBlockIds() {
    return blockIds;
  }
}