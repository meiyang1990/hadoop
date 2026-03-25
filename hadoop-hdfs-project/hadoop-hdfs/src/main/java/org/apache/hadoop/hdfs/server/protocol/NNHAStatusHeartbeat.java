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
import org.apache.hadoop.ha.HAServiceProtocol.HAServiceState;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;

/**
 * NameNode高可用状态心跳信息类，用于Standby节点向Active节点同步自身状态，
 * 支持高可用架构下的状态监控与脑裂防护。
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class NNHAStatusHeartbeat {

  // 当前NameNode的高可用服务状态
  private final HAServiceState state;
  // 当前已处理的最大事务ID，初始为无效值表示未处理任何事务
  private long txid = HdfsServerConstants.INVALID_TXID;
  
  /**
   * 构造高可用状态心跳对象
   * @param state 当前NameNode的高可用状态
   * @param txid 当前已处理的最大事务ID
   */
  public NNHAStatusHeartbeat(HAServiceState state, long txid) {
    this.state = state;
    this.txid = txid;
  }

  /**
   * 获取当前NameNode的高可用服务状态
   * @return 高可用服务状态对象
   */
  public HAServiceState getState() {
    return state;
  }
  
  /**
   * 获取当前已处理的最大事务ID
   * @return 事务ID，若为无效值表示未处理事务
   */
  public long getTxId() {
    return txid;
  }
}