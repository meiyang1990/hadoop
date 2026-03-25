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
import org.apache.hadoop.hdfs.protocol.RollingUpgradeStatus;

@InterfaceAudience.Private
@InterfaceStability.Evolving
/**
 * 数据节点心跳请求的响应类，封装NameNode返回给DataNode的响应信息
 * 用于DatanodeProtocol#sendHeartbeat调用的返回结果
 */
public class HeartbeatResponse {
  /** NameNode返回给DataNode的执行命令数组 */
  private final DatanodeCommand[] commands;
  
  /** NameNode当前的HA状态信息 */
  private final NNHAStatusHeartbeat haStatus;

  /** 滚动升级状态信息 */
  private final RollingUpgradeStatus rollingUpdateStatus;

  /** 全量块报告租约ID，用于限流全量块报告 */
  private final long fullBlockReportLeaseId;

  /** 标识该节点是否被标记为慢节点 */
  private final boolean isSlownode;

  /**
   * 构造心跳响应对象（默认非慢节点）
   * @param cmds NameNode下发给DataNode的命令数组
   * @param haStatus NameNode HA状态
   * @param rollingUpdateStatus 滚动升级状态
   * @param fullBlockReportLeaseId 全量块报告租约ID
   */
  public HeartbeatResponse(DatanodeCommand[] cmds,
      NNHAStatusHeartbeat haStatus, RollingUpgradeStatus rollingUpdateStatus,
      long fullBlockReportLeaseId) {
    this(cmds, haStatus, rollingUpdateStatus, fullBlockReportLeaseId, false);
  }

  /**
   * 完整构造心跳响应对象
   * @param cmds NameNode下发给DataNode的命令数组
   * @param haStatus NameNode HA状态
   * @param rollingUpdateStatus 滚动升级状态
   * @param fullBlockReportLeaseId 全量块报告租约ID
   * @param isSlownode 是否标记该节点为慢节点
   */
  public HeartbeatResponse(DatanodeCommand[] cmds,
      NNHAStatusHeartbeat haStatus, RollingUpgradeStatus rollingUpdateStatus,
      long fullBlockReportLeaseId, boolean isSlownode) {
    commands = cmds;
    this.haStatus = haStatus;
    this.rollingUpdateStatus = rollingUpdateStatus;
    this.fullBlockReportLeaseId = fullBlockReportLeaseId;
    this.isSlownode = isSlownode;
  }
  
  /**
   * 获取NameNode下发的命令数组
   * @return DataNode命令数组
   */
  public DatanodeCommand[] getCommands() {
    return commands;
  }
  
  /**
   * 获取NameNode的HA状态信息
   * @return HA状态对象
   */
  public NNHAStatusHeartbeat getNameNodeHaState() {
    return haStatus;
  }

  /**
   * 获取滚动升级状态信息
   * @return 滚动升级状态对象
   */
  public RollingUpgradeStatus getRollingUpdateStatus() {
    return rollingUpdateStatus;
  }

  /**
   * 获取全量块报告租约ID
   * @return 全量块报告租约ID
   */
  public long getFullBlockReportLeaseId() {
    return fullBlockReportLeaseId;
  }

  /**
   * 获取该节点是否被标记为慢节点
   * @return true表示该节点是慢节点，false表示正常节点
   */
  public boolean getIsSlownode() {
    return isSlownode;
  }
}