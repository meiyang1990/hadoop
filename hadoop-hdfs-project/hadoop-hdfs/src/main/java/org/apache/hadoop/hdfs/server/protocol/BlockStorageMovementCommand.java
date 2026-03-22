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

import java.util.Collection;

import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;

/**
 * 文件说明：HDFS数据节点块存储移动命令，定义NameNode向DataNode下发的块移动指令，用于满足块存储策略要求。
 * 
 * 核心作用：封装需要移动的块列表和目标信息，由NameNode发给目标DataNode，驱动外部存储策略满足服务(SPS)执行块移动任务。
 * DataNode收到命令后会转交外部SPS处理器调度任务，任务完成后将结果回传给NameNode。
 */
public class BlockStorageMovementCommand extends DatanodeCommand {
  private final String blockPoolId;
  private final Collection<BlockMovingInfo> blockMovingTasks;

  /**
   * 构造块存储移动命令对象
   * @param action 协议指定的动作类型
   * @param blockPoolId 块池ID
   * @param blockMovingInfos 需要执行移动的块信息集合
   */
  public BlockStorageMovementCommand(int action, String blockPoolId,
      Collection<BlockMovingInfo> blockMovingInfos) {
    super(action);
    this.blockPoolId = blockPoolId;
    this.blockMovingTasks = blockMovingInfos;
  }

  /**
   * 获取命令所属块池ID
   * @return 块池ID
   */
  public String getBlockPoolId() {
    return blockPoolId;
  }

  /**
   * 获取所有需要移动的块任务信息集合
   * @return 待移动块信息集合
   */
  public Collection<BlockMovingInfo> getBlockMovingTasks() {
    return blockMovingTasks;
  }

  /**
   * 单个块移动信息类，存储单个块的源、目标位置和存储类型信息，用于块移动任务执行。
   */
  public static class BlockMovingInfo {
    private Block blk;
    private DatanodeInfo sourceNode;
    private DatanodeInfo targetNode;
    private StorageType sourceStorageType;
    private StorageType targetStorageType;

    /**
     * 构造单个块移动信息对象
     * @param block 需要移动的块
     * @param sourceDnInfo 源数据节点
     * @param targetDnInfo 目标数据节点
     * @param srcStorageType 源存储介质类型
     * @param targetStorageType 目标存储介质类型
     */
    public BlockMovingInfo(Block block, DatanodeInfo sourceDnInfo,
        DatanodeInfo targetDnInfo, StorageType srcStorageType,
        StorageType targetStorageType) {
      this.blk = block;
      this.sourceNode = sourceDnInfo;
      this.targetNode = targetDnInfo;
      this.sourceStorageType = srcStorageType;
      this.targetStorageType = targetStorageType;
    }

    public void addBlock(Block block) {
      this.blk = block;
    }

    public Block getBlock() {
      return blk;
    }

    public DatanodeInfo getSource() {
      return sourceNode;
    }

    public DatanodeInfo getTarget() {
      return targetNode;
    }

    public StorageType getTargetStorageType() {
      return targetStorageType;
    }

    public StorageType getSourceStorageType() {
      return sourceStorageType;
    }

    @Override
    public String toString() {
      return new StringBuilder().append("BlockMovingInfo(\n  ")
          .append("Moving block: ").append(blk).append(" From: ")
          .append(sourceNode).append(" To: [").append(targetNode).append("\n  ")
          .append(" sourceStorageType: ").append(sourceStorageType)
          .append(" targetStorageType: ").append(targetStorageType).append(")")
          .toString();
    }
  }
}