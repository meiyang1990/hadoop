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

import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor.BlockTargetPair;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStorageInfo;

/**
 * 文件级注释：HDFS节点间通信块命令定义，NameNode向DataNode发送块操作指令的数据结构
 * 
 ****************************************************
 * A BlockCommand is an instruction to a datanode 
 * regarding some blocks under its control.  It tells
 * the DataNode to either invalidate a set of indicated
 * blocks, or to copy a set of indicated blocks to 
 * another DataNode.
 * 
 ****************************************************/
@InterfaceAudience.Private
@InterfaceStability.Evolving
/**
 * 块操作命令类，封装NameNode发给DataNode的块相关操作指令
 * 核心职责：在NameNode与DataNode的心跳响应中传递块操作任务，支持块删除、块复制等操作
 */
public class BlockCommand extends DatanodeCommand {
  
  /**
   * 该常量用于标记不需要DataNode返回确认ACK的块删除操作
   * 将待删除块的大小设为此值，表示不需要ACK，利用了实际块不会有该大小的假设保证兼容性
   */
  public static final long NO_ACK = Long.MAX_VALUE;
  
  final String poolId;
  final Block[] blocks;
  final DatanodeInfo[][] targets;
  final StorageType[][] targetStorageTypes;
  final String[][] targetStorageIDs;

  /**
   * 构造块传输命令，用于向DataNode下发将指定块复制到其他目标DataNode的指令
   * @param action 操作类型
   * @param poolId 块池ID
   * @param blocktargetlist 需要传输的块及其目标节点列表
   */
  public BlockCommand(int action, String poolId,
      List<BlockTargetPair> blocktargetlist) {
    super(action);
    this.poolId = poolId;
    // 初始化块数组
    blocks = new Block[blocktargetlist.size()]; 
    // 初始化目标节点数组
    targets = new DatanodeInfo[blocks.length][];
    // 初始化目标存储类型数组
    targetStorageTypes = new StorageType[blocks.length][];
    // 初始化目标存储ID数组
    targetStorageIDs = new String[blocks.length][];

    // 遍历填充所有块和目标信息
    for(int i = 0; i < blocks.length; i++) {
      BlockTargetPair p = blocktargetlist.get(i);
      blocks[i] = p.block;
      targets[i] = DatanodeStorageInfo.toDatanodeInfos(p.targets);
      targetStorageTypes[i] = DatanodeStorageInfo.toStorageTypes(p.targets);
      targetStorageIDs[i] = DatanodeStorageInfo.toStorageIDs(p.targets);
    }
  }

  // 空目标节点数组常量，用于无目标的操作（如删除）
  private static final DatanodeInfo[][] EMPTY_TARGET_DATANODES = {};
  // 空存储类型数组常量
  private static final StorageType[][] EMPTY_TARGET_STORAGE_TYPES = {};
  // 空存储ID数组常量
  private static final String[][] EMPTY_TARGET_STORAGEIDS = {};

  /**
   * 构造无目标节点的块命令，用于块失效/删除操作
   * @param action 操作类型
   * @param poolId 块池ID
   * @param blocks 本次操作涉及的块数组
   */
  public BlockCommand(int action, String poolId, Block blocks[]) {
    this(action, poolId, blocks, EMPTY_TARGET_DATANODES,
        EMPTY_TARGET_STORAGE_TYPES, EMPTY_TARGET_STORAGEIDS);
  }

  /**
   * 全参数构造块命令，支持自定义目标节点信息
   * @param action 操作类型
   * @param poolId 块池ID
   * @param blocks 本次操作涉及的块数组
   * @param targets 目标DataNode节点二维数组
   * @param targetStorageTypes 目标存储类型二维数组
   * @param targetStorageIDs 目标存储ID二维数组
   */
  public BlockCommand(int action, String poolId, Block[] blocks,
      DatanodeInfo[][] targets, StorageType[][] targetStorageTypes,
      String[][] targetStorageIDs) {
    super(action);
    this.poolId = poolId;
    this.blocks = blocks;
    this.targets = targets;
    this.targetStorageTypes = targetStorageTypes;
    this.targetStorageIDs = targetStorageIDs;
  }
  
  /**
   * 获取块池ID
   * @return 所属块池ID
   */
  public String getBlockPoolId() {
    return poolId;
  }
  
  /**
   * 获取本次操作涉及的块数组
   * @return 块数组
   */
  public Block[] getBlocks() {
    return blocks;
  }

  /**
   * 获取每个块对应的目标DataNode节点数组
   * @return 二维目标DataNode信息数组，第一维对应块索引，第二维对应该块的多个目标
   */
  public DatanodeInfo[][] getTargets() {
    return targets;
  }

  /**
   * 获取每个块对应目标的存储类型数组
   * @return 二维目标存储类型数组
   */
  public StorageType[][] getTargetStorageTypes() {
    return targetStorageTypes;
  }

  /**
   * 获取每个块对应目标的存储ID数组
   * @return 二维目标存储ID数组
   */
  public String[][] getTargetStorageIDs() {
    return targetStorageIDs;
  }
}