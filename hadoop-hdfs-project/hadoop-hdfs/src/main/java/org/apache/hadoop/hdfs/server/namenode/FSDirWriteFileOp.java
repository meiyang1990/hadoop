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
package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.util.Preconditions;
import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.fs.XAttrSetFlag;
import org.apache.hadoop.hdfs.AddBlockFlag;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileEncryptionInfo;
import org.apache.hadoop.fs.XAttr;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.FSLimitException;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.QuotaExceededException;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguous;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoStriped;

import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.protocol.BlockType;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockUnderConstructionFeature;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStorageInfo;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.namenode.FSDirectory.DirOp;
import org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot;
import org.apache.hadoop.hdfs.util.RwLockMode;
import org.apache.hadoop.io.erasurecode.ErasureCodeConstants;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.net.NodeBase;
import org.apache.hadoop.util.ChunkedArrayList;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot.CURRENT_STATE_ID;
import static org.apache.hadoop.util.Time.now;

/**
 * HDFS NameNode写文件操作工具类，封装文件创建、追加块、完成文件等写操作的核心逻辑
 * 所有方法均为静态工具方法，不允许实例化，为NameNode文件系统写操作提供分层实现
 */
class FSDirWriteFileOp {
  private FSDirWriteFileOp() {}

  /**
   * 从文件中移除指定块，更新命名空间和配额统计
   * @param fsd 目录管理器
   * @param path 文件路径
   * @param iip 路径上的INode列表
   * @param fileNode 目标文件INode
   * @param block 要移除的块
   * @return 移除成功返回true，块不存在返回false
   * @throws IOException 移除过程中发生IO异常
   */
  static boolean unprotectedRemoveBlock(
      FSDirectory fsd, String path, INodesInPath iip, INodeFile fileNode,
      Block block) throws IOException {
    // 从文件INode中移除最后一个块
    BlockInfo uc = fileNode.removeLastBlock(block);
    if (uc == null) {
      return false;
    }
    // 递减DataNode计划存储块计数
    if (uc.getUnderConstructionFeature() != null) {
      DatanodeStorageInfo.decrementBlocksScheduled(uc
          .getUnderConstructionFeature().getExpectedStorageLocations());
    }
    // 从块管理器中移除该块
    fsd.getBlockManager().removeBlockFromMap(uc);

    if(NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("DIR* FSDirectory.removeBlock: "
          +path+" with "+block
          +" block is removed from the file system");
    }

    // 更新目录配额和空间消耗统计
    fsd.updateCount(iip, 0, -fileNode.getPreferredBlockSize(),
        fileNode.getPreferredBlockReplication(), true);
    return true;
  }

  /**
   * 将文件的块列表持久化到编辑日志
   * @param fsd 目录管理器
   * @param path 文件路径
   * @param file 目标文件INode
   * @param logRetryCache 是否记录到重试缓存
   */
  static void persistBlocks(
      FSDirectory fsd, String path, INodeFile file, boolean logRetryCache) {
    assert fsd.getFSNamesystem().hasWriteLock(RwLockMode.FS);
    Preconditions.checkArgument(file.isUnderConstruction());
    fsd.getEditLog().logUpdateBlocks(path, file, logRetryCache);
    if(NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("persistBlocks: " + path
              + " with " + file.getBlocks().length + " blocks is persisted to" +
              " the file system");
    }
  }

  /**
   * 放弃指定块，从正在构建的文件中移除该块并更新日志
   * @param fsd 目录管理器
   * @param pc 权限检查器
   * @param b 要放弃的扩展块
   * @param fileId 文件ID
   * @param src 文件路径
   * @param holder 租约持有者客户端名称
   * @throws IOException 操作过程中发生IO异常
   */
  static void abandonBlock(
      FSDirectory fsd, FSPermissionChecker pc, ExtendedBlock b, long fileId,
      String src, String holder) throws IOException {
    final INodesInPath iip = fsd.resolvePath(pc, src, fileId);
    src = iip.getPath();
    FSNamesystem fsn = fsd.getFSNamesystem();
    final INodeFile file = fsn.checkLease(iip, holder, fileId);
    Preconditions.checkState(file.isUnderConstruction());
    if (file.getBlockType() == BlockType.STRIPED) {
      return; // 纠删码文件不支持放弃块操作
    }

    Block localBlock = ExtendedBlock.getLocalBlock(b);
    fsd.writeLock();
    try {
      // 从文件中移除目标块
      if (!unprotectedRemoveBlock(fsd, src, iip, file, localBlock)) {
        return;
      }
    } finally {
      fsd.writeUnlock();
    }
    // 持久化更新后的块列表到编辑日志
    persistBlocks(fsd, src, file, false);
  }

  /**
   * 检查块所属的块池ID是否匹配当前NameNode
   * @param fsn 文件系统命名空间
   * @param block 待检查的扩展块
   * @throws IOException 块池ID不匹配时抛出异常
   */
  static void checkBlock(FSNamesystem fsn, ExtendedBlock block)
      throws IOException {
    String bpId = fsn.getBlockPoolId();
    if (block != null && !bpId.equals(block.getBlockPoolId())) {
      throw new IOException("Unexpected BlockPoolId " + block.getBlockPoolId()
          + " - expected " + bpId);
    }
  }

  /**
   * addBlock操作第一阶段：在读锁下分析文件状态，预验证添加新块条件并生成参数
   * 不创建新块，仅做状态校验和参数准备，处理重试场景和位置生成前置逻辑
   * @param fsn 文件系统命名空间
   * @param pc 权限检查器
   * @param src 文件路径
   * @param fileId 文件ID
   * @param clientName 客户端名称
   * @param previous 客户端提供的上一个块
   * @param onRetryBlock 输出参数，重试场景下返回已有块
   * @return 验证结果，包含新块所需参数，重试场景返回null
   * @throws IOException 验证失败抛出异常
   */
  static ValidateAddBlockResult validateAddBlock(
      FSNamesystem fsn, FSPermissionChecker pc,
      String src, long fileId, String clientName,
      ExtendedBlock previous, LocatedBlock[] onRetryBlock) throws IOException {
    final long blockSize;
    final short numTargets;
    final byte storagePolicyID;
    String clientMachine;
    final BlockType blockType;

    INodesInPath iip = fsn.dir.resolvePath(pc, src, fileId);
    // 分析文件状态，处理重试场景
    FileState fileState = analyzeFileState(fsn, iip, fileId, clientName,
                                           previous, onRetryBlock);
    if (onRetryBlock[0] != null && onRetryBlock[0].getLocations().length > 0) {
      // 这是重试请求，已有块位置信息，直接返回
      return null;
    }

    final INodeFile pendingFile = fileState.inode;
    // 检查倒数第二个块是否已满足最小副本数要求
    if (!fsn.checkFileProgress(src, pendingFile, false)) {
      throw new NotReplicatedYetException("Not replicated yet: " + src);
    }
    // 检查文件块数是否超过集群最大限制
    if (pendingFile.getBlocks().length >= fsn.maxBlocksPerFile) {
      throw new IOException("File has reached the limit on maximum number of"
          + " blocks (" + DFSConfigKeys.DFS_NAMENODE_MAX_BLOCKS_PER_FILE_KEY
          + "): " + pendingFile.getBlocks().length + " >= "
          + fsn.maxBlocksPerFile);
    }
    // 从文件INode中获取新块所需参数
    blockSize = pendingFile.getPreferredBlockSize();
    clientMachine = pendingFile.getFileUnderConstructionFeature()
        .getClientMachine();
    blockType = pendingFile.getBlockType();
    ErasureCodingPolicy ecPolicy = null;
    // 纠删码文件需要获取纠删码策略并计算目标DataNode数量
    if (blockType == BlockType.STRIPED) {
      ecPolicy =
          FSDirErasureCodingOp.unprotectedGetErasureCodingPolicy(fsn, iip);
      numTargets = (short) (ecPolicy.getSchema().getNumDataUnits()
          + ecPolicy.getSchema().getNumParityUnits());
    } else {
      // 普通复制块，目标数量等于文件副本数
      numTargets = pendingFile.getFileReplication();
    }
    storagePolicyID = pendingFile.getStoragePolicyID();
    return new ValidateAddBlockResult(blockSize, numTargets, storagePolicyID,
                                      clientMachine, blockType, ecPolicy);
  }

  /**
   * 创建LocatedBlock对象并生成写块令牌
   * @param fsn 文件系统命名空间
   * @param blk 块信息对象
   * @param locs 目标DataNode存储位置
   * @param offset 块在文件中的偏移量
   * @return 生成好的LocatedBlock对象
   * @throws IOException 生成令牌过程中发生异常
   */
  static LocatedBlock makeLocatedBlock(FSNamesystem fsn, BlockInfo blk,
      DatanodeStorageInfo[] locs, long offset) throws IOException {
    LocatedBlock lBlk = BlockManager.newLocatedBlock(
        fsn.getExtendedBlock(new Block(blk)), blk, locs, offset);
    fsn.getBlockManager().setBlockToken(lBlk,
        BlockTokenIdentifier.AccessMode.WRITE);
    return lBlk;
  }

  /**
   * addBlock操作第二阶段：在写锁下重新验证状态，分配并存储新块
   * 重复第一阶段状态分析，确保在选靶过程中状态未发生变化，确认后添加新块到命名空间
   * @param fsn 文件系统命名空间
   * @param src 文件路径
   * @param fileId 文件ID
   * @param clientName 客户端名称
   * @param previous 客户端提供的上一个块
   * @param targets 第一阶段选出的目标DataNode位置
   * @return 新分配块的LocatedBlock对象
   * @throws IOException 操作失败抛出异常
   */
  static LocatedBlock storeAllocatedBlock(FSNamesystem fsn, String src,
      long fileId, String clientName, ExtendedBlock previous,
      DatanodeStorageInfo[] targets) throws IOException {
    long offset;
    // 重新分析文件状态，因为选靶过程中状态可能已变更
    LocatedBlock[] onRetryBlock = new LocatedBlock[1];
    INodesInPath iip = fsn.dir.resolvePath(null, src, fileId);
    FileState fileState = analyzeFileState(fsn, iip, fileId, clientName,
                                           previous, onRetryBlock);
    final INodeFile pendingFile = fileState.inode;
    src = fileState.path;

    if (onRetryBlock[0] != null) {
      if (onRetryBlock[0].getLocations().length > 0) {
        // 重试场景，直接返回已有块
        return onRetryBlock[0];
      } else {
        // 已有分配的块但无位置，添加新选出的位置后返回
        BlockInfo lastBlockInFile = pendingFile.getLastBlock();
        lastBlockInFile.getUnderConstructionFeature().setExpectedLocations(
            lastBlockInFile, targets, pendingFile.getBlockType());
        offset = pendingFile.computeFileSize();
        return makeLocatedBlock(fsn, lastBlockInFile, targets, offset);
      }
    }

    // 提交上一个块，满足最小副本数则完成该块
    fsn.commitOrCompleteLastBlock(pendingFile, fileState.iip,
                                  ExtendedBlock.getLocalBlock(previous));

    final BlockType blockType = pendingFile.getBlockType();
    // 创建新块对象
    Block newBlock = fsn.createNewBlock(blockType);
    INodesInPath inodesInPath = INodesInPath.fromINode(pendingFile);
    // 将新块保存到文件INode和块管理器
    saveAllocatedBlock(fsn, src, inodesInPath, newBlock, targets, blockType);

    // 将添加块操作持久化到编辑日志
    persistNewBlock(fsn, src, pendingFile);
    offset = pendingFile.computeFileSize();

    // 返回构造好的LocatedBlock给客户端
    return makeLocatedBlock(fsn, fsn.getStoredBlock(newBlock), targets, offset);
  }

  /**
   * 为新块选择目标DataNode存储位置
   * @param bm 块管理器
   * @param src 文件路径
   * @param excludedNodes 要排除的DataNode列表
   * @param favoredNodes 客户端偏好的DataNode列表
   * @param flags addBlock操作标志位
   * @param r 第一阶段验证结果
   * @return 选出的目标DataNode存储信息数组
   * @throws IOException 选靶失败抛出异常
   */
  static DatanodeStorageInfo[] chooseTargetForNewBlock(
      BlockManager bm, String src, DatanodeInfo[] excludedNodes,
      String[] favoredNodes, EnumSet<AddBlockFlag> flags,
      ValidateAddBlockResult r) throws IOException {
    Node clientNode = null;

    boolean ignoreClientLocality = (flags != null
            && flags.contains(AddBlockFlag.IGNORE_CLIENT_LOCALITY));

    // 如果不忽略客户端位置，解析获取客户端网络拓扑节点
    if (!ignoreClientLocality) {
      clientNode = bm.getDatanodeManager().getDatanodeByHost(r.clientMachine);
      if (clientNode == null) {
        clientNode = getClientNode(bm, r.clientMachine);
      }
    }

    // 构造排除节点集合
    Set<Node> excludedNodesSet =
        (excludedNodes == null) ? new HashSet<>()
            : new HashSet<>(Arrays.asList(excludedNodes));

    // 构造偏好节点列表
    List<String> favoredNodesList =
        (favoredNodes == null) ? Collections.emptyList()
            : Arrays.asList(favoredNodes);

    // 调用块管理器为新块选择目标位置
    return bm.chooseTarget4NewBlock(src, r.numTargets, clientNode,
                                    excludedNodesSet, r.blockSize,
                                    favoredNodesList, r.storagePolicyID,
                                    r.blockType, r.ecPolicy, flags);
  }

  /**
   * 解析客户端机器地址，获取对应的网络拓扑节点
   * @param bm 块管理器
   * @param clientMachine 客户端机器主机名
   * @return 客户端的网络拓扑节点，解析失败返回null
   */
  static Node getClientNode(BlockManager