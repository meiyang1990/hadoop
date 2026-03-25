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

import java.io.IOException;
import java.util.Set;

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.hdfs.protocol.AlreadyBeingCreatedException;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
import org.apache.hadoop.hdfs.protocol.QuotaExceededException;
import org.apache.hadoop.hdfs.protocol.SnapshotAccessControlException;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguous;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.protocol.BlockType;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockUnderConstructionFeature;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.BlockUCState;
import org.apache.hadoop.hdfs.server.namenode.FSDirectory.DirOp;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem.RecoverLeaseOp;
import org.apache.hadoop.hdfs.server.namenode.INode.BlocksMapUpdateInfo;
import org.apache.hadoop.hdfs.util.RwLockMode;

import org.apache.hadoop.classification.VisibleForTesting;

/**
 * 文件系统截断操作工具类，负责处理HDFS NameNode端的文件截断逻辑
 * 提供截断操作的权限校验、 quota检查、块清理和恢复准备等完整流程
 */
final class FSDirTruncateOp {

  /**
   * 私有构造函数，禁止实例化该工具类
   */
  private FSDirTruncateOp() {}

  /**
   * 执行文件截断操作，将文件裁剪到指定大小
   *
   * @param fsn 文件系统命名空间对象
   * @param srcArg 目标文件路径
   * @param newLength 截断后的目标文件大小
   * @param clientName 客户端名称
   * @param clientMachine 客户端机器信息
   * @param mtime 修改时间戳
   * @param toRemoveBlocks 用于收集待删除块的信息对象
   * @param pc 权限检查器
   * @return 截断操作结果，包含是否需要客户端等待块恢复和文件信息
   * @throws IOException 操作IO异常
   * @throws UnresolvedLinkException 路径链接未解析异常
   */
  static TruncateResult truncate(final FSNamesystem fsn, final String srcArg,
      final long newLength, final String clientName,
      final String clientMachine, final long mtime,
      final BlocksMapUpdateInfo toRemoveBlocks, final FSPermissionChecker pc)
      throws IOException, UnresolvedLinkException {
    assert fsn.hasWriteLock(RwLockMode.GLOBAL);

    FSDirectory fsd = fsn.getFSDirectory();
    final String src;
    final INodesInPath iip;
    final boolean onBlockBoundary;
    Block truncateBlock = null;
    fsd.writeLock();
    try {
      // 解析目标文件路径
      iip = fsd.resolvePath(pc, srcArg, DirOp.WRITE);
      src = iip.getPath();
      if (fsd.isPermissionEnabled()) {
        // 检查写权限
        fsd.checkPathAccess(pc, iip, FsAction.WRITE);
      }
      INodeFile file = INodeFile.valueOf(iip.getLastINode(), src);

      // 不支持纠删码条带化文件截断
      if (file.isStriped()) {
        throw new UnsupportedOperationException(
            "Cannot truncate file with striped block " + src);
      }

      final BlockStoragePolicy lpPolicy = fsd.getBlockManager()
          .getStoragePolicy("LAZY_PERSIST");

      // 不支持LAZY_PERSIST策略文件截断
      if (lpPolicy != null && lpPolicy.getId() == file.getStoragePolicyID()) {
        throw new UnsupportedOperationException(
            "Cannot truncate lazy persist file " + src);
      }

      // 检查文件是否已经处于截断过程中
      final BlockInfo last = file.getLastBlock();
      if (last != null && last.getBlockUCState()
          == BlockUCState.UNDER_RECOVERY) {
        final BlockInfo truncatedBlock = last.getUnderConstructionFeature()
            .getTruncateBlock();
        if (truncatedBlock != null) {
          // 计算当前截断目标长度
          final long truncateLength = file.computeFileSize(false, false)
              + truncatedBlock.getNumBytes();
          if (newLength == truncateLength) {
            // 已有相同长度截断在进行，直接返回结果
            return new TruncateResult(false, fsd.getAuditFileInfo(iip));
          } else {
            // 已有不同长度截断在进行，抛出异常
            throw new AlreadyBeingCreatedException(
                RecoverLeaseOp.TRUNCATE_FILE.getExceptionMessage(src,
                    clientName, clientMachine, src + " is being truncated."));
          }
        }
      }

      // 恢复已有租约，处理文件被其他客户端占用的情况
      fsn.recoverLeaseInternal(RecoverLeaseOp.TRUNCATE_FILE, iip, src,
          clientName, clientMachine, false);
      // 获取原文件长度
      long oldLength = file.computeFileSize();
      if (oldLength == newLength) {
        // 长度一致无需截断，直接返回
        return new TruncateResult(true, fsd.getAuditFileInfo(iip));
      }
      if (oldLength < newLength) {
        // 截断不能扩大文件大小，抛出异常
        throw new HadoopIllegalArgumentException(
            "Cannot truncate to a larger file size. Current size: " + oldLength
                + ", truncate size: " + newLength + ".");
      }
      // 计算配额变更
      final QuotaCounts delta = new QuotaCounts.Builder().build();
      // 执行不受保护的截断逻辑，清理多余块
      onBlockBoundary = unprotectedTruncate(fsn, iip, newLength,
          toRemoveBlocks, mtime, delta);
      if (!onBlockBoundary) {
        // 截断点不在块边界，需要准备块恢复
        long lastBlockDelta = file.computeFileSize() - newLength;
        assert lastBlockDelta > 0 : "delta is 0 only if on block bounday";
        // 准备文件，将最后一块转为恢复状态
        truncateBlock = prepareFileForTruncate(fsn, iip, clientName,
            clientMachine, lastBlockDelta, null);
      }

      // 更新目录配额计数
      fsd.updateCountNoQuotaCheck(iip, iip.length() - 1, delta);
    } finally {
      fsd.writeUnlock();
    }

    // 记录截断操作到编辑日志
    fsn.getEditLog().logTruncate(src, clientName, clientMachine, newLength,
        mtime, truncateBlock);
    return new TruncateResult(onBlockBoundary, fsd.getAuditFileInfo(iip));
  }

  /**
   * 无保护截断实现，不处理租约恢复，仅用于编辑日志重放时
   *
   * @param fsn 文件系统命名空间对象
   * @param iip 路径对应的INode链表
   * @param clientName 客户端名称
   * @param clientMachine 客户端机器信息
   * @param newLength 截断后的目标文件大小
   * @param mtime 修改时间戳
   * @param truncateBlock 截断块信息
   * @throws UnresolvedLinkException 路径链接未解析异常
   * @throws QuotaExceededException 配额超限异常
   * @throws SnapshotAccessControlException 快照访问控制异常
   * @throws IOException 操作IO异常
   */
  static void unprotectedTruncate(final FSNamesystem fsn,
      final INodesInPath iip,
      final String clientName, final String clientMachine,
      final long newLength, final long mtime, final Block truncateBlock)
      throws UnresolvedLinkException, QuotaExceededException,
      SnapshotAccessControlException, IOException {
    assert fsn.hasWriteLock(RwLockMode.GLOBAL);

    FSDirectory fsd = fsn.getFSDirectory();
    INodeFile file = iip.getLastINode().asFile();
    BlocksMapUpdateInfo collectedBlocks = new BlocksMapUpdateInfo();
    boolean onBlockBoundary = unprotectedTruncate(fsn, iip, newLength,
        collectedBlocks, mtime, null);

    if (!onBlockBoundary) {
      BlockInfo oldBlock = file.getLastBlock();
      Block tBlk = prepareFileForTruncate(fsn, iip, clientName, clientMachine,
          file.computeFileSize() - newLength, truncateBlock);
      assert Block.matchingIdAndGenStamp(tBlk, truncateBlock) &&
          tBlk.getNumBytes() == truncateBlock.getNumBytes() :
          "Should be the same block.";
      // 如果是新块且原块不在最新快照中，删除原块
      if (oldBlock.getBlockId() != tBlk.getBlockId()
          && !file.isBlockInLatestSnapshot(oldBlock)) {
        oldBlock.delete();
        fsd.getBlockManager().removeBlockFromMap(oldBlock);
      }
    }
    assert onBlockBoundary == (truncateBlock == null) :
      "truncateBlock is null iff on block boundary: " + truncateBlock;
    // 移除收集到的待删除块并更新安全模式总块计数
    fsn.getBlockManager().removeBlocksAndUpdateSafemodeTotal(collectedBlocks);
  }

  /**
   * 将文件设置为构建中状态，创建租约，为截断准备最后一块，调度DataNode进行块截断恢复
   *
   * @param fsn 文件系统命名空间对象
   * @param iip 路径对应的INode链表
   * @param leaseHolder 租约持有者（客户端名称）
   * @param clientMachine 客户端机器信息
   * @param lastBlockDelta 最后一块需要裁剪的大小
   * @param newBlock 新截断块（编辑日志重放时传入，新截断时为null）
   * @return 生成的截断块，会被写入编辑日志
   * @throws IOException 操作IO异常
   */
  @VisibleForTesting
  static Block prepareFileForTruncate(FSNamesystem fsn, INodesInPath iip,
      String leaseHolder, String clientMachine, long lastBlockDelta,
      Block newBlock) throws IOException {
    assert fsn.hasWriteLock(RwLockMode.GLOBAL);

    INodeFile file = iip.getLastINode().asFile();
    assert !file.isStriped();
    // 记录修改到快照
    file.recordModification(iip.getLatestSnapshotId());
    // 将文件转为构建中状态
    file.toUnderConstruction(leaseHolder, clientMachine);
    assert file.isUnderConstruction() : "inode should be under construction.";
    // 添加截断租约
    fsn.getLeaseManager().addLease(
        file.getFileUnderConstructionFeature().getClientName(), file.getId());
    boolean shouldRecoverNow = (newBlock == null);
    BlockInfo oldBlock = file.getLastBlock();

    // 判断是否需要复制块（还是原地截断）
    boolean shouldCopyOnTruncate = shouldCopyOnTruncate(fsn, file, oldBlock);
    if (newBlock == null) {
      // 需要复制则创建新块，否则复用原块ID更新生成Stamp
      newBlock = (shouldCopyOnTruncate) ?
          fsn.createNewBlock(BlockType.CONTIGUOUS)
          : new Block(oldBlock.getBlockId(), oldBlock.getNumBytes(),
          fsn.nextGenerationStamp(fsn.getBlockManager().isLegacyBlock(
              oldBlock)));
    }

    final BlockInfo truncatedBlockUC;
    BlockManager blockManager = fsn.getFSDirectory().getBlockManager();
    if (shouldCopyOnTruncate) {
      // 复制方式：创建新块，记录原块信息，由DataNode复制后截断
      truncatedBlockUC = new BlockInfoContiguous(newBlock,
          file.getPreferredBlockReplication());
      truncatedBlockUC.convertToBlockUnderConstruction(
          BlockUCState.UNDER_CONSTRUCTION, blockManager.getStorages(oldBlock));
      // 设置截断后新块大小
      truncatedBlockUC.setNumBytes(oldBlock.getNumBytes() - lastBlockDelta);
      // 保存原块引用用于恢复
      truncatedBlockUC.getUnderConstructionFeature().setTruncateBlock(oldBlock);
      // 更新文件最后一块为新块
      file.setLastBlock(truncatedBlockUC);
      // 将新块添加到块映射
      blockManager.addBlockCollection(truncatedBlockUC, file);

      NameNode.stateChangeLog.debug(
          "BLOCK* prepareFileForTruncate: Scheduling copy-on-truncate to new"
              + " size {}  new block {} old block {}",
          truncatedBlockUC.getNumBytes(), newBlock, oldBlock);
    } else {
      // 原地方式：直接将原块转为构建中，更新生成Stamp后就地截断
      blockManager.convertLastBlockToUnderConstruction(file, lastBlockDelta);
      oldBlock = file.getLastBlock();
      assert !oldBlock.isComplete() : "oldBlock should be under construction";
      BlockUnderConstructionFeature uc = oldBlock.getUnderConstructionFeature();
      // 保存原块引用用于恢复
      uc.setTruncateBlock(new BlockInfoContiguous(oldBlock,
          oldBlock.getReplication()));
      // 设置截断后大小
      uc.getTruncateBlock().setNumBytes(oldBlock.getNumBytes() - lastBlockDelta);
      final long newGenerationStamp = newBlock.getGenerationStamp();
      // 更新生成Stamp保证版本正确性
      uc.getTruncateBlock().setGenerationStamp(newGenerationStamp);
      // 更新备用NameNode的全局生成Stamp
      blockManager.getBlockIdManager().setGenerationStampIfGreater(
          newGenerationStamp);
      truncatedBlockUC = oldBlock;

      NameNode.stateChangeLog.debug("BLOCK* prepareFileForTruncate: " +
          "{} Scheduling in-place block truncate to new size {}",
          uc, uc.getTruncateBlock().getNumBytes());
    }
    if (shouldRecoverNow) {
      // 初始化块恢复流程，由NameNode调度DataNode执行截断
      truncatedBlockUC.getUnderConstructionFeature().initializeBlockRecovery(
          truncatedBlockUC, newBlock.getGenerationStamp(), true);
    }

    return newBlock;
  }

  /**
   * 底层无保护截断逻辑，清理超过目标长度的块，更新文件大小和修改时间
   *
   * @param fsn 文件系统命名空间对象
   * @param iip 路径对应的INode链表
   * @param newLength 截断后的目标文件大小
   * @param collectedBlocks 收集待删除块的信息对象
   * @param mtime 修改时间戳
   * @param delta 配额变更计数对象
   * @return true如果截断点正好在块边界，无需后续块恢复；false否则需要恢复
   * @throws IOException 操作IO异常
   */
  private static boolean unprotectedTruncate(FSNamesystem fsn,
      INodesInPath iip, long newLength, BlocksMapUpdateInfo collectedBlocks,
      long mtime, QuotaCounts delta) throws IOException {
    assert fsn.hasWriteLock(RwLockMode.GLOBAL);

    INodeFile file = iip.getLastINode().asFile();
    int latestSnapshot = iip.getLatestSnapshotId();
    // 记录文件修改，处理快照相关逻辑
    file.recordModification(latestSnapshot, true);

    // 检查截断后的配额是否足够
    verifyQuotaForTruncate(fsn, iip, file, newLength, delta);

    // 获取快照需要保留的块集合
    Set<BlockInfo> toRetain = file.getSnapshotBlocksToRetain(latestSnapshot);
    // 收集超过目标长度的块到待删除列表，返回剩余长度
    long remainingLength = file.collectBlocksBeyondMax(newLength,
        collectedBlocks, toRetain);
    // 更新文件修改时间
    file.setModificationTime(mtime);
    // 返回是否正好在块边界
    return (remainingLength - newLength) == 0;
  }

  /**
   * 验证截断后的目录配额是否足够
   *
   * @param fsn 文件系统命名空间对象
   * @param iip 路径对应的INode链表
   * @param file 目标文件INode
   * @param newLength 截断后的目标文件大小
   * @param delta 配额变更计数对象
   * @throws QuotaExceededException 如果配额超限抛出该异常
   */
  private static void verifyQuotaForTruncate(FSNamesystem fsn,
      INodesInPath iip, INodeFile file, long newLength, QuotaCounts delta)
      throws QuotaExceededException {
    FSDirectory fsd = fsn.getFSDirectory();