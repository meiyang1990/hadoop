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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LastBlockWithStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.QuotaExceededException;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.BlockUCState;
import org.apache.hadoop.hdfs.server.namenode.FSDirectory.DirOp;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem.RecoverLeaseOp;
import org.apache.hadoop.hdfs.server.namenode.NameNodeLayoutVersion.Feature;
import org.apache.hadoop.hdfs.util.RwLockMode;
import org.apache.hadoop.ipc.RetriableException;

import org.apache.hadoop.util.Preconditions;

/**
 * 文件追加写入操作辅助类，负责处理HDFS NameNode侧的文件追加操作逻辑
 */
final class FSDirAppendOp {

  /**
   * 工具类禁止实例化
   */
  private FSDirAppendOp() {}

  /**
   * 对已有文件执行追加写入操作，准备写入所需的最后一个块信息
   * @param fsn 命名空间对象
   * @param srcArg 目标文件路径
   * @param pc 权限检查器
   * @param holder 客户端名称
   * @param clientMachine 客户端机器信息
   * @param newBlock 是否追加到新块
   * @param logRetryCache 是否在编辑日志中记录RPC ID用于重试缓存重建
   * @return 最后一个块信息和文件状态
   * @throws IOException 各类IO异常，包括文件不存在、权限不足、配额超限等
   */
  static LastBlockWithStatus appendFile(final FSNamesystem fsn,
      final String srcArg, final FSPermissionChecker pc, final String holder,
      final String clientMachine, final boolean newBlock,
      final boolean logRetryCache) throws IOException {
    assert fsn.hasWriteLock(RwLockMode.GLOBAL);

    final LocatedBlock lb;
    final FSDirectory fsd = fsn.getFSDirectory();
    final INodesInPath iip;
    // 获取目录写锁
    fsd.writeLock();
    try {
      // 解析文件路径得到INode链表
      iip = fsd.resolvePath(pc, srcArg, DirOp.WRITE);
      // 获取路径最后一个节点
      final INode inode = iip.getLastINode();
      final String path = iip.getPath();
      // 目标路径已经存在且是目录，抛出异常
      if (inode != null && inode.isDirectory()) {
        throw new FileAlreadyExistsException("Cannot append to directory "
            + path + "; already exists as a directory.");
      }
      // 启用权限检查时，检查用户是否有写权限
      if (fsd.isPermissionEnabled()) {
        fsd.checkPathAccess(pc, iip, FsAction.WRITE);
      }

      // 目标文件不存在，抛出异常
      if (inode == null) {
        throw new FileNotFoundException(
            "Failed to append to non-existent file " + path + " for client "
                + clientMachine);
      }
      // 将INode转换为文件类型
      final INodeFile file = INodeFile.valueOf(inode, path, true);

      // 纠删码文件不支持追加到已有块，仅支持追加到新块
      if (file.isStriped() && !newBlock) {
        throw new UnsupportedOperationException(
            "Append on EC file without new block is not supported. Use "
                + CreateFlag.NEW_BLOCK + " create flag while appending file.");
      }

      BlockManager blockManager = fsd.getBlockManager();
      // 获取LAZY_PERSIST存储策略
      final BlockStoragePolicy lpPolicy = blockManager
          .getStoragePolicy("LAZY_PERSIST");
      // 不允许对LAZY_PERSIST文件执行追加操作
      if (lpPolicy != null && lpPolicy.getId() == file.getStoragePolicyID()) {
        throw new UnsupportedOperationException(
            "Cannot append to lazy persist file " + path);
      }
      // 打开文件追加前，需要先恢复租约（处理上一个客户端未正常关闭的情况）
      fsn.recoverLeaseInternal(RecoverLeaseOp.APPEND_FILE, iip, path, holder,
          clientMachine, false);

      // 获取文件最后一个块
      final BlockInfo lastBlock = file.getLastBlock();
      // 检查块的复制状态是否满足要求
      if (lastBlock != null) {
        // 块已提交但未完成复制，抛出可重试异常等待复制完成
        if (lastBlock.getBlockUCState() == BlockUCState.COMMITTED) {
          throw new RetriableException(
              new NotReplicatedYetException("append: lastBlock="
                  + lastBlock + " of src=" + path
                  + " is COMMITTED but not yet COMPLETE."));
        } else if (lastBlock.isComplete()
          && !blockManager.isSufficientlyReplicated(lastBlock)) {
          // 块已完成但副本数不足，抛出异常
          throw new IOException("append: lastBlock=" + lastBlock + " of src="
              + path + " is not sufficiently replicated yet.");
        }
      }
      // 将文件转为追加写入状态，准备最后一个块信息
      lb = prepareFileForAppend(fsn, iip, holder, clientMachine, newBlock,
          true, logRetryCache);
    } catch (IOException ie) {
      // 记录异常日志
      NameNode.stateChangeLog
          .warn("DIR* NameSystem.append: " + ie.getMessage());
      throw ie;
    } finally {
      // 释放目录写锁
      fsd.writeUnlock();
    }

    // 获取文件状态信息
    HdfsFileStatus stat =
        FSDirStatAndListingOp.getFileInfo(fsd, iip, false, false);
    if (lb != null) {
      // 记录调试日志
      NameNode.stateChangeLog.debug(
          "DIR* NameSystem.appendFile: file {} for {} at {} block {} block"
              + " size {}", srcArg, holder, clientMachine, lb.getBlock(), lb
              .getBlock().getNumBytes());
    }
    return new LastBlockWithStatus(lb, stat);
  }

  /**
   * 将文件状态转为构造中（under construction），重建内存租约记录，为追加操作准备文件
   * @param fsn 命名空间对象
   * @param iip 文件路径对应的INode链表
   * @param leaseHolder 租约持有者标识
   * @param clientMachine 客户端机器标识
   * @param newBlock 是否追加到新块
   * @param writeToEditLog 是否将变更持久化到编辑日志
   * @param logRetryCache 是否在编辑日志中记录RPC ID用于重试缓存重建
   * @return 如果最后一个块未满返回块位置信息，否则返回null
   * @throws IOException IO异常
   */
  static LocatedBlock prepareFileForAppend(final FSNamesystem fsn,
      final INodesInPath iip, final String leaseHolder,
      final String clientMachine, final boolean newBlock,
      final boolean writeToEditLog, final boolean logRetryCache)
      throws IOException {
    assert fsn.hasWriteLock(RwLockMode.GLOBAL);

    final INodeFile file = iip.getLastINode().asFile();
    // 配额检查，计算预期配额变化量
    final QuotaCounts delta = verifyQuotaForUCBlock(fsn, file, iip);

    // 记录文件修改到快照
    file.recordModification(iip.getLatestSnapshotId());
    // 将文件转为构造中状态
    file.toUnderConstruction(leaseHolder, clientMachine);

    // 为文件添加租约
    fsn.getLeaseManager().addLease(
        file.getFileUnderConstructionFeature().getClientName(), file.getId());

    LocatedBlock ret = null;
    if (!newBlock) {
      // 不强制追加新块时，将已有最后一个块转为构造中状态
      FSDirectory fsd = fsn.getFSDirectory();
      ret = fsd.getBlockManager().convertLastBlockToUnderConstruction(file, 0);
      if (ret != null && delta != null) {
        Preconditions.checkState(delta.getStorageSpace() >= 0, "appending to"
            + " a block with size larger than the preferred block size");
        // 更新目录配额计数
        fsd.writeLock();
        try {
          fsd.updateCountNoQuotaCheck(iip, iip.length() - 1, delta);
        } finally {
          fsd.writeUnlock();
        }
      }
    } else {
      // 强制追加新块时，返回已有最后一个块信息让客户端后续分配新块
      BlockInfo lastBlock = file.getLastBlock();
      if (lastBlock != null) {
        ExtendedBlock blk = new ExtendedBlock(fsn.getBlockPoolId(), lastBlock);
        ret = new LocatedBlock(blk, DatanodeInfo.EMPTY_ARRAY);
      }
    }

    // 将追加操作记录到编辑日志
    if (writeToEditLog) {
      final String path = iip.getPath();
      // 支持APPEND_NEW_BLOCK特性的版本，记录追加日志
      if (NameNodeLayoutVersion.supports(Feature.APPEND_NEW_BLOCK,
          fsn.getEffectiveLayoutVersion())) {
        fsn.getEditLog().logAppendFile(path, file, newBlock, logRetryCache);
      } else {
        // 旧版本兼容，记录打开文件日志
        fsn.getEditLog().logOpenFile(path, file, false, logRetryCache);
      }
    }
    return ret;
  }

  /**
   * 验证块转为构造中状态后的配额是否满足要求，用于追加和截断操作
   * @param fsn 命名空间对象
   * @param file 目标文件INode
   * @param iip 文件路径对应的INode链表
   * @return 预期配额变化量，null表示无变化或无需后续更新配额
   * @throws QuotaExceededException 配额超限异常
   */
  private static QuotaCounts verifyQuotaForUCBlock(FSNamesystem fsn,
      INodeFile file, INodesInPath iip) throws QuotaExceededException {
    FSDirectory fsd = fsn.getFSDirectory();
    // 镜像未加载完成或跳过配额检查，直接返回
    if (!fsn.isImageLoaded() || fsd.shouldSkipQuotaChecks()) {
      return null;
    }
    // 存在最后一个块时检查配额
    if (file.getLastBlock() != null) {
      final QuotaCounts delta = computeQuotaDeltaForUCBlock(fsn, file);
      fsd.readLock();
      try {
        FSDirectory.verifyQuota(iip, iip.length() - 1, delta, null);
        return delta;
      } finally {
        fsd.readUnlock();
      }
    }
    return null;
  }

  /**
   * 计算将已完成块转为构造中块后的配额变化量
   * @param fsn 命名空间对象
   * @param file 目标文件INode
   * @return 配额变化量
   */
  private static QuotaCounts computeQuotaDeltaForUCBlock(FSNamesystem fsn,
      INodeFile file) {
    final QuotaCounts delta = new QuotaCounts.Builder().build();
    final BlockInfo lastBlock = file.getLastBlock();
    if (lastBlock != null) {
      // 计算配额增量：偏好块大小减去已有块大小
      final long diff = file.getPreferredBlockSize() - lastBlock.getNumBytes();
      final short repl = lastBlock.getReplication();
      delta.addStorageSpace(diff * repl);
      // 根据存储策略计算各存储类型的配额增量
      final BlockStoragePolicy policy = fsn.getFSDirectory()
          .getBlockStoragePolicySuite().getPolicy(file.getStoragePolicyID());
      List<StorageType> types = policy.chooseStorageTypes(repl);
      for (StorageType t : types) {
        if (t.supportTypeQuota()) {
          delta.addTypeSpace(t, diff);
        }
      }
    }
    return delta;
  }
}