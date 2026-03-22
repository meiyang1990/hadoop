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

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.util.Preconditions;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.InvalidPathException;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.ParentNotDirectoryException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.QuotaExceededException;
import org.apache.hadoop.hdfs.protocol.SnapshotException;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockStoragePolicySuite;
import org.apache.hadoop.hdfs.server.namenode.FSDirectory.DirOp;
import org.apache.hadoop.hdfs.server.namenode.INode.BlocksMapUpdateInfo;
import org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot;
import org.apache.hadoop.hdfs.server.namenode.snapshot.SnapshotManager;
import org.apache.hadoop.hdfs.util.ReadOnlyList;
import org.apache.hadoop.util.ChunkedArrayList;
import org.apache.hadoop.util.Time;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static org.apache.hadoop.hdfs.protocol.FSLimitException.MaxDirectoryItemsExceededException;
import static org.apache.hadoop.hdfs.protocol.FSLimitException.PathComponentTooLongException;

/**
 * HDFS NameNode文件系统重命名操作工具类，封装了所有重命名相关的核心逻辑
 * 包括权限检查、配额验证、快照校验、事务性重命名执行与回滚等功能
 */
class FSDirRenameOp {
  /**
   * 已废弃的旧版重命名入口方法，兼容旧版本逻辑
   * @param fsd FSDirectory目录管理对象
   * @param pc 权限检查器
   * @param src 源路径字符串
   * @param dst 目标路径字符串
   * @param logRetryCache 是否记录到重试缓存
   * @return 重命名操作结果
   * @throws IOException 重命名过程中出现IO或逻辑错误
   */
  @Deprecated
  static RenameResult renameToInt(
      FSDirectory fsd, FSPermissionChecker pc, final String src,
      final String dst, boolean logRetryCache) throws IOException {
    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("DIR* NameSystem.renameTo: " + src +
          " to " + dst);
    }

    // Rename does not operate on link targets
    // Do not resolveLink when checking permissions of src and dst
    INodesInPath srcIIP = fsd.resolvePath(pc, src, DirOp.WRITE_LINK);
    INodesInPath dstIIP = fsd.resolvePath(pc, dst, DirOp.CREATE_LINK);
    dstIIP = dstForRenameTo(srcIIP, dstIIP);
    return renameTo(fsd, pc, srcIIP, dstIIP, logRetryCache);
  }

  /**
   * 验证重命名操作的配额是否满足，源移动到目标后的配额变化计算
   * @param fsd FSDirectory目录管理对象
   * @param src 源路径INodes列表
   * @param dst 目标路径INodes列表
   * @return 源目录配额变化和目标目录配额变化的二元组
   * @throws QuotaExceededException 配额超出限制时抛出异常
   */
  private static Pair<Optional<QuotaCounts>, Optional<QuotaCounts>> verifyQuotaForRename(
      FSDirectory fsd, INodesInPath src, INodesInPath dst) throws QuotaExceededException {
    Optional<QuotaCounts> srcDelta = Optional.empty();
    Optional<QuotaCounts> dstDelta = Optional.empty();
    if (!fsd.getFSNamesystem().isImageLoaded() || fsd.shouldSkipQuotaChecks()) {
      // Do not check quota if edits log is still being processed
      return Pair.of(srcDelta, dstDelta);
    }
    int i = 0;
    while (src.getINode(i) == dst.getINode(i)) {
      i++;
    }
    // src[i - 1] is the last common ancestor.
    BlockStoragePolicySuite bsps = fsd.getBlockStoragePolicySuite();
    // Assume dstParent existence check done by callers.
    INode dstParent = dst.getINode(-2);
    // Use the destination parent's storage policy for quota delta verify.
    final boolean isSrcSetSp = src.getLastINode().isSetStoragePolicy();
    final byte storagePolicyID = isSrcSetSp ?
        src.getLastINode().getLocalStoragePolicyID() :
        dstParent.getStoragePolicyID();
    final QuotaCounts delta = src.getLastINode()
        .computeQuotaUsage(bsps, storagePolicyID, false,
            Snapshot.CURRENT_STATE_ID);
    QuotaCounts srcQuota = new QuotaCounts.Builder().quotaCount(delta).build();
    srcDelta = Optional.of(srcQuota);

    // Reduce the required quota by dst that is being removed
    final INode dstINode = dst.getLastINode();
    if (dstINode != null) {
      QuotaCounts counts = dstINode.computeQuotaUsage(bsps);
      QuotaCounts dstQuota = new QuotaCounts.Builder().quotaCount(counts).build();
      dstDelta = Optional.of(dstQuota);
      delta.subtract(counts);
    }
    FSDirectory.verifyQuota(dst, dst.length() - 1, delta, src.getINode(i - 1));
    return Pair.of(srcDelta, dstDelta);
  }

  /**
   * 验证重命名操作是否满足文件系统限制，包括路径组件长度和目录最大项数限制
   * @param fsd FSDirectory目录管理对象
   * @param srcIIP 源路径INodes列表
   * @param dstIIP 目标路径INodes列表
   * @throws PathComponentTooLongException 路径组件过长时抛出异常
   * @throws MaxDirectoryItemsExceededException 目录项数超出限制时抛出异常
   */
  static void verifyFsLimitsForRename(FSDirectory fsd, INodesInPath srcIIP,
      INodesInPath dstIIP)
      throws PathComponentTooLongException, MaxDirectoryItemsExceededException {
    byte[] dstChildName = dstIIP.getLastLocalName();
    final String parentPath = dstIIP.getParentPath();
    fsd.verifyMaxComponentLength(dstChildName, parentPath);
    // Do not enforce max directory items if renaming within same directory.
    if (srcIIP.getINode(-2) != dstIIP.getINode(-2)) {
      fsd.verifyMaxDirItems(dstIIP.getINode(-2).asDirectory(), parentPath);
    }
  }

  /**
   * 仅用于编辑日志加载的重命名方法，已废弃，保持向后兼容
   * @param fsd FSDirectory目录管理对象
   * @param src 源路径字符串
   * @param dst 目标路径字符串
   * @param timestamp 修改时间戳
   * @return 重命名后的目标INodesInPath对象
   * @throws IOException 重命名过程中出现IO错误
   */
  /**
   * <br>
   * Note: This is to be used by {@link FSEditLogLoader} only.
   * <br>
   */
  @Deprecated
  static INodesInPath renameForEditLog(FSDirectory fsd, String src, String dst,
      long timestamp) throws IOException {
    final INodesInPath srcIIP = fsd.getINodesInPath(src, DirOp.WRITE_LINK);
    INodesInPath dstIIP = fsd.getINodesInPath(dst, DirOp.WRITE_LINK);
    // this is wrong but accidentally works.  the edit contains the full path
    // so the following will do nothing, but shouldn't change due to backward
    // compatibility when maybe full path wasn't logged.
    dstIIP = dstForRenameTo(srcIIP, dstIIP);
    return unprotectedRenameTo(fsd, srcIIP, dstIIP, timestamp);
  }

  /**
   * 处理目标路径：如果目标是已存在目录，则将源文件名追加到目标目录下生成新目标路径
   * @param srcIIP 源路径INodes列表
   * @param dstIIP 初始目标路径INodes列表
   * @return 处理后的最终目标路径INodes列表
   * @throws IOException 处理过程中出现IO错误
   */
  // if destination is a directory, append source child's name, else return
  // iip as-is.
  private static INodesInPath dstForRenameTo(
      INodesInPath srcIIP, INodesInPath dstIIP) throws IOException {
    INode dstINode = dstIIP.getLastINode();
    if (dstINode != null && dstINode.isDirectory()) {
      byte[] childName = srcIIP.getLastLocalName();
      // new dest might exist so look it up.
      INode childINode = dstINode.asDirectory().getChild(
          childName, dstIIP.getPathSnapshotId());
      dstIIP = INodesInPath.append(dstIIP, childINode, childName);
    }
    return dstIIP;
  }

  /**
   * 已废弃的无保护重命名方法，仅用于旧版本兼容
   * @param fsd FSDirectory目录管理对象
   * @param srcIIP 源路径INodes列表
   * @param dstIIP 目标路径INodes列表
   * @param timestamp 修改时间戳
   * @return 重命名成功返回目标INodesInPath，失败返回null
   * @throws IOException 重命名过程中出现IO错误
   */
  /**
   * Change a path name
   *
   * @param fsd FSDirectory
   * @param srcIIP source path
   * @param dstIIP destination path
   * @return true INodesInPath if rename succeeds; null otherwise
   * @deprecated See {@link #renameToInt(FSDirectory, FSPermissionChecker,
   * String, String, boolean, Options.Rename...)}
   */
  @Deprecated
  static INodesInPath unprotectedRenameTo(FSDirectory fsd,
      final INodesInPath srcIIP, final INodesInPath dstIIP, long timestamp)
      throws IOException {
    assert fsd.hasWriteLock();
    final INode srcInode = srcIIP.getLastINode();
    List<INodeDirectory> snapshottableDirs = new ArrayList<>();
    try {
      validateRenameSource(fsd, srcIIP, snapshottableDirs);
    } catch (SnapshotException e) {
      throw e;
    } catch (IOException ignored) {
      return null;
    }

    String src = srcIIP.getPath();
    String dst = dstIIP.getPath();
    // validate the destination
    if (dst.equals(src)) {
      return dstIIP;
    }

    try {
      validateDestination(src, dst, srcInode);
    } catch (IOException ignored) {
      return null;
    }

    if (dstIIP.getLastINode() != null) {
      NameNode.stateChangeLog.warn("DIR* FSDirectory.unprotectedRenameTo: " +
          "failed to rename " + src + " to " + dst + " because destination " +
          "exists");
      return null;
    }
    INode dstParent = dstIIP.getINode(-2);
    if (dstParent == null) {
      NameNode.stateChangeLog.warn("DIR* FSDirectory.unprotectedRenameTo: " +
          "failed to rename " + src + " to " + dst + " because destination's " +
          "parent does not exist");
      return null;
    }

    validateNestSnapshot(fsd, src, dstParent.asDirectory(), snapshottableDirs);
    checkUnderSameSnapshottableRoot(fsd, srcIIP, dstIIP);
    fsd.ezManager.checkMoveValidity(srcIIP, dstIIP);
    // Ensure dst has quota to accommodate rename
    verifyFsLimitsForRename(fsd, srcIIP, dstIIP);
    Pair<Optional<QuotaCounts>, Optional<QuotaCounts>> countPair =
        verifyQuotaForRename(fsd, srcIIP, dstIIP);

    RenameOperation tx = new RenameOperation(fsd, srcIIP, dstIIP, countPair);

    boolean added = false;

    INodesInPath renamedIIP = null;
    try {
      // remove src
      if (!tx.removeSrc4OldRename()) {
        return null;
      }

      renamedIIP = tx.addSourceToDestination();
      added = (renamedIIP != null);
      if (added) {
        if (NameNode.stateChangeLog.isDebugEnabled()) {
          NameNode.stateChangeLog.debug("DIR* FSDirectory" +
              ".unprotectedRenameTo: " + src + " is renamed to " + dst);
        }

        tx.updateMtimeAndLease(timestamp);
        tx.updateQuotasInSourceTree(fsd.getBlockStoragePolicySuite());

        return renamedIIP;
      }
    } finally {
      if (!added) {
        tx.restoreSource();
      }
    }
    NameNode.stateChangeLog.warn("DIR* FSDirectory.unprotectedRenameTo: " +
        "failed to rename " + src + " to " + dst);
    return null;
  }

  /**
   * 支持POSIX语义的新版重命名入口方法
   * @param fsd FSDirectory目录管理对象
   * @param pc 权限检查器
   * @param srcArg 源路径字符串
   * @param dstArg 目标路径字符串
   * @param logRetryCache 是否记录到重试缓存
   * @param options 重命名选项，支持OVERWRITE和TO_TRASH等
   * @return 重命名操作结果对象
   * @throws IOException 重命名过程中出现IO错误
   */
  /**
   * The new rename which has the POSIX semantic.
   */
  static RenameResult renameToInt(
      FSDirectory fsd, FSPermissionChecker pc, final String srcArg,
      final String dstArg, boolean logRetryCache, Options.Rename... options)
      throws IOException {
    String src = srcArg;
    String dst = dstArg;
    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("DIR* NameSystem.renameTo: with options={} {} to {}",
          Arrays.toString(options), src, dst);
    }

    BlocksMapUpdateInfo collectedBlocks = new BlocksMapUpdateInfo();
    // returns resolved path
    return renameTo(fsd, pc, src, dst, collectedBlocks, logRetryCache, options);
  }

  /**
   * 核心重命名方法，处理路径解析、权限检查，执行无保护重命名并记录日志
   * @see {@link #unprotectedRenameTo(FSDirectory, INodesInPath, INodesInPath,
   * long, BlocksMapUpdateInfo, Options.Rename...)}
   * @param fsd FSDirectory目录管理对象
   * @param pc 权限检查器
   * @param src 源路径字符串
   * @param dst 目标路径字符串
   * @param collectedBlocks 收集需要删除的块信息
   * @param logRetryCache 是否记录到重试缓存
   * @param options 重命名选项
   * @return 重命名操作结果对象
   * @throws IOException 重命名过程中出现IO错误
   */
  static RenameResult renameTo(FSDirectory fsd, FSPermissionChecker pc,
      String src, String dst, BlocksMapUpdateInfo collectedBlocks,
      boolean logRetryCache,Options.Rename... options)
          throws IOException {
    final INodesInPath srcIIP = fsd.resolvePath(pc, src, DirOp.WRITE_LINK);
    final INodesInPath dstIIP = fsd.resolvePath(pc, dst, DirOp.CREATE_LINK);

    if(fsd.isNonEmptyDirectory(srcIIP)) {
      DFSUtil.checkProtectedDescendants(fsd, srcIIP);
    }

    if (fsd.isPermissionEnabled()) {
      boolean renameToTrash = false;
      if (null != options &&
          Arrays.asList(options).
          contains(Options.Rename.TO_TRASH)) {
        renameToTrash = true;
      }

      if(renameToTrash) {
        // if destination is the trash directory,