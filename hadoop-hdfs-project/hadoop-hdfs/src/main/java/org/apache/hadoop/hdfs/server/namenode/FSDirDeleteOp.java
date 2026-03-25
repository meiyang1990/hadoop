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

import org.apache.hadoop.fs.InvalidPathException;
import org.apache.hadoop.fs.PathIsNotEmptyDirectoryException;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.server.namenode.FSDirectory.DirOp;
import org.apache.hadoop.hdfs.server.namenode.INode.BlocksMapUpdateInfo;
import org.apache.hadoop.hdfs.server.namenode.INode.ReclaimContext;
import org.apache.hadoop.hdfs.util.RwLockMode;
import org.apache.hadoop.util.ChunkedArrayList;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot.CURRENT_STATE_ID;
import static org.apache.hadoop.util.Time.now;

/**
 * HDFS名称节点删除操作工具类，封装文件/目录删除的核心逻辑，负责从命名空间中移除节点并收集待清理的数据块。
 * 支持增量删除大目录、一次性删除小文件/目录，配合EditLog日志回放处理也支持增量删除流程。
 */
class FSDirDeleteOp {
  /**
   * 删除指定路径，收集删除路径下所有待清理的数据块和INode信息。
   * @param fsd FSDirectory实例，维护命名空间目录树
   * @param iip 包含待删除路径所有INode的INodesInPath实例
   * @param collectedBlocks 收集删除目录下所有需要清理的数据块
   * @param removedINodes 收集需要从INodeMap移除的INode
   * @param removedUCFiles 收集需要移除的未闭合文件租约
   * @param mtime 删除操作的修改时间戳
   * @return 删除的文件总数，删除失败返回-1
   * @throws IOException 当删除操作发生IO异常时抛出
   */
  static long delete(FSDirectory fsd, INodesInPath iip,
      BlocksMapUpdateInfo collectedBlocks, List<INode> removedINodes,
      List<Long> removedUCFiles, long mtime) throws IOException {
    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("DIR* FSDirectory.delete: " + iip.getPath());
    }
    long filesRemoved = -1;
    FSNamesystem fsn = fsd.getFSNamesystem();
    fsd.writeLock();
    try {
      if (deleteAllowed(iip)) {
        List<INodeDirectory> snapshottableDirs = new ArrayList<>();
        // 检查删除路径是否涉及快照目录
        FSDirSnapshotOp.checkSnapshot(fsd, iip, snapshottableDirs);
        ReclaimContext context = new ReclaimContext(
            fsd.getBlockStoragePolicySuite(), collectedBlocks, removedINodes,
            removedUCFiles);
        if (unprotectedDelete(fsd, iip, context, mtime)) {
          // 获取删除操作导致的命名空间配额变化
          filesRemoved = context.quotaDelta().getNsDelta();
          // 从可快照目录列表中移除已删除的可快照目录
          fsn.removeSnapshottableDirs(snapshottableDirs);
        }
        // 更新数据块副本因子统计
        fsd.updateReplicationFactor(context.collectedBlocks()
                                        .toUpdateReplicationInfo());
        // 更新所有祖先目录的配额计数
        fsd.updateCount(iip, context.quotaDelta(), false);
      }
    } finally {
      fsd.writeUnlock();
    }
    return filesRemoved;
  }

  /**
   * 从命名空间删除指定路径文件/目录，权限检查、路径解析后执行删除逻辑。
   * 大目录采用增量删除，每次持锁只删除少量块平衡锁性能；小目录/文件一次性删除完成。
   * @param fsn FSNamesystem实例，维护HDFS文件系统整体命名空间
   * @param pc 权限检查器，用于验证当前用户操作权限
   * @param src 待删除路径
   * @param recursive 是否允许递归删除非空目录
   * @param logRetryCache 是否在编辑日志中记录RPC ID用于重试缓存重建
   * @return 从删除路径收集到的待清理数据块信息
   * @throws IOException 当路径非法、权限不足、目录非空未递归或IO异常时抛出
   */
  static BlocksMapUpdateInfo delete(
      FSNamesystem fsn, FSPermissionChecker pc, String src, boolean recursive,
      boolean logRetryCache) throws IOException {
    FSDirectory fsd = fsn.getFSDirectory();

    if (FSDirectory.isExactReservedName(src)) {
      throw new InvalidPathException(src);
    }

    final INodesInPath iip = fsd.resolvePath(pc, src, DirOp.WRITE_LINK);
    if (fsd.isPermissionEnabled()) {
      // 检查路径写权限和父目录全部权限
      fsd.checkPermission(pc, iip, false, null, FsAction.WRITE, null,
                          FsAction.ALL, true);
    }
    if (fsd.isNonEmptyDirectory(iip)) {
      if (!recursive) {
        throw new PathIsNotEmptyDirectoryException(
            iip.getPath() + " is non empty");
      }
      // 检查路径子节点是否包含保留路径，禁止删除保留路径
      DFSUtil.checkProtectedDescendants(fsd, iip);
    }

    return deleteInternal(fsn, iip, logRetryCache);
  }

  /**
   * 供EditLog日志回放使用的删除接口，直接在已加锁的命名空间执行删除，不记录日志。
   * 仅用于编辑日志重放场景，外部禁止直接调用。
   * @param fsd FSDirectory实例，维护命名空间目录树
   * @param iip 包含待删除路径所有INode的INodesInPath实例
   * @param mtime 删除操作的修改时间戳
   * @throws IOException 删除过程IO异常
   */
  static void deleteForEditLog(FSDirectory fsd, INodesInPath iip, long mtime)
      throws IOException {
    assert fsd.hasWriteLock();
    FSNamesystem fsn = fsd.getFSNamesystem();
    BlocksMapUpdateInfo collectedBlocks = new BlocksMapUpdateInfo();
    List<INode> removedINodes = new ChunkedArrayList<>();
    List<Long> removedUCFiles = new ChunkedArrayList<>();
    if (!deleteAllowed(iip)) {
      return;
    }
    List<INodeDirectory> snapshottableDirs = new ArrayList<>();
    // 检查删除路径是否涉及快照目录
    FSDirSnapshotOp.checkSnapshot(fsd, iip, snapshottableDirs);
    boolean filesRemoved = unprotectedDelete(fsd, iip,
        new ReclaimContext(fsd.getBlockStoragePolicySuite(),
            collectedBlocks, removedINodes, removedUCFiles),
        mtime);

    if (filesRemoved) {
      // 移除已删除可快照目录
      fsn.removeSnapshottableDirs(snapshottableDirs);
      // 移除租约并清理INode
      fsn.removeLeasesAndINodes(removedUCFiles, removedINodes, false);
      // 从块管理器移除数据块，更新安全模式总块数统计
      fsn.getBlockManager().removeBlocksAndUpdateSafemodeTotal(collectedBlocks);
    }
  }

  /**
   * 删除内部核心实现，执行删除后记录日志、更新指标、清理租约和INode。
   * 大目录采用增量删除，小目录/文件一次性删除完成。
   * @param fsn FSNamesystem实例，维护HDFS文件系统整体命名空间
   * @param iip 包含待删除路径所有INode的INodesInPath实例
   * @param logRetryCache 是否在编辑日志中记录RPC ID用于重试缓存重建
   * @return 从删除路径收集到的待清理数据块信息，删除失败返回null
   * @throws IOException 删除过程IO异常
   */
  static BlocksMapUpdateInfo deleteInternal(
      FSNamesystem fsn, INodesInPath iip, boolean logRetryCache)
      throws IOException {
    // 要求已经获取全局写锁
    assert fsn.hasWriteLock(RwLockMode.GLOBAL);
    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("DIR* NameSystem.delete: " + iip.getPath());
    }

    FSDirectory fsd = fsn.getFSDirectory();
    BlocksMapUpdateInfo collectedBlocks = new BlocksMapUpdateInfo();
    List<INode> removedINodes = new ChunkedArrayList<>();
    List<Long> removedUCFiles = new ChunkedArrayList<>();

    long mtime = now();
    // 从目录树断开目标节点，收集需要清理的信息
    long filesRemoved = delete(
        fsd, iip, collectedBlocks, removedINodes, removedUCFiles, mtime);
    if (filesRemoved < 0) {
      return null;
    }
    // 记录删除操作到编辑日志
    fsd.getEditLog().logDelete(iip.getPath(), mtime, logRetryCache);
    // 更新指标统计删除文件数
    incrDeletedFileCount(filesRemoved);

    // 移除未闭合文件租约，从INodeMap移除已删除INode
    fsn.removeLeasesAndINodes(removedUCFiles, removedINodes, true);

    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug(
          "DIR* Namesystem.delete: " + iip.getPath() +" is removed");
    }
    return collectedBlocks;
  }

  /**
   * 更新NameNode指标，增加已删除文件计数。
   * @param count 新增删除的文件数量
   */
  static void incrDeletedFileCount(long count) {
    NameNode.getNameNodeMetrics().incrFilesDeleted(count);
  }

  /**
   * 检查删除操作是否允许，校验路径存在性和根目录保护。
   * @param iip 包含待删除路径所有INode的INodesInPath实例
   * @return 允许删除返回true，否则返回false
   */
  private static boolean deleteAllowed(final INodesInPath iip) {
    if (iip.length() < 1 || iip.getLastINode() == null) {
      if (NameNode.stateChangeLog.isDebugEnabled()) {
        NameNode.stateChangeLog.debug(
            "DIR* FSDirectory.unprotectedDelete: failed to remove "
                + iip.getPath() + " because it does not exist");
      }
      return false;
    } else if (iip.length() == 1) { // src is the root
      NameNode.stateChangeLog.warn(
          "DIR* FSDirectory.unprotectedDelete: failed to remove " +
              iip.getPath() + " because the root is not allowed to be deleted");
      return false;
    }
    return true;
  }

  /**
   * 无保护删除核心逻辑，直接从命名空间移除节点，收集待清理块和INode。
   * 要求已经持有写锁，前置权限和删除允许检查。
   * @param fsd FSDirectory实例，维护命名空间目录树
   * @param iip 从路径解析得到的所有INode
   * @param reclaimContext 上下文对象，收集待删除块和INode信息
   * @param mtime 删除操作的修改时间戳
   * @return 删除成功返回true，否则返回false
   */
  private static boolean unprotectedDelete(FSDirectory fsd, INodesInPath iip,
      ReclaimContext reclaimContext, long mtime) {
    assert fsd.hasWriteLock();

    // 获取待删除目标节点
    INode targetNode = iip.getLastINode();
    if (targetNode == null) {
      return false;
    }

    // 记录修改到最新快照
    final int latestSnapshot = iip.getLatestSnapshotId();
    targetNode.recordModification(latestSnapshot);

    // 从父目录移除目标节点
    long removed = fsd.removeLastINode(iip);
    if (removed == -1) {
      return false;
    }

    // 更新父目录修改时间
    final INodeDirectory parent = targetNode.getParent();
    parent.updateModificationTime(mtime, latestSnapshot);

    // 收集需要清理的数据块和INode，处理快照场景
    if (!targetNode.isInLatestSnapshot(latestSnapshot)) {
      // 节点不在最新快照中，完整销毁收集所有块
      targetNode.destroyAndCollectBlocks(reclaimContext);
    } else {
      // 节点存在于快照，只清理当前状态下的子树，保留快照版本数据
      targetNode.cleanSubtree(reclaimContext, CURRENT_STATE_ID, latestSnapshot);
    }

    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("DIR* FSDirectory.unprotectedDelete: "
          + iip.getPath() + " is removed");
    }
    return true;
  }
}