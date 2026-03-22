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

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.fs.InvalidPathException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.FSLimitException;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReportListing;
import org.apache.hadoop.hdfs.protocol.SnapshotException;
import org.apache.hadoop.hdfs.protocol.SnapshottableDirectoryStatus;
import org.apache.hadoop.hdfs.protocol.SnapshotStatus;
import org.apache.hadoop.hdfs.server.namenode.FSDirectory.DirOp;
import org.apache.hadoop.hdfs.server.namenode.snapshot.DirectorySnapshottableFeature;
import org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot;
import org.apache.hadoop.hdfs.server.namenode.snapshot.SnapshotManager;
import org.apache.hadoop.hdfs.util.ReadOnlyList;
import org.apache.hadoop.util.ChunkedArrayList;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * HDFS NameNode目录快照操作工具类，封装了目录级别所有快照相关操作的核心逻辑
 * 作为FSDirectory的辅助类，提供快照创建、删除、重命名、diff对比等操作的入口，
 * 权限校验、目录锁管理，并委托SnapshotManager执行具体快照处理
 */
class FSDirSnapshotOp {
  public static final Logger LOG =
      LoggerFactory.getLogger(FSDirSnapshotOp.class);

  /**
   * 验证快照名称是否合法，检查是否包含路径分隔符并符合HDFS名称长度限制
   * @param fsd 文件目录管理器
   * @param snapshotName 待验证的快照名称
   * @param path 快照所在目录路径
   * @throws FSLimitException.PathComponentTooLongException 当名称长度超出限制时抛出
   */
  static void verifySnapshotName(FSDirectory fsd, String snapshotName,
      String path)
      throws FSLimitException.PathComponentTooLongException {
    if (snapshotName.contains(Path.SEPARATOR)) {
      throw new HadoopIllegalArgumentException(
          "Snapshot name cannot contain \"" + Path.SEPARATOR + "\"");
    }
    final byte[] bytes = DFSUtil.string2Bytes(snapshotName);
    fsd.verifyINodeName(bytes);
    fsd.verifyMaxComponentLength(bytes, path);
  }

  /**
   * 允许指定目录开启快照功能，将目录标记为可快照
   * @param fsd 文件目录管理器
   * @param snapshotManager 快照管理器
   * @param path 需要开启快照功能的目录路径
   * @throws IOException 操作失败时抛出IO异常
   */
  static void allowSnapshot(FSDirectory fsd, SnapshotManager snapshotManager,
                            String path) throws IOException {
    fsd.writeLock();
    try {
      snapshotManager.setSnapshottable(path, true);
    } finally {
      fsd.writeUnlock();
    }
    fsd.getEditLog().logAllowSnapshot(path);
  }

  /**
   * 禁止指定目录开启快照功能，取消目录的可快照标记
   * @param fsd 文件目录管理器
   * @param snapshotManager 快照管理器
   * @param path 需要禁用快照功能的目录路径
   * @throws IOException 操作失败时抛出IO异常
   */
  static void disallowSnapshot(
      FSDirectory fsd, SnapshotManager snapshotManager,
      String path) throws IOException {
    fsd.writeLock();
    try {
      snapshotManager.resetSnapshottable(path);
    } finally {
      fsd.writeUnlock();
    }
    fsd.getEditLog().logDisallowSnapshot(path);
  }

  /**
   * 在指定可快照目录创建新快照
   * @param fsd 文件目录管理器
   * @param pc 权限检查器
   * @param snapshotManager 快照管理器
   * @param snapshotRoot 要创建快照的根目录路径
   * @param snapshotName 快照名称
   * @param logRetryCache 是否在编辑日志中记录RPC ID用于重试缓存重建
   * @return 创建完成的快照完整路径
   * @throws IOException 操作失败时抛出IO异常
   */
  static String createSnapshot(
      FSDirectory fsd, FSPermissionChecker pc, SnapshotManager snapshotManager,
      String snapshotRoot, String snapshotName, boolean logRetryCache)
      throws IOException {
    final INodesInPath iip = fsd.resolvePath(pc, snapshotRoot, DirOp.WRITE);
    if (fsd.isPermissionEnabled()) {
      fsd.checkOwner(pc, iip);
    }

    if (snapshotName == null || snapshotName.isEmpty()) {
      snapshotName = Snapshot.generateDefaultSnapshotName();
    } else if (!DFSUtil.isValidNameForComponent(snapshotName)) {
      throw new InvalidPathException("Invalid snapshot name: " + snapshotName);
    }

    String snapshotPath;
    verifySnapshotName(fsd, snapshotName, snapshotRoot);
    // 记录快照创建时间
    final long now = Time.now();
    fsd.writeLock();
    try {
      snapshotPath = snapshotManager.createSnapshot(
          fsd.getFSNamesystem().getLeaseManager(),
          iip, snapshotRoot, snapshotName, now);
    } finally {
      fsd.writeUnlock();
    }
    fsd.getEditLog().logCreateSnapshot(snapshotRoot, snapshotName,
        logRetryCache, now);
    LOG.info("Created Snapshot for SnapshotRoot {}", snapshotRoot);
    return snapshotPath;
  }

  /**
   * 重命名指定目录下的已有快照
   * @param fsd 文件目录管理器
   * @param pc 权限检查器
   * @param snapshotManager 快照管理器
   * @param path 快照所在根目录路径
   * @param snapshotOldName 快照原名称
   * @param snapshotNewName 快照新名称
   * @param logRetryCache 是否在编辑日志中记录RPC ID用于重试缓存重建
   * @throws IOException 操作失败时抛出IO异常
   */
  static void renameSnapshot(FSDirectory fsd, FSPermissionChecker pc,
      SnapshotManager snapshotManager, String path, String snapshotOldName,
      String snapshotNewName, boolean logRetryCache) throws IOException {
    final INodesInPath iip = fsd.resolvePath(pc, path, DirOp.WRITE);
    if (fsd.isPermissionEnabled()) {
      fsd.checkOwner(pc, iip);
    }
    verifySnapshotName(fsd, snapshotNewName, path);
    // 记录快照修改时间
    final long now = Time.now();
    fsd.writeLock();
    try {
      snapshotManager.renameSnapshot(iip, path, snapshotOldName,
          snapshotNewName, now);
    } finally {
      fsd.writeUnlock();
    }
    fsd.getEditLog().logRenameSnapshot(path, snapshotOldName,
        snapshotNewName, logRetryCache, now);
    LOG.info("Snapshot renamed from {} to {} for SnapshotRoot {}",
        snapshotOldName, snapshotNewName, path);
  }

  /**
   * 获取整个集群中所有可快照目录的列表
   * @param fsd 文件目录管理器
   * @param pc 权限检查器
   * @param snapshotManager 快照管理器
   * @return 可快照目录状态数组，仅对当前用户可见的目录会被返回
   * @throws IOException 获取列表失败时抛出IO异常
   */
  static SnapshottableDirectoryStatus[] getSnapshottableDirListing(
      FSDirectory fsd, FSPermissionChecker pc, SnapshotManager snapshotManager)
      throws IOException {
    fsd.readLock();
    try {
      final String user = pc.isSuperUser()? null : pc.getUser();
      return snapshotManager.getSnapshottableDirListing(user);
    } finally {
      fsd.readUnlock();
    }
  }

  /**
   * 获取指定可快照目录下所有已创建快照的列表
   * @param fsd 文件目录管理器
   * @param pc 权限检查器
   * @param snapshotManager 快照管理器
   * @param path 可快照目录路径
   * @return 该目录下所有快照状态数组
   * @throws IOException 获取列表失败时抛出IO异常
   */
  static SnapshotStatus[] getSnapshotListing(
      FSDirectory fsd, FSPermissionChecker pc, SnapshotManager snapshotManager,
      String path)
      throws IOException {
    fsd.readLock();
    try {
      INodesInPath iip = fsd.getINodesInPath(path, DirOp.READ);
      if (fsd.isPermissionEnabled()) {
        fsd.checkPathAccess(pc, iip, FsAction.READ);
      }
      return snapshotManager.getSnapshotListing(iip);
    } finally {
      fsd.readUnlock();
    }
  }

  /**
   * 获取同一个目录两个快照之间的完整差异报告
   * @param fsd 文件目录管理器
   * @param pc 权限检查器
   * @param snapshotManager 快照管理器
   * @param path 快照所在目录路径
   * @param fromSnapshot 起始快照名称，为空代表当前目录状态
   * @param toSnapshot 结束快照名称，为空代表当前目录状态
   * @return 完整的快照差异报告
   * @throws IOException 获取差异失败时抛出IO异常
   */
  static SnapshotDiffReport getSnapshotDiffReport(FSDirectory fsd,
      FSPermissionChecker pc, SnapshotManager snapshotManager, String path,
      String fromSnapshot, String toSnapshot) throws IOException {
    SnapshotDiffReport diffs;
    fsd.readLock();
    try {
      INodesInPath iip = fsd.resolvePath(pc, path, DirOp.READ);
      if (fsd.isPermissionEnabled()) {
        checkSubtreeReadPermission(fsd, pc, path, fromSnapshot);
        checkSubtreeReadPermission(fsd, pc, path, toSnapshot);
      }
      diffs = snapshotManager.diff(iip, path, fromSnapshot, toSnapshot);
    } finally {
      fsd.readUnlock();
    }
    return diffs;
  }

  /**
   * 分页获取同一个目录两个快照之间的差异报告，用于大数据量差分时避免返回过多数据
   * @param fsd 文件目录管理器
   * @param pc 权限检查器
   * @param snapshotManager 快照管理器
   * @param path 快照所在目录路径
   * @param fromSnapshot 起始快照名称，为空代表当前目录状态
   * @param toSnapshot 结束快照名称，为空代表当前目录状态
   * @param startPath 从该路径开始返回后续差异项
   * @param index 当前分页的索引
   * @param snapshotDiffReportLimit 单次返回差异项数量限制
   * @return 分页后的差异报告
   * @throws IOException 获取差异失败时抛出IO异常
   */
  static SnapshotDiffReportListing getSnapshotDiffReportListing(FSDirectory fsd,
      FSPermissionChecker pc, SnapshotManager snapshotManager, String path,
      String fromSnapshot, String toSnapshot, byte[] startPath, int index,
      int snapshotDiffReportLimit) throws IOException {
    SnapshotDiffReportListing diffs;
    fsd.readLock();
    try {
      INodesInPath iip = fsd.resolvePath(pc, path, DirOp.READ);
      if (fsd.isPermissionEnabled()) {
        checkSubtreeReadPermission(fsd, pc, path, fromSnapshot);
        checkSubtreeReadPermission(fsd, pc, path, toSnapshot);
      }
      diffs = snapshotManager
          .diff(iip, path, fromSnapshot, toSnapshot, startPath, index,
              snapshotDiffReportLimit);
    } catch (Exception e) {
      throw e;
    } finally {
      fsd.readUnlock();
    }
    return diffs;
  }

  /**
   * 根据目标文件路径，获取该文件在所有已创建快照中的完整路径集合
   * @param fsd 文件目录管理器
   * @param lsf 可快照目录特征列表
   * @param file 目标文件完整路径
   * @return 该文件在所有快照中的完整路径集合，仅包含存在的文件路径
   * @throws IOException 获取文件信息失败时抛出IO异常
   */
  static Collection<String> getSnapshotFiles(FSDirectory fsd,
      List<DirectorySnapshottableFeature> lsf,
      String file) throws IOException {
    ArrayList<String> snaps = new ArrayList<>();
    for (DirectorySnapshottableFeature sf : lsf) {
      // 遍历每个可快照父目录
      final ReadOnlyList<Snapshot> lsnap = sf.getSnapshotList();
      for (Snapshot s : lsnap) {
        // 遍历父目录下每个快照
        final String dirName = s.getRoot().getRootFullPathName();
        if (!file.startsWith(dirName)) {
          // 文件不在当前快照根目录下，无需继续检查
          break;
        }
        String snapname = s.getRoot().getFullPathName();
        if (dirName.equals(Path.SEPARATOR)) { // 处理根目录特殊情况
          snapname += Path.SEPARATOR;
        }
        snapname += file.substring(file.indexOf(dirName) + dirName.length());
        HdfsFileStatus stat =
            fsd.getFSNamesystem().getFileInfo(snapname, true, false, false);
        if (stat != null) {
          snaps.add(snapname);
        }
      }
    }
    return snaps;
  }

  /**
   * 删除指定可快照目录下的指定快照
   * @param fsd 文件目录管理器
   * @param pc 权限检查器
   * @param snapshotManager 快照管理器
   * @param snapshotRoot 快照所在根目录路径
   * @param snapshotName 待删除快照名称
   * @param logRetryCache 是否在编辑日志中记录RPC ID用于重试缓存重建
   * @return 需要更新块映射信息的块集合，用于后续清理
   * @throws IOException 删除失败时抛出IO异常
   */
  static INode.BlocksMapUpdateInfo deleteSnapshot(
      FSDirectory fsd, FSPermissionChecker pc, SnapshotManager snapshotManager,
      String snapshotRoot, String snapshotName, boolean logRetryCache)
      throws IOException {
    final INodesInPath iip = fsd.resolvePath(pc, snapshotRoot, DirOp.WRITE);
    if (fsd.isPermissionEnabled()) {
      fsd.checkOwner(pc, iip);
    }

    // 记录快照删除时间
    final long now = Time.now();
    final INode.BlocksMapUpdateInfo collectedBlocks = deleteSnapshot(
        fsd, snapshotManager, iip, snapshotName, now, snapshotRoot,
        logRetryCache);
    LOG.info("Snapshot {} deleted for SnapshotRoot {}",
        snapshotName, snapshotRoot);
    return collectedBlocks;
  }

  /**
   * 执行删除快照的实际操作，处理INode删除、 quota更新和块回收
   * @param fsd 文件目录管理器
   * @param snapshotManager 快照管理器
   * @param iip 快照根目录的INode路径
   * @param snapshotName 待删除快照名称
   * @param now 删除操作时间戳
   * @param snapshotRoot 快照根目录路径
   * @param logRetryCache 是否在编辑日志中记录RPC ID用于重试缓存重建
   * @return 需要更新块映射信息的块集合，用于后续清理
   * @throws IOException 删除失败时抛出IO异常
   */
  static INode.BlocksMapUpdateInfo deleteSnapshot(
      FSDirectory fsd, SnapshotManager snapshotManager, INodesInPath iip,
      String snapshotName, long now, String snapshotRoot, boolean logRetryCache)
      throws IOException {
    INode.BlocksMapUpdateInfo collectedBlocks = new INode.BlocksMapUpdateInfo();
    ChunkedArrayList<INode> removedINodes = new ChunkedArrayList<>();
    INode.ReclaimContext context = new INode.ReclaimContext(
        fsd.getBlockStoragePolicySuite(), collectedBlocks, removedINodes, null);
    fsd.writeLock();
    try {
      snapshotManager.deleteSnapshot(iip, snapshotName, context, now);
      fsd.updateCount(iip, context.quotaDelta(), false);
      fsd.removeFromInodeMap(removedINodes);
      fsd.updateReplicationFactor(context.collectedBlocks()
                                      .toUpdateReplicationInfo());
    } finally {
      fsd.writeUnlock();
    }
    removedINodes.clear();
    fsd.getEditLog().logDeleteSnapshot(snapshotRoot, snapshotName