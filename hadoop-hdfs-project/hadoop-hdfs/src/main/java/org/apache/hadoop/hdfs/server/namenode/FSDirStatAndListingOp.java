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

import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.DirectoryListingStartAfterNotFoundException;
import org.apache.hadoop.fs.FileEncryptionInfo;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.QuotaUsage;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.HdfsLocatedFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.SnapshotException;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.namenode.FSDirectory.DirOp;
import org.apache.hadoop.hdfs.server.namenode.snapshot.DirectorySnapshottableFeature;
import org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot;
import org.apache.hadoop.hdfs.util.ReadOnlyList;
import org.apache.hadoop.hdfs.util.RwLockMode;
import org.apache.hadoop.security.AccessControlException;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.EnumSet;

import static org.apache.hadoop.util.Time.now;

/**
 * HDFS NameNode 文件状态统计与目录列出操作工具类
 * 封装了获取文件信息、目录列表、内容摘要、配额使用、块位置信息等各类查询操作
 * 为NameNode的客户端查询请求提供统一的内部实现
 */
class FSDirStatAndListingOp {
  /**
   * 内部获取目录列表的入口方法，处理路径解析和权限检查后调用实际列出逻辑
   * @param fsd 文件目录管理器
   * @param pc 权限检查器
   * @param srcArg 源目录路径字符串
   * @param startAfter 列出起始位置（从该名称之后开始返回）
   * @param needLocation 是否需要返回块位置信息
   * @return 目录列表结果
   * @throws IOException 路径解析、权限检查或列出过程中的IO异常
   */
  static DirectoryListing getListingInt(FSDirectory fsd, FSPermissionChecker pc,
      final String srcArg, byte[] startAfter, boolean needLocation)
      throws IOException {
    final INodesInPath iip = fsd.resolvePath(pc, srcArg, DirOp.READ);

    // 当startAfter是完整路径时，解析提取出最后一层节点名称，只有需要时才处理
    if (startAfter.length > 0 && startAfter[0] == Path.SEPARATOR_CHAR) {
      final String startAfterString = DFSUtil.bytes2String(startAfter);
      if (FSDirectory.isReservedName(startAfterString)) {
        try {
          byte[][] components = INode.getPathComponents(startAfterString);
          components = FSDirectory.resolveComponents(components, fsd);
          startAfter = components[components.length - 1];
        } catch (IOException e) {
          // 可能对应节点已被删除
          throw new DirectoryListingStartAfterNotFoundException(
              "Can't find startAfter " + startAfterString);
        }
      }
    }

    if (fsd.isPermissionEnabled()) {
      if (iip.getLastINode() != null && iip.getLastINode().isDirectory()) {
        fsd.checkPathAccess(pc, iip, FsAction.READ_EXECUTE);
      }
    }
    return getListing(fsd, iip, startAfter, needLocation);
  }

  /**
   * 获取指定路径文件的文件信息
   * @param fsd 文件目录管理器
   * @param pc 权限检查器
   * @param srcArg 文件路径字符串
   * @param resolveLink 如果路径指向符号链接，是否需要解析链接
   * @param needLocation 结果中是否需要包含块位置信息
   * @param needBlockToken 块位置信息中是否需要包含块令牌
   * @return 文件信息对象，文件不存在时返回null
   * @throws IOException 路径解析、权限检查过程中的IO异常
   */
  static HdfsFileStatus getFileInfo(FSDirectory fsd, FSPermissionChecker pc,
      String srcArg, boolean resolveLink, boolean needLocation,
      boolean needBlockToken) throws IOException {
    DirOp dirOp = resolveLink ? DirOp.READ : DirOp.READ_LINK;
    final INodesInPath iip;
    if (pc.isSuperUser()) {
      // 超级用户遇到祖先权限问题时，按当前文件系统契约返回null而非抛出异常
      try {
        iip = fsd.resolvePath(pc, srcArg, dirOp);
        pc.checkSuperuserPrivilege(iip.getPath());
      } catch (AccessControlException ace) {
        return null;
      }
    } else {
      iip = fsd.resolvePath(pc, srcArg, dirOp);
    }
    return getFileInfo(fsd, iip, needLocation, needBlockToken);
  }

  /**
   * 检查指定路径的文件是否已完成关闭（不再处于构建状态）
   * @param fsd 文件目录管理器
   * @param pc 权限检查器
   * @param src 文件路径字符串
   * @return 文件已关闭返回true，否则返回false
   * @throws IOException 路径解析、权限检查过程中的IO异常
   */
  static boolean isFileClosed(FSDirectory fsd, FSPermissionChecker pc,
      String src) throws IOException {
    final INodesInPath iip = fsd.resolvePath(pc, src, DirOp.READ);
    return !INodeFile.valueOf(iip.getLastINode(), src).isUnderConstruction();
  }

  /**
   * 获取指定路径下的内容摘要（包含存储空间、文件数量统计）
   * @param fsd 文件目录管理器
   * @param pc 权限检查器
   * @param src 目标路径字符串
   * @return 目标路径的内容摘要
   * @throws IOException 路径解析、权限检查或统计过程中的IO异常
   */
  static ContentSummary getContentSummary(
      FSDirectory fsd, FSPermissionChecker pc, String src) throws IOException {
    final INodesInPath iip = fsd.resolvePath(pc, src, DirOp.READ_LINK);
    if (fsd.isPermissionEnabled() && fsd.isPermissionContentSummarySubAccess()) {
      fsd.checkPermission(pc, iip, false, null, null, null,
          FsAction.READ_EXECUTE);
      pc = null;
    }
    // 遍历子目录过程中会逐次检查权限
    return getContentSummaryInt(fsd, pc, iip);
  }

  /**
   * 获取指定文件指定偏移范围的块位置信息
   * @see ClientProtocol#getBlockLocations(String, long, long)
   * @param fsd 文件目录管理器
   * @param pc 权限检查器
   * @param src 文件路径字符串
   * @param offset 起始偏移量
   * @param length 请求长度
   * @param needBlockToken 是否需要生成块令牌
   * @return 包含是否需要更新访问时间标记和块位置信息的结果对象
   * @throws IOException 路径解析、参数检查或查询过程中的IO异常
   */
  static GetBlockLocationsResult getBlockLocations(
      FSDirectory fsd, FSPermissionChecker pc, String src, long offset,
      long length, boolean needBlockToken) throws IOException {
    Preconditions.checkArgument(offset >= 0,
        "Negative offset is not supported. File: " + src);
    Preconditions.checkArgument(length >= 0,
        "Negative length is not supported. File: " + src);
    BlockManager bm = fsd.getBlockManager();
    fsd.readLock();
    try {
      // 先解析路径，后续统一进行权限检查
      final INodesInPath iip = fsd.resolvePath(null, src, DirOp.READ);
      src = iip.getPath();
      final INodeFile inode = INodeFile.valueOf(iip.getLastINode(), src);
      if (fsd.isPermissionEnabled()) {
        fsd.checkUnreadableBySuperuser(pc, iip);
        fsd.checkPathAccess(pc, iip, FsAction.READ);
      }

      // 根据是否快照计算文件大小
      final long fileSize = iip.isSnapshot()
          ? inode.computeFileSize(iip.getPathSnapshotId())
          : inode.computeFileSizeNotIncludingLastUcBlock();

      boolean isUc = inode.isUnderConstruction();
      if (iip.isSnapshot()) {
        // 快照文件需要确保返回长度不超过快照中记录的文件大小
        length = Math.min(length, fileSize - offset);
        isUc = false;
      }

      // 获取文件加密信息和纠删码策略
      final FileEncryptionInfo feInfo =
          FSDirEncryptionZoneOp.getFileEncryptionInfo(fsd, iip);
      final ErasureCodingPolicy ecPolicy = FSDirErasureCodingOp.
          unprotectedGetErasureCodingPolicy(fsd.getFSNamesystem(), iip);

      // 从块管理器创建请求范围的位置信息
      final LocatedBlocks blocks = bm.createLocatedBlocks(
          inode.getBlocks(iip.getPathSnapshotId()), fileSize, isUc, offset,
          length, needBlockToken, iip.isSnapshot(), feInfo, ecPolicy);

      // 判断是否需要更新访问时间
      final long now = now();
      boolean updateAccessTime = fsd.isAccessTimeSupported()
          && !iip.isSnapshot()
          && now > inode.getAccessTime() + fsd.getAccessTimePrecision();
      return new GetBlockLocationsResult(updateAccessTime, blocks, iip);
    } finally {
      fsd.readUnlock();
    }
  }

  /**
   * 存储策略ID继承逻辑：节点指定了策略则用节点自身，否则继承父节点策略
   * @param inodePolicy 当前节点的存储策略ID
   * @param parentPolicy 父节点的存储策略ID
   * @return 最终生效的存储策略ID
   */
  private static byte getStoragePolicyID(byte inodePolicy, byte parentPolicy) {
    return inodePolicy != HdfsConstants.BLOCK_STORAGE_POLICY_ID_UNSPECIFIED
        ? inodePolicy : parentPolicy;
  }

  /**
   * 获取指定目录的分页列表，满足数量限制后停止返回剩余标记
   * 停止条件：1.已添加lsLimit个文件；2.启用位置信息且位置数量已达到lsLimit限制
   * @param fsd 文件目录管理器
   * @param iip 路径对应的INodes集合
   * @param startAfter 从该名称之后开始列出
   * @param needLocation 是否需要返回块位置信息
   * @return 从startAfter开始的分页目录列表
   * @throws IOException 查询过程中的IO异常
   */
  private static DirectoryListing getListing(FSDirectory fsd, INodesInPath iip,
      byte[] startAfter, boolean needLocation)
      throws IOException {
    if (FSDirectory.isExactReservedName(iip.getPathComponents())) {
      return getReservedListing(fsd);
    }

    fsd.readLock();
    try {
      if (iip.isDotSnapshotDir()) {
        return getSnapshotsListing(fsd, iip, startAfter);
      }
      final int snapshot = iip.getPathSnapshotId();
      final INode targetNode = iip.getLastINode();
      if (targetNode == null) {
        return null;
      }

      byte parentStoragePolicy = targetNode.getStoragePolicyID();

      if (!targetNode.isDirectory()) {
        // 目标不是目录，返回文件自身的状态信息
        return new DirectoryListing(
            new HdfsFileStatus[]{ createFileStatus(
                fsd, iip, null, parentStoragePolicy, needLocation, false)
            }, 0);
      }

      // 获取目录子节点，计算起始位置和本次返回数量
      final INodeDirectory dirInode = targetNode.asDirectory();
      final ReadOnlyList<INode> contents = dirInode.getChildrenList(snapshot);
      int startChild = INodeDirectory.nextChild(contents, startAfter);
      int totalNumChildren = contents.size();
      int numOfListing = Math.min(totalNumChildren - startChild,
          fsd.getLsLimit());
      int locationBudget = fsd.getLsLimit();
      int listingCnt = 0;
      HdfsFileStatus listing[] = new HdfsFileStatus[numOfListing];
      // 遍历子节点生成文件状态，控制响应大小不超过位置数量限制
      for (int i = 0; i < numOfListing && locationBudget > 0; i++) {
        INode child = contents.get(startChild+i);
        byte childStoragePolicy =
            !child.isSymlink()
                ? getStoragePolicyID(child.getLocalStoragePolicyID(),
                    parentStoragePolicy)
            : parentStoragePolicy;
        listing[i] = createFileStatus(fsd, iip, child, childStoragePolicy,
            needLocation, false);
        listingCnt++;
        if (listing[i] instanceof HdfsLocatedFileStatus) {
          // 达到位置数量限制后停止，避免响应体积过大
          LocatedBlocks blks =
              ((HdfsLocatedFileStatus) listing[i]).getLocatedBlocks();
          if (blks != null) {
            ErasureCodingPolicy ecPolicy = listing[i].getErasureCodingPolicy();
            if (ecPolicy != null && !ecPolicy.isReplicationPolicy()) {
              // 纠删码文件按数据单元+校验单元总数估算位置数量
              locationBudget -= blks.locatedBlockCount() *
                  (ecPolicy.getNumDataUnits() + ecPolicy.getNumParityUnits());
            } else {
              // 副本文件按副本系数估算位置数量
              locationBudget -=
                  blks.locatedBlockCount() * listing[i].getReplication();
            }
          }
        }
      }
      // 如果因为位置限制提前停止，截断结果数组
      if (listingCnt < numOfListing) {
          listing = Arrays.copyOf(listing, listingCnt);
      }
      return new DirectoryListing(
          listing, totalNumChildren-startChild-listingCnt);
    } finally {
      fsd.readUnlock();
    }
  }

  /**
   * 获取可快照目录下所有快照的列表
   * @param fsd 文件目录管理器
   * @param iip /.snapshot路径对应的INodes集合
   * @param startAfter 从该名称之后开始列出
   * @return 快照列表结果
   * @throws IOException 路径解析或权限检查异常
   */
  private static DirectoryListing getSnapshotsListing(
      FSDirectory fsd, INodesInPath iip, byte[] startAfter)
      throws IOException {
    Preconditions.checkState(fsd.hasReadLock());
    Preconditions.checkArgument(iip.isDotSnapshotDir(),
        "%s does not end with %s",
        iip.getPath(), HdfsConstants.SEPARATOR_DOT_SNAPSHOT_DIR);
    // 去掉末尾的.snapshot节点，获取父目录
    iip = iip.getParentINodesInPath();
    final String dirPath = iip.getPath();
    final INode node = iip.getLastINode();
    final INodeDirectory dirNode = INodeDirectory.valueOf(node, dirPath);
    final DirectorySnapshottableFeature sf = dirNode.getDirectorySnapshottableFeature();
    if (sf == null) {
      throw new SnapshotException(
          "Directory is not a snapshottable directory: " + dirPath);
    }
    // 二分查找找到起始位置
    final ReadOnlyList<Snapshot> snapshots = sf.getSnapshotList();
    int skipSize = ReadOnlyList.Util.binarySearch(snapshots, startAfter);
    skipSize = skipSize < 0 ? -skipSize - 1 : skipSize + 1;
    // 按lsLimit分页返回
    int numOfListing = Math.min(snapshots.size() - skipSize, fsd.getLsLimit());
    final HdfsFileStatus listing[] = new HdfsFileStatus[numOfListing];
    for (int i = 0; i < numOfListing; i++) {
      Snapshot.Root sRoot = snapshots.get(i + skipSize).getRoot();
      listing[i] = createFileStatus(fsd, iip, sRoot,
          HdfsConstants.BLOCK_STORAGE_POLICY_ID_UNSPECIFIED, false, false);
    }
    return new DirectoryListing(
        listing, snapshots.size() - skipSize - numOfListing);
  }

  /**