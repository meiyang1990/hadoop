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
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.InvalidPathException;
import org.apache.hadoop.fs.PathIsNotDirectoryException;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.fs.XAttr;
import org.apache.hadoop.fs.XAttrSetFlag;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.QuotaExceededException;
import org.apache.hadoop.hdfs.protocol.SnapshotAccessControlException;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockStoragePolicySuite;
import org.apache.hadoop.hdfs.server.namenode.FSDirectory.DirOp;
import org.apache.hadoop.hdfs.util.EnumCounters;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.util.Lists;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_QUOTA_BY_STORAGETYPE_ENABLED_KEY;

/**
 * HDFS NameNode 文件系统属性操作工具类
 * 提供文件/目录各类元数据属性的修改、查询操作，包括权限、属主、修改时间、副本数、存储策略、配额等
 */
public class FSDirAttrOp {
  /**
   * 设置文件/目录的权限
   * @param fsd 文件目录管理器
   * @param pc 权限检查器
   * @param src 目标路径
   * @param permission 要设置的权限
   * @return 操作后的文件状态信息，用于审计日志
   * @throws IOException 操作失败时抛出异常
   */
  static FileStatus setPermission(
      FSDirectory fsd, FSPermissionChecker pc, final String src,
      FsPermission permission) throws IOException {
    if (FSDirectory.isExactReservedName(src)) {
      throw new InvalidPathException(src);
    }
    INodesInPath iip;
    boolean changed;
    fsd.writeLock();
    try {
      iip = fsd.resolvePath(pc, src, DirOp.WRITE);
      fsd.checkOwner(pc, iip);
      changed = unprotectedSetPermission(fsd, iip, permission);
    } finally {
      fsd.writeUnlock();
    }
    if (changed) {
      fsd.getEditLog().logSetPermissions(iip.getPath(), permission);
    }
    return fsd.getAuditFileInfo(iip);
  }

  /**
   * 设置文件/目录的属主和属组
   * @param fsd 文件目录管理器
   * @param pc 权限检查器
   * @param src 目标路径
   * @param username 新用户名，null表示不修改
   * @param group 新组名，null表示不修改
   * @return 操作后的文件状态信息，用于审计日志
   * @throws IOException 操作失败时抛出异常
   */
  static FileStatus setOwner(
      FSDirectory fsd, FSPermissionChecker pc, String src, String username,
      String group) throws IOException {
    if (FSDirectory.isExactReservedName(src)) {
      throw new InvalidPathException(src);
    }
    INodesInPath iip;
    boolean changed;
    fsd.writeLock();
    try {
      iip = fsd.resolvePath(pc, src, DirOp.WRITE);
      fsd.checkOwner(pc, iip);
      // At this point, the user must be either owner or super user.
      // superuser: can change owner to a different user,
      // change owner group to any group
      // owner: can't change owner to a different user but can change owner
      // group to different group that the user belongs to.
      if ((username != null && !pc.getUser().equals(username)) ||
          (group != null && !pc.isMemberOfGroup(group))) {
        try {
          // check if the user is superuser
          pc.checkSuperuserPrivilege(iip.getPath());
        } catch (AccessControlException e) {
          if (username != null && !pc.getUser().equals(username)) {
            throw new AccessControlException("User " + pc.getUser()
                + " is not a super user (non-super user cannot change owner).");
          }
          if (group != null && !pc.isMemberOfGroup(group)) {
            throw new AccessControlException(
                "User " + pc.getUser() + " does not belong to " + group);
          }
        }
      }
      changed = unprotectedSetOwner(fsd, iip, username, group);
    } finally {
      fsd.writeUnlock();
    }
    if (changed) {
      fsd.getEditLog().logSetOwner(iip.getPath(), username, group);
    }
    return fsd.getAuditFileInfo(iip);
  }

  /**
   * 设置文件/目录的修改时间和访问时间
   * @param fsd 文件目录管理器
   * @param pc 权限检查器
   * @param src 目标路径
   * @param mtime 新修改时间，-1表示不修改
   * @param atime 新访问时间，-1表示不修改
   * @return 操作后的文件状态信息，用于审计日志
   * @throws IOException 操作失败时抛出异常
   */
  static FileStatus setTimes(
      FSDirectory fsd, FSPermissionChecker pc, String src, long mtime,
      long atime) throws IOException {
    INodesInPath iip;
    fsd.writeLock();
    try {
      iip = fsd.resolvePath(pc, src, DirOp.WRITE);
      // Write access is required to set access and modification times
      if (fsd.isPermissionEnabled()) {
        fsd.checkPathAccess(pc, iip, FsAction.WRITE);
      }
      boolean changed = unprotectedSetTimes(fsd, iip, mtime, atime, true);
      if (changed) {
        fsd.getEditLog().logTimes(iip.getPath(), mtime, atime);
      }
    } finally {
      fsd.writeUnlock();
    }
    return fsd.getAuditFileInfo(iip);
  }

  /**
   * 设置文件的副本数
   * @param fsd 文件目录管理器
   * @param pc 权限检查器
   * @param bm 块管理器
   * @param src 目标路径
   * @param replication 新副本数
   * @return 是否修改成功，目标不是文件时返回false
   * @throws IOException 操作失败时抛出异常
   */
  static boolean setReplication(
      FSDirectory fsd, FSPermissionChecker pc, BlockManager bm, String src,
      final short replication) throws IOException {
    bm.verifyReplication(src, replication, null);
    final boolean isFile;
    fsd.writeLock();
    try {
      final INodesInPath iip = fsd.resolvePath(pc, src, DirOp.WRITE);
      if (fsd.isPermissionEnabled()) {
        fsd.checkPathAccess(pc, iip, FsAction.WRITE);
      }

      final BlockInfo[] blocks = unprotectedSetReplication(fsd, iip,
                                                           replication);
      isFile = blocks != null;
      if (isFile) {
        fsd.getEditLog().logSetReplication(iip.getPath(), replication);
      }
    } finally {
      fsd.writeUnlock();
    }
    return isFile;
  }

  /**
   * 取消文件/目录已设置的存储策略，恢复继承父目录策略
   * @param fsd 文件目录管理器
   * @param pc 权限检查器
   * @param bm 块管理器
   * @param src 目标路径
   * @return 操作后的文件状态信息，用于审计日志
   * @throws IOException 操作失败时抛出异常
   */
  static FileStatus unsetStoragePolicy(FSDirectory fsd, FSPermissionChecker pc,
      BlockManager bm, String src) throws IOException {
    return setStoragePolicy(fsd, pc, bm, src,
        HdfsConstants.BLOCK_STORAGE_POLICY_ID_UNSPECIFIED);
  }

  /**
   * 根据策略名称设置文件/目录的存储策略
   * @param fsd 文件目录管理器
   * @param pc 权限检查器
   * @param bm 块管理器
   * @param src 目标路径
   * @param policyName 存储策略名称
   * @return 操作后的文件状态信息，用于审计日志
   * @throws IOException 操作失败时抛出异常
   */
  static FileStatus setStoragePolicy(FSDirectory fsd, FSPermissionChecker pc,
      BlockManager bm, String src, final String policyName) throws IOException {
    // get the corresponding policy and make sure the policy name is valid
    BlockStoragePolicy policy = bm.getStoragePolicy(policyName);
    if (policy == null) {
      throw new HadoopIllegalArgumentException(
          "Cannot find a block policy with the name " + policyName);
    }
    return setStoragePolicy(fsd, pc, bm, src, policy.getId());
  }

  /**
   * 根据策略ID设置文件/目录的存储策略
   * @param fsd 文件目录管理器
   * @param pc 权限检查器
   * @param bm 块管理器
   * @param src 目标路径
   * @param policyId 存储策略ID
   * @return 操作后的文件状态信息，用于审计日志
   * @throws IOException 操作失败时抛出异常
   */
  static FileStatus setStoragePolicy(FSDirectory fsd, FSPermissionChecker pc,
      BlockManager bm, String src, final byte policyId)
      throws IOException {
    INodesInPath iip;
    fsd.writeLock();
    try {
      iip = fsd.resolvePath(pc, src, DirOp.WRITE);

      if (fsd.isPermissionEnabled()) {
        fsd.checkPathAccess(pc, iip, FsAction.WRITE);
      }

      unprotectedSetStoragePolicy(fsd, bm, iip, policyId);
      fsd.getEditLog().logSetStoragePolicy(iip.getPath(), policyId);
    } finally {
      fsd.writeUnlock();
    }
    return fsd.getAuditFileInfo(iip);
  }

  /**
   * 获取系统支持的所有块存储策略列表
   * @param bm 块管理器
   * @return 所有存储策略数组
   * @throws IOException 操作失败时抛出异常
   */
  static BlockStoragePolicy[] getStoragePolicies(BlockManager bm)
      throws IOException {
    return bm.getStoragePolicies();
  }

  /**
   * 获取指定路径当前生效的存储策略
   * @param fsd 文件目录管理器
   * @param pc 权限检查器
   * @param bm 块管理器
   * @param path 目标路径
   * @return 当前存储策略
   * @throws IOException 操作失败时抛出异常
   */
  static BlockStoragePolicy getStoragePolicy(FSDirectory fsd,
      FSPermissionChecker pc, BlockManager bm, String path) throws IOException {
    fsd.readLock();
    try {
      final INodesInPath iip = fsd.resolvePath(pc, path, DirOp.READ_LINK);
      if (fsd.isPermissionEnabled()) {
        fsd.checkPathAccess(pc, iip, FsAction.READ);
      }
      INode inode = iip.getLastINode();
      if (inode == null) {
        throw new FileNotFoundException("File/Directory does not exist: "
            + iip.getPath());
      }
      return bm.getStoragePolicy(inode.getStoragePolicyID());
    } finally {
      fsd.readUnlock();
    }
  }

  /**
   * 获取指定文件的首选块大小
   * @param fsd 文件目录管理器
   * @param pc 权限检查器
   * @param src 目标文件路径
   * @return 文件的首选块大小
   * @throws IOException 操作失败时抛出异常
   */
  static long getPreferredBlockSize(FSDirectory fsd, FSPermissionChecker pc,
      String src) throws IOException {
    fsd.readLock();
    try {
      final INodesInPath iip = fsd.resolvePath(pc, src, DirOp.READ_LINK);
      return INodeFile.valueOf(iip.getLastINode(), iip.getPath())
          .getPreferredBlockSize();
    } finally {
      fsd.readUnlock();
    }
  }

  /**
   * Set the namespace, storagespace and typespace quota for a directory.
   *
   * Note: This does not support ".inodes" relative path.
   * 设置目录配额，支持命名空间配额、存储空间配额和存储类型配额
   * @param fsd 文件目录管理器
   * @param pc 权限检查器
   * @param src 目标目录路径
   * @param nsQuota 命名空间配额（文件/目录数量限制）
   * @param ssQuota 存储空间配额/存储类型配额
   * @param type 存储类型，null表示设置全局配额，非null表示按存储类型设置配额
   * @param allowOwner 是否允许目录所有者设置配额
   * @throws IOException 操作失败时抛出异常
   */
  static void setQuota(FSDirectory fsd, FSPermissionChecker pc, String src,
      long nsQuota, long ssQuota, StorageType type, boolean allowOwner)
      throws IOException {

    fsd.writeLock();
    try {
      INodesInPath iip = fsd.resolvePath(pc, src, DirOp.WRITE);
      // Here, the assumption is that the caller of this method has
      // already checked for super user privilege
      if (fsd.isPermissionEnabled() && !pc.isSuperUser() && allowOwner) {
        try {
          fsd.checkOwner(pc, iip.getParentINodesInPath());
        } catch(AccessControlException ace) {
          throw new AccessControlException(
              "Access denied for user " + pc.getUser() +
              ". Superuser or owner of parent folder privilege is required");
        }
      }
      INodeDirectory changed =
          unprotectedSetQuota(fsd, iip, nsQuota, ssQuota, type);
      if (changed != null) {
        final QuotaCounts q = changed.getQuotaCounts();
        if (type == null) {
          fsd.getEditLog().logSetQuota(src, q.getNameSpace(), q.getStorageSpace());
        } else {
          fsd.getEditLog().logSetQuotaByStorageType(
              src, q.getTypeSpaces().get(type), type);
        }
      }
    } finally {
      fsd.writeUnlock();
    }
  }

  /**
   * 无锁保护版本的设置权限操作，调用方需已获取写锁
   * @param fsd 文件目录管理器
   * @param iip 路径节点链表
   * @param permissions 要设置的权限
   * @return 是否发生了修改
   */
  static boolean unprotectedSetPermission(
      FSDirectory fsd, INodesInPath iip, FsPermission permissions)
      throws FileNotFoundException, UnresolvedLinkException,
             QuotaExceededException, SnapshotAccessControlException {
    assert fsd.hasWriteLock();
    final INode inode = FSDirectory.resolveLastINode(iip);
    int snapshotId = iip.getLatestSnapshotId();
    long oldPerm = inode.getPermissionLong();
    inode.setPermission(permissions, snapshotId);
    return oldPerm != inode.getPermissionLong();
  }

  /**
   * 无锁保护版本的设置属主操作，调用方需已获取写锁
   * @param fsd 文件目录管理器
   * @param iip 路径节点链表
   * @param username 新用户名，null表示不修改
   * @param groupname 新组名，null表示不修改
   * @return 是否发生了修改
   */
  static boolean unprotectedSetOwner(
      FSDirectory fsd, INodesInPath iip, String username, String groupname)
      throws FileNotFoundException, UnresolvedLinkException,
      QuotaExceededException, SnapshotAccessControlException {
    assert fsd.hasWriteLock();
    final INode inode = FSDirectory.resolveLastINode(iip