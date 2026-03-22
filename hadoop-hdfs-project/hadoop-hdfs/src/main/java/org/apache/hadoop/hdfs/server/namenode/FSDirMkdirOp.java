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

import org.apache.hadoop.fs.permission.FsCreateModes;
import org.apache.hadoop.util.Preconditions;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.ParentNotDirectoryException;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.AclException;
import org.apache.hadoop.hdfs.protocol.QuotaExceededException;
import org.apache.hadoop.hdfs.server.namenode.FSDirectory.DirOp;
import org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot;
import org.apache.hadoop.security.AccessControlException;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import static org.apache.hadoop.util.Time.now;

/**
 * HDFS NameNode 创建目录操作工具类
 * 封装了创建目录相关的所有核心逻辑，包括逐级创建父目录、权限处理、日志记录等操作
 */
class FSDirMkdirOp {

  /**
   * 创建多级目录操作入口
   * @param fsn NameNode文件系统管理器
   * @param pc 权限检查器
   * @param src 待创建目录路径
   * @param permissions 目录权限状态
   * @param createParent 是否自动创建不存在的父目录
   * @return 新创建目录的FileStatus信息
   * @throws IOException 创建过程中可能抛出IO、权限、配额等异常
   */
  static FileStatus mkdirs(FSNamesystem fsn, FSPermissionChecker pc, String src,
      PermissionStatus permissions, boolean createParent) throws IOException {
    FSDirectory fsd = fsn.getFSDirectory();
    if(NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("DIR* NameSystem.mkdirs: " + src);
    }
    // 获取目录树写锁，保证创建操作的原子性
    fsd.writeLock();
    try {
      // 解析路径得到对应的INodes路径
      INodesInPath iip = fsd.resolvePath(pc, src, DirOp.CREATE);

      final INode lastINode = iip.getLastINode();
      if (lastINode != null && lastINode.isFile()) {
        throw new FileAlreadyExistsException("Path is not a directory: " + src);
      }

      if (lastINode == null) {
        if (fsd.isPermissionEnabled()) {
          // 检查所有祖先目录的写权限
          fsd.checkAncestorAccess(pc, iip, FsAction.WRITE);
        }

        if (!createParent) {
          // 不自动创建父目录时，验证父目录必须存在且为目录
          fsd.verifyParentDir(iip);
        }

        // 检查文件系统对象数量配额，确保有足够空间创建新目录
        fsn.checkFsObjectLimit();

        // 创建所有不存在的父目录，并给祖先目录添加隐式u+wx权限保证用户可遍历
        INodesInPath existing =
            createParentDirectories(fsd, iip, permissions, false);
        if (existing != null) {
          existing = createSingleDirectory(
              fsd, existing, iip.getLastLocalName(), permissions);
        }
        if (existing == null) {
          throw new IOException("Failed to create directory: " + src);
        }
        iip = existing;
      }
      // 返回新创建目录的审计信息
      return fsd.getAuditFileInfo(iip);
    } finally {
      // 释放写锁
      fsd.writeUnlock();
    }
  }

  /**
   * 为给定路径创建所有不存在的祖先目录
   * 所有祖先目录继承父权限并添加隐式u+wx权限，供创建文件和符号链接时隐式创建父目录使用
   *
   * @param fsd FSDirectory目录管理器
   * @param iip 目标路径的INodes路径
   * @param permission 新创建目录的基础权限
   * @return 包含所有已存在和新创建祖先目录的INodesInPath，出错返回null
   * @throws IOException 创建过程中的IO异常
   */
  static INodesInPath createAncestorDirectories(
      FSDirectory fsd, INodesInPath iip, PermissionStatus permission)
      throws IOException {
    return createParentDirectories(fsd, iip, permission, true);
  }

  /**
   * 创建路径中所有不存在的父目录，支持权限继承模式
   * @param fsd FSDirectory目录管理器
   * @param iip 目标路径的INodes路径
   * @param perm 新目录权限
   * @param inheritPerms 是否继承父目录权限，true则继承，false使用传入权限
   * @return 所有父目录创建完成后的INodesInPath，出错返回null
   * @throws IOException 创建过程中的IO异常
   */
  private static INodesInPath createParentDirectories(FSDirectory fsd,
      INodesInPath iip, PermissionStatus perm, boolean inheritPerms)
      throws IOException {
    assert fsd.hasWriteLock();
    // 获取已存在的最长前缀目录路径
    INodesInPath existing = iip.getExistingINodes();
    int missing = iip.length() - existing.length();
    if (missing == 0) {  // 路径已全部存在，返回目标的父目录路径
      existing = iip.getParentINodesInPath();
    } else if (missing > 1) { // 需要创建至少一个祖先目录
      // 如果需要继承权限则使用父目录权限，否则使用传入权限
      PermissionStatus basePerm = inheritPerms
          ? existing.getLastINode().getPermissionStatus()
          : perm;
      // 给权限添加隐式u+wx，保证用户可遍历创建的祖先目录
      perm = addImplicitUwx(basePerm, perm);
      // 逐级创建所有缺失的目录
      final int last = iip.length() - 2;
      for (int i = existing.length(); existing != null && i <= last; i++) {
        byte[] component = iip.getPathComponent(i);
        existing = createSingleDirectory(fsd, existing, component, perm);
      }
    }
    return existing;
  }

  /**
   * 从编辑日志重放创建目录操作，用于NameNode启动时加载元数据
   * @param fsd FSDirectory目录管理器
   * @param inodeId 新目录的inode ID
   * @param src 目录路径
   * @param permissions 目录权限状态
   * @param aclEntries ACL权限列表
   * @param timestamp 创建时间戳
   * @throws QuotaExceededException 配额超限异常
   * @throws UnresolvedLinkException 链接未解析异常
   * @throws AclException ACL配置异常
   * @throws FileAlreadyExistsException 文件已存在异常
   * @throws ParentNotDirectoryException 父路径不是目录异常
   * @throws AccessControlException 权限访问异常
   */
  static void mkdirForEditLog(FSDirectory fsd, long inodeId, String src,
      PermissionStatus permissions, List<AclEntry> aclEntries, long timestamp)
      throws QuotaExceededException, UnresolvedLinkException, AclException,
      FileAlreadyExistsException, ParentNotDirectoryException,
      AccessControlException {
    assert fsd.hasWriteLock();
    INodesInPath iip = fsd.getINodesInPath(src, DirOp.WRITE_LINK);
    final byte[] localName = iip.getLastLocalName();
    final INodesInPath existing = iip.getParentINodesInPath();
    Preconditions.checkState(existing.getLastINode() != null);
    unprotectedMkdir(fsd, inodeId, existing, localName, permissions, aclEntries,
        timestamp);
  }

  /**
   * 创建单个目录，包含 metrics 统计和编辑日志记录
   * @param fsd FSDirectory目录管理器
   * @param existing 父目录的INodes路径
   * @param localName 新目录名称字节数组
   * @param perm 新目录权限状态
   * @return 新目录所在的INodesInPath，创建失败返回null
   * @throws IOException 创建过程中的各类异常
   */
  private static INodesInPath createSingleDirectory(FSDirectory fsd,
      INodesInPath existing, byte[] localName, PermissionStatus perm)
      throws IOException {
    assert fsd.hasWriteLock();
    existing = unprotectedMkdir(fsd, fsd.allocateNewInodeId(), existing,
        localName, perm, null, now());
    if (existing == null) {
      return null;
    }

    final INode newNode = existing.getLastINode();
    // 目录创建也计入FilesCreated指标，和FilesDeleted对应
    NameNode.getNameNodeMetrics().incrFilesCreated();

    String cur = existing.getPath();
    // 记录创建目录操作到编辑日志，保证故障恢复
    fsd.getEditLog().logMkDir(cur, newNode);
    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("mkdirs: created directory " + cur);
    }
    return existing;
  }

  /**
   * 给祖先目录添加隐式u+wx权限，确保用户可以遍历访问后续路径
   * @param parentPerm 父目录权限状态
   * @param perm 原权限状态
   * @return 添加了u+wx权限后的新权限状态
   */
  private static PermissionStatus addImplicitUwx(PermissionStatus parentPerm,
      PermissionStatus perm) {
    FsPermission p = parentPerm.getPermission();
    FsPermission ancestorPerm;
    if (p.getUnmasked() == null) {
      ancestorPerm = new FsPermission(
          p.getUserAction().or(FsAction.WRITE_EXECUTE),
          p.getGroupAction(),
          p.getOtherAction());
    } else {
      ancestorPerm = FsCreateModes.create(
          new FsPermission(
            p.getUserAction().or(FsAction.WRITE_EXECUTE),
            p.getGroupAction(),
            p.getOtherAction()), p.getUnmasked());
    }
    return new PermissionStatus(perm.getUserName(), perm.getGroupName(),
        ancestorPerm);
  }

  /**
   * 无保护创建单个目录的底层实现，不做权限检查和日志记录，直接操作目录树
   * @param fsd FSDirectory目录管理器
   * @param inodeId 新目录的inode ID
   * @param parent 父目录的INodes路径
   * @param name 新目录名称字节数组
   * @param permission 新目录权限状态
   * @param aclEntries ACL权限列表
   * @param timestamp 创建时间戳
   * @return 新创建目录所在的INodesInPath
   * @throws QuotaExceededException 配额超限异常
   * @throws AclException ACL配置异常
   * @throws FileAlreadyExistsException 文件已存在异常
   */
  private static INodesInPath unprotectedMkdir(FSDirectory fsd, long inodeId,
      INodesInPath parent, byte[] name, PermissionStatus permission,
      List<AclEntry> aclEntries, long timestamp)
      throws QuotaExceededException, AclException, FileAlreadyExistsException {
    assert fsd.hasWriteLock();
    assert parent.getLastINode() != null;
    if (!parent.getLastINode().isDirectory()) {
      throw new FileAlreadyExistsException("Parent path is not a directory: " +
          parent.getPath() + " " + DFSUtil.bytes2String(name));
    }
    // 创建新的目录INode对象
    final INodeDirectory dir = new INodeDirectory(inodeId, name, permission,
        timestamp);

    // 将新目录添加到父目录的inode列表中
    INodesInPath iip = fsd.addLastINode(parent, dir, permission.getPermission(),
        true, Optional.empty());
    // 如果存在ACL配置，更新inode的ACL信息
    if (iip != null && aclEntries != null) {
      AclStorage.updateINodeAcl(dir, aclEntries, Snapshot.CURRENT_STATE_ID);
    }
    return iip;
  }
}