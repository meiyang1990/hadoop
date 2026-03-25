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

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.InvalidPathException;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.QuotaExceededException;
import org.apache.hadoop.hdfs.server.namenode.FSDirectory.DirOp;

import java.io.IOException;

import static org.apache.hadoop.util.Time.now;

/**
 * HDFS NameNode符号链接操作工具类，提供创建符号链接的核心逻辑实现
 * 负责处理符号链接的路径解析、权限检查、配额校验和日志记录等操作
 */
class FSDirSymlinkOp {

  /**
   * 内部创建符号链接的入口方法，完成路径校验、权限检查和符号链接创建全流程
   * @param fsn NameNode文件系统管理器
   * @param target 符号链接指向的目标路径
   * @param linkArg 符号链接本身的路径
   * @param dirPerms 目录权限状态，用于创建父目录时使用
   * @param createParent 是否自动创建不存在的父目录
   * @param logRetryCache 是否记录到重试缓存，用于故障恢复
   * @return 创建完成的符号链接的FileStatus信息
   * @throws IOException 路径非法、创建失败、权限不足或配额超限等异常
   */
  static FileStatus createSymlinkInt(
      FSNamesystem fsn, String target, final String linkArg,
      PermissionStatus dirPerms, boolean createParent, boolean logRetryCache)
      throws IOException {
    FSDirectory fsd = fsn.getFSDirectory();
    String link = linkArg;
    // 校验符号链接名称合法性
    if (!DFSUtil.isValidName(link)) {
      throw new InvalidPathException("Invalid link name: " + link);
    }
    // 校验目标路径名称合法性
    if (FSDirectory.isReservedName(target) || target.isEmpty()
        || FSDirectory.isExactReservedName(target)) {
      throw new InvalidPathException("Invalid target name: " + target);
    }

    // 调试日志记录创建符号链接操作
    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("DIR* NameSystem.createSymlink: target="
          + target + " link=" + link);
    }

    // 获取权限检查器
    FSPermissionChecker pc = fsn.getPermissionChecker();
    INodesInPath iip;
    // 获取目录写锁，保证路径解析和创建操作原子性
    fsd.writeLock();
    try {
      // 解析符号链接路径，获取INodes路径信息
      iip = fsd.resolvePath(pc, link, DirOp.WRITE_LINK);
      // 获取解析后的规范化路径
      link = iip.getPath();
      // 如果不自动创建父目录，校验父目录是否存在
      if (!createParent) {
        fsd.verifyParentDir(iip);
      }
      // 校验路径是否可以创建：路径合法且目标位置不存在同名文件/目录
      if (!fsd.isValidToCreate(link, iip)) {
        throw new IOException(
            "failed to create link " + link +
                " either because the filename is invalid or the file exists");
      }
      // 权限开启时，校验祖先目录写权限
      if (fsd.isPermissionEnabled()) {
        fsd.checkAncestorAccess(pc, iip, FsAction.WRITE);
      }
      // 校验文件系统inode配额，确保有足够空间创建新符号链接
      fsn.checkFsObjectLimit();

      // 将符号链接添加到文件系统命名空间
      addSymlink(fsd, link, iip, target, dirPerms, createParent, logRetryCache);
    } finally {
      // 释放目录写锁
      fsd.writeUnlock();
    }
    // 增加创建符号链接操作指标计数
    NameNode.getNameNodeMetrics().incrCreateSymlinkOps();
    // 返回符号链接审计所需的文件信息
    return fsd.getAuditFileInfo(iip);
  }

  /**
   * 无保护添加符号链接，直接将符号linkinode添加到目录树，不做权限和校验检查
   * 调用方必须已经获取目录写锁，完成前置校验
   * @param fsd 文件目录管理器
   * @param iip 父目录的INodes路径信息
   * @param localName 符号链接的本地名称（父目录下的名称）
   * @param id 新符号链接的inode ID
   * @param target 符号链接指向的目标路径
   * @param mtime 修改时间
   * @param atime 访问时间
   * @param perm 权限状态
   * @return 创建成功返回符号linkinode对象，失败返回null
   * @throws UnresolvedLinkException 路径解析遇到未解析链接异常
   * @throws QuotaExceededException 目录配额超限异常
   */
  static INodeSymlink unprotectedAddSymlink(FSDirectory fsd, INodesInPath iip,
      byte[] localName, long id, String target, long mtime, long atime,
      PermissionStatus perm)
      throws UnresolvedLinkException, QuotaExceededException {
    assert fsd.hasWriteLock();
    // 创建新的符号linkinode对象
    final INodeSymlink symlink = new INodeSymlink(id, null, perm, mtime, atime,
        target);
    // 设置符号链接在父目录中的本地名称
    symlink.setLocalName(localName);
    // 将inode添加到目录树，返回添加结果
    return fsd.addINode(iip, symlink, perm.getPermission()) != null ?
        symlink : null;
  }

  /**
   * Add the given symbolic link to the fs. Record it in the edits log.
   * 将符号链接添加到文件系统，并记录到编辑日志，处理父目录创建逻辑
   * @param fsd 文件目录管理器
   * @param path 符号链接完整路径
   * @param iip 符号链接路径解析后的INodes信息
   * @param target 符号链接目标路径
   * @param dirPerms 目录权限状态，用于创建父目录
   * @param createParent 是否自动创建不存在的父目录
   * @param logRetryCache 是否记录到重试缓存
   * @return 创建成功返回符号linkinode，失败返回null
   * @throws IOException IO异常、配额异常等
   */
  private static INodeSymlink addSymlink(FSDirectory fsd, String path,
      INodesInPath iip, String target, PermissionStatus dirPerms,
      boolean createParent, boolean logRetryCache) throws IOException {
    // 获取当前时间作为创建、修改、访问时间
    final long mtime = now();
    final INodesInPath parent;
    // 如果需要自动创建父目录，递归创建所有不存在的祖先目录
    if (createParent) {
      parent = FSDirMkdirOp.createAncestorDirectories(fsd, iip, dirPerms);
      if (parent == null) {
        return null;
      }
    } else {
      // 获取已存在的父目录路径信息
      parent = iip.getParentINodesInPath();
    }
    // 从传入的权限状态获取用户名，符号链接所属用户继承当前创建用户
    final String userName = dirPerms.getUserName();
    // 分配新的inode ID
    long id = fsd.allocateNewInodeId();
    // 创建符号链接的默认权限状态，使用默认文件权限
    PermissionStatus perm = new PermissionStatus(
        userName, null, FsPermission.getDefault());
    // 调用无保护添加方法，将符号链接添加到目录树
    INodeSymlink newNode = unprotectedAddSymlink(fsd, parent,
        iip.getLastLocalName(), id, target, mtime, mtime, perm);
    // 添加失败直接返回null
    if (newNode == null) {
      NameNode.stateChangeLog.info("addSymlink: failed to add " + path);
      return null;
    }
    // 将创建符号链接操作记录到编辑日志，用于故障恢复和镜像构建
    fsd.getEditLog().logSymlink(path, target, mtime, mtime, newNode,
        logRetryCache);

    // 调试日志记录符号链接创建完成
    if(NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("addSymlink: " + path + " is added");
    }
    return newNode;
  }
}