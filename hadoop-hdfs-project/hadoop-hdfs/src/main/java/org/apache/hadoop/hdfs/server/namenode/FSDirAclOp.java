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
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclEntryScope;
import org.apache.hadoop.fs.permission.AclEntryType;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.AclException;
import org.apache.hadoop.hdfs.server.namenode.FSDirectory.DirOp;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * HDFS NameNode 访问控制列表(ACL)操作工具类，提供所有ACL相关目录操作的静态实现
 * 封装了ACL的增删改查核心逻辑，被NameNode文件目录操作路由调用，负责维护文件系统的ACL权限信息
 */
class FSDirAclOp {

  /**
   * 向指定路径添加/修改ACL条目，保留已有其他ACL条目不变
   * @param fsd FSDirectory 目录管理器
   * @param pc 权限检查器
   * @param srcArg 目标路径
   * @param aclSpec 待添加/修改的ACL条目列表
   * @return 操作完成后返回目标路径的FileStatus用于审计日志
   * @throws IOException 处理过程中如果发生错误抛出IO异常或ACL异常
   */
  static FileStatus modifyAclEntries(
      FSDirectory fsd, FSPermissionChecker pc, final String srcArg,
      List<AclEntry> aclSpec) throws IOException {
    String src = srcArg;
    checkAclsConfigFlag(fsd);
    INodesInPath iip;
    fsd.writeLock();
    try {
      iip = fsd.resolvePath(pc, src, DirOp.WRITE);
      src = iip.getPath();
      fsd.checkOwner(pc, iip);
      INode inode = FSDirectory.resolveLastINode(iip);
      int snapshotId = iip.getLatestSnapshotId();
      List<AclEntry> existingAcl = AclStorage.readINodeLogicalAcl(inode);
      List<AclEntry> newAcl = AclTransformation.mergeAclEntries(
          existingAcl, aclSpec);
      AclStorage.updateINodeAcl(inode, newAcl, snapshotId);
      fsd.getEditLog().logSetAcl(src, newAcl);
    } catch (AclException e){
      throw new AclException(e.getMessage() + " Path: " + src, e);
    } finally {
      fsd.writeUnlock();
    }
    return fsd.getAuditFileInfo(iip);
  }

  /**
   * 从指定路径删除指定的ACL条目，保留其他已有ACL条目不变
   * @param fsd FSDirectory 目录管理器
   * @param pc 权限检查器
   * @param srcArg 目标路径
   * @param aclSpec 待删除的ACL条目列表
   * @return 操作完成后返回目标路径的FileStatus用于审计日志
   * @throws IOException 处理过程中如果发生错误抛出IO异常或ACL异常
   */
  static FileStatus removeAclEntries(
      FSDirectory fsd, FSPermissionChecker pc, final String srcArg,
      List<AclEntry> aclSpec) throws IOException {
    String src = srcArg;
    checkAclsConfigFlag(fsd);
    INodesInPath iip;
    fsd.writeLock();
    try {
      iip = fsd.resolvePath(pc, src, DirOp.WRITE);
      src = iip.getPath();
      fsd.checkOwner(pc, iip);
      INode inode = FSDirectory.resolveLastINode(iip);
      int snapshotId = iip.getLatestSnapshotId();
      List<AclEntry> existingAcl = AclStorage.readINodeLogicalAcl(inode);
      List<AclEntry> newAcl = AclTransformation.filterAclEntriesByAclSpec(
        existingAcl, aclSpec);
      AclStorage.updateINodeAcl(inode, newAcl, snapshotId);
      fsd.getEditLog().logSetAcl(src, newAcl);
    } catch (AclException e){
      throw new AclException(e.getMessage() + " Path: " + src, e);
    } finally {
      fsd.writeUnlock();
    }
    return fsd.getAuditFileInfo(iip);
  }

  /**
   * 删除指定目录的默认ACL条目，保留访问ACL条目不变
   * @param fsd FSDirectory 目录管理器
   * @param pc 权限检查器
   * @param srcArg 目标目录路径
   * @return 操作完成后返回目标路径的FileStatus用于审计日志
   * @throws IOException 处理过程中如果发生错误抛出IO异常或ACL异常
   */
  static FileStatus removeDefaultAcl(FSDirectory fsd, FSPermissionChecker pc,
      final String srcArg) throws IOException {
    String src = srcArg;
    checkAclsConfigFlag(fsd);
    INodesInPath iip;
    fsd.writeLock();
    try {
      iip = fsd.resolvePath(pc, src, DirOp.WRITE);
      src = iip.getPath();
      fsd.checkOwner(pc, iip);
      INode inode = FSDirectory.resolveLastINode(iip);
      int snapshotId = iip.getLatestSnapshotId();
      List<AclEntry> existingAcl = AclStorage.readINodeLogicalAcl(inode);
      List<AclEntry> newAcl = AclTransformation.filterDefaultAclEntries(
        existingAcl);
      AclStorage.updateINodeAcl(inode, newAcl, snapshotId);
      fsd.getEditLog().logSetAcl(src, newAcl);
    } catch (AclException e){
      throw new AclException(e.getMessage() + " Path: " + src, e);
    } finally {
      fsd.writeUnlock();
    }
    return fsd.getAuditFileInfo(iip);
  }

  /**
   * 删除指定路径的所有ACL条目，恢复为基础权限模型
   * @param fsd FSDirectory 目录管理器
   * @param pc 权限检查器
   * @param srcArg 目标路径
   * @return 操作完成后返回目标路径的FileStatus用于审计日志
   * @throws IOException 处理过程中如果发生错误抛出IO异常或ACL异常
   */
  static FileStatus removeAcl(FSDirectory fsd, FSPermissionChecker pc,
      final String srcArg) throws IOException {
    String src = srcArg;
    checkAclsConfigFlag(fsd);
    INodesInPath iip;
    fsd.writeLock();
    try {
      iip = fsd.resolvePath(pc, src, DirOp.WRITE);
      src = iip.getPath();
      fsd.checkOwner(pc, iip);
      unprotectedRemoveAcl(fsd, iip);
    } catch (AclException e){
      throw new AclException(e.getMessage() + " Path: " + src, e);
    } finally {
      fsd.writeUnlock();
    }
    fsd.getEditLog().logSetAcl(src, AclFeature.EMPTY_ENTRY_LIST);
    return fsd.getAuditFileInfo(iip);
  }

  /**
   * 覆盖设置指定路径的完整ACL列表，替换所有已有ACL条目
   * @param fsd FSDirectory 目录管理器
   * @param pc 权限检查器
   * @param srcArg 目标路径
   * @param aclSpec 完整ACL条目列表，将完全替换原有ACL
   * @return 操作完成后返回目标路径的FileStatus用于审计日志
   * @throws IOException 处理过程中如果发生错误抛出IO异常或ACL异常
   */
  static FileStatus setAcl(
      FSDirectory fsd, FSPermissionChecker pc, final String srcArg,
      List<AclEntry> aclSpec) throws IOException {
    String src = srcArg;
    checkAclsConfigFlag(fsd);
    INodesInPath iip;
    fsd.writeLock();
    try {
      iip = fsd.resolvePath(pc, src, DirOp.WRITE);
      fsd.checkOwner(pc, iip);
      List<AclEntry> newAcl = unprotectedSetAcl(fsd, iip, aclSpec, false);
      fsd.getEditLog().logSetAcl(iip.getPath(), newAcl);
    } catch (AclException e){
      throw new AclException(e.getMessage() + " Path: " + src, e);
    } finally {
      fsd.writeUnlock();
    }
    return fsd.getAuditFileInfo(iip);
  }

  /**
   * 获取指定路径的ACL状态信息，包含所有者、组、权限和所有ACL条目
   * @param fsd FSDirectory 目录管理器
   * @param pc 权限检查器
   * @param src 目标路径
   * @return 目标路径的完整AclStatus信息
   * @throws IOException 处理过程中如果发生错误抛出IO异常或ACL异常
   */
  static AclStatus getAclStatus(
      FSDirectory fsd, FSPermissionChecker pc, String src) throws IOException {
    checkAclsConfigFlag(fsd);
    fsd.readLock();
    try {
      INodesInPath iip = fsd.resolvePath(pc, src, DirOp.READ);
      // 对于.snapshot结尾的路径没有实际inode，返回空AclStatus，与getFileInfo行为一致
      if (iip.isDotSnapshotDir() && fsd.getINode4DotSnapshot(iip) != null) {
        return new AclStatus.Builder().owner("").group("").build();
      }
      INodeAttributes inodeAttrs = fsd.getAttributes(iip);
      List<AclEntry> acl = AclStorage.readINodeAcl(inodeAttrs);
      FsPermission fsPermission = inodeAttrs.getFsPermission();
      return new AclStatus.Builder()
          .owner(inodeAttrs.getUserName()).group(inodeAttrs.getGroupName())
          .stickyBit(fsPermission.getStickyBit())
          .setPermission(fsPermission)
          .addEntries(acl).build();
    } catch (AclException e){
      throw new AclException(e.getMessage() + " Path: " + src, e);
    } finally {
      fsd.readUnlock();
    }
  }

  /**
   * 未带权限检查的ACL设置内部方法，供编辑日志回放等内部场景调用，调用方必须已经持有写锁并完成权限检查
   * @param fsd FSDirectory 目录管理器
   * @param iip 目标路径的INodesInPath解析结果
   * @param aclSpec 待设置的ACL条目列表
   * @param fromEdits 是否来自编辑日志回放，编辑日志回放时直接使用日志中已处理好的ACL，无需再次转换
   * @return 最终设置到inode的ACL条目列表
   * @throws IOException 处理过程中如果发生错误抛出IO异常或ACL异常
   */
  static List<AclEntry> unprotectedSetAcl(FSDirectory fsd, INodesInPath iip,
      List<AclEntry> aclSpec, boolean fromEdits) throws IOException {
    assert fsd.hasWriteLock();

    // ACL删除通过OP_SET_ACL携带空列表记录到编辑日志
    if (aclSpec.isEmpty()) {
      unprotectedRemoveAcl(fsd, iip);
      return AclFeature.EMPTY_ENTRY_LIST;
    }

    INode inode = FSDirectory.resolveLastINode(iip);
    int snapshotId = iip.getLatestSnapshotId();
    List<AclEntry> newAcl = aclSpec;
    if (!fromEdits) {
      List<AclEntry> existingAcl = AclStorage.readINodeLogicalAcl(inode);
      newAcl = AclTransformation.replaceAclEntries(existingAcl, aclSpec);
    }
    AclStorage.updateINodeAcl(inode, newAcl, snapshotId);
    return newAcl;
  }

  /**
   * 检查ACL功能是否在NameNode配置中启用，未启用则抛出异常拒绝操作
   * @param fsd FSDirectory 目录管理器，用于获取配置状态
   * @throws AclException 如果ACL功能未启用则抛出异常
   */
  private static void checkAclsConfigFlag(FSDirectory fsd) throws AclException {
    if (!fsd.isAclsEnabled()) {
      throw new AclException(String.format(
          "The ACL operation has been rejected.  "
              + "Support for ACLs has been disabled by setting %s to false.",
          DFSConfigKeys.DFS_NAMENODE_ACLS_ENABLED_KEY));
    }
  }

  /**
   * 未带权限检查的删除所有ACL内部方法，调用方必须已经持有写锁并完成权限检查
   * 会还原权限位，移除ACL特性
   * @param fsd FSDirectory 目录管理器
   * @param iip 目标路径的INodesInPath解析结果
   * @throws IOException 处理过程中如果发生错误抛出IO异常
   */
  private static void unprotectedRemoveAcl(FSDirectory fsd, INodesInPath iip)
      throws IOException {
    assert fsd.hasWriteLock();
    INode inode = FSDirectory.resolveLastINode(iip);
    int snapshotId = iip.getLatestSnapshotId();
    AclFeature f = inode.getAclFeature();
    if (f == null) {
      return;
    }

    FsPermission perm = inode.getFsPermission();
    List<AclEntry> featureEntries = AclStorage.getEntriesFromAclFeature(f);
    if (featureEntries.get(0).getScope() == AclEntryScope.ACCESS) {
      // 从ACL条目中恢复组权限到权限位，覆盖掩码，最小ACL不包含掩码
      AclEntry groupEntryKey = new AclEntry.Builder()
          .setScope(AclEntryScope.ACCESS).setType(AclEntryType.GROUP).build();
      int groupEntryIndex = Collections.binarySearch(
          featureEntries, groupEntryKey,
          AclTransformation.ACL_ENTRY_COMPARATOR);
      if (groupEntryIndex < 0 || groupEntryIndex > featureEntries.size()) {
        throw new IndexOutOfBoundsException(
            "Invalid group entry index after binary-searching inode: "
                + inode.getFullPathName() + "(" + inode.getId() + ") "
                + "with featureEntries:" + featureEntries);
      }
      FsAction groupPerm = featureEntries.get(groupEntryIndex).getPermission();
      FsPermission newPerm = new FsPermission(perm.getUserAction(), groupPerm,
          perm.getOtherAction(), perm.getStickyBit());
      inode.setPermission(newPerm, snapshotId);
    }

    inode.removeAclFeature(snapshotId);
  }
}