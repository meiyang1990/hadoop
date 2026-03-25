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
import org.apache.hadoop.fs.XAttr;
import org.apache.hadoop.fs.XAttrSetFlag;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.XAttrHelper;
import org.apache.hadoop.hdfs.protocol.XAttrNotFoundException;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.ReencryptionInfoProto;
import org.apache.hadoop.hdfs.protocolPB.PBHelperClient;
import org.apache.hadoop.hdfs.server.namenode.FSDirectory.DirOp;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.util.Lists;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.util.Preconditions;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.EnumSet;
import java.util.List;
import java.util.ListIterator;

import static org.apache.hadoop.hdfs.server.common.HdfsServerConstants.CRYPTO_XATTR_ENCRYPTION_ZONE;
import static org.apache.hadoop.hdfs.server.common.HdfsServerConstants.SECURITY_XATTR_UNREADABLE_BY_SUPERUSER;
import static org.apache.hadoop.hdfs.server.common.HdfsServerConstants.XATTR_SATISFY_STORAGE_POLICY;
import static org.apache.hadoop.hdfs.server.common.HdfsServerConstants.XATTR_SNAPSHOT_DELETED;

/**
 * HDFS NameNode 扩展属性(XAttr)操作工具类，提供对文件/目录扩展属性的增删查改核心逻辑
 * 所有操作都围绕INode节点的扩展属性进行管理，整合了权限检查、容量限制和特殊XAttr处理逻辑
 */
public class FSDirXAttrOp {
  // 加密区扩展属性静态定义
  private static final XAttr KEYID_XATTR =
      XAttrHelper.buildXAttr(CRYPTO_XATTR_ENCRYPTION_ZONE, null);
  // 超级用户不可读扩展属性静态定义
  private static final XAttr UNREADABLE_BY_SUPERUSER_XATTR =
      XAttrHelper.buildXAttr(SECURITY_XATTR_UNREADABLE_BY_SUPERUSER, null);

  /**
   * 为指定路径的文件/目录设置扩展属性
   * @param fsd 文件目录对象，管理NameNode目录树
   * @param pc 权限检查器
   * @param src 目标路径
   * @param xAttr 需要设置的扩展属性
   * @param flag 设置标志(创建/替换等)
   * @param logRetryCache 是否在编辑日志中记录RPC ID用于重试缓存重建
   * @return 目标文件/目录的FileStatus信息
   * @throws IOException 操作失败时抛出异常
   */
  static FileStatus setXAttr(
      FSDirectory fsd, FSPermissionChecker pc, String src, XAttr xAttr,
      EnumSet<XAttrSetFlag> flag, boolean logRetryCache)
      throws IOException {
    // 检查XAttr功能是否开启
    checkXAttrsConfigFlag(fsd);
    // 检查XAttr大小是否超过配置限制
    checkXAttrSize(fsd, xAttr);
    // 检查当前用户对该XAttr的操作权限
    XAttrPermissionFilter.checkPermissionForApi(
        pc, xAttr, FSDirectory.isReservedRawName(src));
    List<XAttr> xAttrs = Lists.newArrayListWithCapacity(1);
    xAttrs.add(xAttr);
    INodesInPath iip;
    // 获取目录写锁
    fsd.writeLock();
    try {
      // 解析路径获取INodesInPath
      iip = fsd.resolvePath(pc, src, DirOp.WRITE);
      src = iip.getPath();
      // 检查修改XAttr所需的访问权限
      checkXAttrChangeAccess(fsd, iip, xAttr, pc);
      // 执行无保护的XAttr设置操作
      unprotectedSetXAttrs(fsd, iip, xAttrs, flag);
    } finally {
      // 释放目录写锁
      fsd.writeUnlock();
    }
    // 记录设置XAttr操作到编辑日志
    fsd.getEditLog().logSetXAttrs(src, xAttrs, logRetryCache);
    // 返回审计用的文件信息
    return fsd.getAuditFileInfo(iip);
  }

  /**
   * 获取指定路径下指定的扩展属性列表
   * @param fsd 文件目录对象
   * @param pc 权限检查器
   * @param srcArg 目标路径
   * @param xAttrs 需要获取的扩展属性列表，为空则获取全部
   * @return 符合条件的扩展属性列表
   * @throws IOException 操作失败时抛出异常
   */
  static List<XAttr> getXAttrs(FSDirectory fsd, FSPermissionChecker pc,
      final String srcArg, List<XAttr> xAttrs) throws IOException {
    String src = srcArg;
    checkXAttrsConfigFlag(fsd);
    final boolean isRawPath = FSDirectory.isReservedRawName(src);
    boolean getAll = xAttrs == null || xAttrs.isEmpty();
    if (!getAll) {
      XAttrPermissionFilter.checkPermissionForApi(pc, xAttrs, isRawPath);
    }
    // 解析路径获取INodesInPath
    final INodesInPath iip = fsd.resolvePath(pc, src, DirOp.READ);
    if (fsd.isPermissionEnabled()) {
      // 检查路径读权限
      fsd.checkPathAccess(pc, iip, FsAction.READ);
    }
    // 获取该inode所有XAttr
    List<XAttr> all = FSDirXAttrOp.getXAttrs(fsd, iip);
    // 根据权限过滤XAttr
    List<XAttr> filteredAll = XAttrPermissionFilter.
        filterXAttrsForApi(pc, all, isRawPath);

    if (getAll) {
      return filteredAll;
    }
    if (filteredAll == null || filteredAll.isEmpty()) {
      throw new XAttrNotFoundException();
    }
    List<XAttr> toGet = Lists.newArrayListWithCapacity(xAttrs.size());
    // 按请求列表匹配XAttr
    for (XAttr xAttr : xAttrs) {
      boolean foundIt = false;
      for (XAttr a : filteredAll) {
        if (xAttr.getNameSpace() == a.getNameSpace() && xAttr.getName().equals(
            a.getName())) {
          toGet.add(a);
          foundIt = true;
          break;
        }
      }
      if (!foundIt) {
        throw new XAttrNotFoundException();
      }
    }
    return toGet;
  }

  /**
   * 列出指定路径下所有有权限访问的扩展属性
   * @param fsd 文件目录对象
   * @param pc 权限检查器
   * @param src 目标路径
   * @return 所有可访问扩展属性列表
   * @throws IOException 操作失败时抛出异常
   */
  static List<XAttr> listXAttrs(
      FSDirectory fsd, FSPermissionChecker pc, String src) throws IOException {
    FSDirXAttrOp.checkXAttrsConfigFlag(fsd);
    final boolean isRawPath = FSDirectory.isReservedRawName(src);
    final INodesInPath iip = fsd.resolvePath(pc, src, DirOp.READ);
    if (fsd.isPermissionEnabled()) {
      fsd.checkPathAccess(pc, iip, FsAction.READ);
    }
    final List<XAttr> all = FSDirXAttrOp.getXAttrs(fsd, iip);
    // 过滤当前用户可访问的XAttr返回
    return XAttrPermissionFilter.
        filterXAttrsForApi(pc, all, isRawPath);
  }

  /**
   * 从指定路径删除指定扩展属性
   * @param fsd 文件目录对象
   * @param pc 权限检查器
   * @param src 目标路径
   * @param xAttr 需要删除的扩展属性
   * @param logRetryCache 是否在编辑日志中记录RPC ID用于重试缓存重建
   * @return 目标文件/目录的FileStatus信息
   * @throws IOException 操作失败时抛出异常
   */
  static FileStatus removeXAttr(
      FSDirectory fsd, FSPermissionChecker pc, String src, XAttr xAttr,
      boolean logRetryCache) throws IOException {
    FSDirXAttrOp.checkXAttrsConfigFlag(fsd);
    XAttrPermissionFilter.checkPermissionForApi(
        pc, xAttr, FSDirectory.isReservedRawName(src));

    List<XAttr> xAttrs = Lists.newArrayListWithCapacity(1);
    xAttrs.add(xAttr);
    INodesInPath iip;
    fsd.writeLock();
    try {
      iip = fsd.resolvePath(pc, src, DirOp.WRITE);
      src = iip.getPath();
      checkXAttrChangeAccess(fsd, iip, xAttr, pc);

      // 执行无保护删除操作
      List<XAttr> removedXAttrs = unprotectedRemoveXAttrs(fsd, iip, xAttrs);
      if (removedXAttrs != null && !removedXAttrs.isEmpty()) {
        // 记录删除操作到编辑日志
        fsd.getEditLog().logRemoveXAttrs(src, removedXAttrs, logRetryCache);
      } else {
        throw new IOException(
            "No matching attributes found for remove operation");
      }
    } finally {
      fsd.writeUnlock();
    }
    return fsd.getAuditFileInfo(iip);
  }

  /**
   * 无锁保护删除指定扩展属性，直接修改INode存储
   * @param fsd 文件目录对象
   * @param iip 路径对应INodes
   * @param toRemove 需要删除的扩展属性列表
   * @return 成功删除的扩展属性列表，无删除则返回null
   * @throws IOException 操作失败时抛出异常
   */
  static List<XAttr> unprotectedRemoveXAttrs(
      FSDirectory fsd, final INodesInPath iip, final List<XAttr> toRemove)
      throws IOException {
    assert fsd.hasWriteLock();
    INode inode = FSDirectory.resolveLastINode(iip);
    int snapshotId = iip.getLatestSnapshotId();
    List<XAttr> existingXAttrs = XAttrStorage.readINodeXAttrs(inode);
    List<XAttr> removedXAttrs = Lists.newArrayListWithCapacity(toRemove.size());
    List<XAttr> newXAttrs = filterINodeXAttrs(existingXAttrs, toRemove,
                                              removedXAttrs);
    if (existingXAttrs.size() != newXAttrs.size()) {
      // 更新INode中的XAttr存储
      XAttrStorage.updateINodeXAttrs(inode, newXAttrs, snapshotId);
      return removedXAttrs;
    }
    return null;
  }

  /**
   * 从现有扩展属性列表中过滤掉待删除的属性
   * @param existingXAttrs 现有扩展属性列表
   * @param toFilter 待过滤删除的扩展属性列表
   * @param filtered 输出参数，保存成功匹配删除的扩展属性
   * @return 过滤后剩余的扩展属性列表
   * @throws AccessControlException 尝试删除禁止删除的属性时抛出异常
   */
  @VisibleForTesting
  static List<XAttr> filterINodeXAttrs(
      final List<XAttr> existingXAttrs, final List<XAttr> toFilter,
      final List<XAttr> filtered)
    throws AccessControlException {
    if (existingXAttrs == null || existingXAttrs.isEmpty() ||
        toFilter == null || toFilter.isEmpty()) {
      return existingXAttrs;
    }

    List<XAttr> newXAttrs =
        Lists.newArrayListWithCapacity(existingXAttrs.size());
    for (XAttr a : existingXAttrs) {
      boolean add = true;
      for (ListIterator<XAttr> it = toFilter.listIterator(); it.hasNext()
          ;) {
        XAttr filter = it.next();
        // 禁止删除加密区扩展属性
        Preconditions.checkArgument(
            !KEYID_XATTR.equalsIgnoreValue(filter),
            "The encryption zone xattr should never be deleted.");
        // 禁止删除超级用户不可读扩展属性
        if (UNREADABLE_BY_SUPERUSER_XATTR.equalsIgnoreValue(filter)) {
          throw new AccessControlException("The xattr '" +
              SECURITY_XATTR_UNREADABLE_BY_SUPERUSER + "' can not be deleted.");
        }
        if (a.equalsIgnoreValue(filter)) {
          add = false;
          it.remove();
          filtered.add(filter);
          break;
        }
      }
      if (add) {
        newXAttrs.add(a);
      }
    }

    return newXAttrs;
  }

  /**
   * 无锁保护设置扩展属性，处理特殊XAttr的业务逻辑并更新INode存储
   * @param fsd 文件目录对象
   * @param iip 路径对应INodes
   * @param xAttrs 需要设置的扩展属性列表
   * @param flag 设置标志
   * @return 修改后的INode
   * @throws IOException 操作失败时抛出异常
   */
  public static INode unprotectedSetXAttrs(
      FSDirectory fsd, final INodesInPath iip, final List<XAttr> xAttrs,
      final EnumSet<XAttrSetFlag> flag)
      throws IOException {
    assert fsd.hasWriteLock();
    INode inode = FSDirectory.resolveLastINode(iip);
    List<XAttr> existingXAttrs = XAttrStorage.readINodeXAttrs(inode);
    // 执行设置XAttr合并生成新列表
    List<XAttr> newXAttrs = setINodeXAttrs(fsd, existingXAttrs, xAttrs, flag);
    final boolean isFile = inode.isFile();

    // 遍历处理每个XAttr的特殊业务逻辑
    for (XAttr xattr : newXAttrs) {
      final String xaName = XAttrHelper.getPrefixedName(xattr);

      // 处理加密区XAttr：添加新加密区到加密区管理器
      if (CRYPTO_XATTR_ENCRYPTION_ZONE.equals(xaName)) {
        final HdfsProtos.ZoneEncryptionInfoProto ezProto =
            HdfsProtos.ZoneEncryptionInfoProto.parseFrom(xattr.getValue());
        fsd.ezManager.addEncryptionZone(inode.getId(),
            PBHelperClient.convert(ezProto.getSuite()),
            PBHelperClient.convert(ezProto.getCryptoProtocolVersion()),
            ezProto.getKeyName());

        if (ezProto.hasReencryptionProto()) {
          ReencryptionInfoProto reProto = ezProto.getReencryptionProto();
          fsd.ezManager.getReencryptionStatus()
              .updateZoneStatus(inode.getId(), iip.getPath(), reProto);
        }
      }

      // 处理存储策略满足XAttr：将inode加入存储策略移动队列
      if (XATTR_SATISFY_STORAGE_POLICY.equals(xaName)) {
        FSDirSatisfyStoragePolicyOp.unprotectedSatisfyStoragePolicy(inode, fsd);
        continue;
      }

      // 检查安全XAttr只能设置在文件上
      if (!isFile && SECURITY_XATTR_UNREADABLE_BY_SUPERUSER.equals(xaName)) {
        throw new IOException("Can only set '" +
            SECURITY_XATTR_UNREADABLE_BY_SUPERUSER + "' on a file.");
      }

      // 检查快照删除XAttr只能设置在快照根目录
      if (xaName.equals(XATTR_SNAPSHOT_DELETED) && !(inode.isDirectory() &&
          inode.getParent().isSnapshottable())) {
        throw new IOException("Can only set '" +
            XATTR_SNAPSHOT_DELETED + "' on a snapshot root.");
      }
    }

    // 更新INode的XAttr存储
    XAttrStorage.updateINodeXAttrs(inode, newXAttrs, iip.getLatestSnapshotId());
    return inode;
  }

  /**
   * 根据现有XAttr和待设置XAttr合并生成新的XAttr列表，验证标志和数量限制
   * @param fsd 文件目录对象
   * @param existingXAttrs 现有XAttr列表
   * @param toSet 待设置XAttr列表
   * @param flag 设置标志
   * @return 合并后的新XAttr