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
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.PathIsNotDirectoryException;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.fs.XAttr;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.SnapshotException;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockStoragePolicySuite;
import org.apache.hadoop.hdfs.server.namenode.INodeReference.WithCount;
import org.apache.hadoop.hdfs.server.namenode.visitor.NamespaceVisitor;
import org.apache.hadoop.hdfs.server.namenode.snapshot.DirectorySnapshottableFeature;
import org.apache.hadoop.hdfs.server.namenode.snapshot.DirectoryWithSnapshotFeature;
import org.apache.hadoop.hdfs.server.namenode.snapshot.DirectoryWithSnapshotFeature.DirectoryDiffList;
import org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot;
import org.apache.hadoop.hdfs.server.namenode.snapshot.SnapshotManager;
import org.apache.hadoop.hdfs.util.ReadOnlyList;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.util.Preconditions;
import org.apache.hadoop.security.AccessControlException;

import static org.apache.hadoop.hdfs.protocol.HdfsConstants.BLOCK_STORAGE_POLICY_ID_UNSPECIFIED;

/**
 * 文件系统目录INode类，负责管理目录元数据、子节点列表，并支持配额、快照等扩展特性，是HDFS命名空间树的核心节点类型之一。
 */
public class INodeDirectory extends INodeWithAdditionalFields
    implements INodeDirectoryAttributes {

  /**
   * 将通用INode转换为目录INode，校验节点存在性和类型，抛出对应异常。
   * @param inode 待转换的INode对象
   * @param path 目录路径，用于异常信息展示
   * @return 转换后的目录INode对象
   * @throws FileNotFoundException 目录不存在时抛出
   * @throws PathIsNotDirectoryException 节点不是目录时抛出
   */
  public static INodeDirectory valueOf(INode inode, Object path
      ) throws FileNotFoundException, PathIsNotDirectoryException {
    if (inode == null) {
      throw new FileNotFoundException("Directory does not exist: "
          + DFSUtil.path2String(path));
    }
    if (!inode.isDirectory()) {
      throw new PathIsNotDirectoryException(DFSUtil.path2String(path));
    }
    return inode.asDirectory(); 
  }

  // 性能统计显示大多数目录的子节点数量在1-4之间，因此初始化ArrayList使用较小的初始容量优化内存占用
  public static final int DEFAULT_FILES_PER_DIRECTORY = 2;

  static final byte[] ROOT_NAME = DFSUtil.string2Bytes("");

  private List<INode> children = null;
  
  /**
   * 构造方法，创建一个新的目录INode。
   * @param id INode ID
   * @param name 目录名称字节数组
   * @param permissions 权限状态对象
   * @param mtime 修改时间
   */
  public INodeDirectory(long id, byte[] name, PermissionStatus permissions,
      long mtime) {
    super(id, name, permissions, mtime, 0L);
  }
  
  /**
   * 拷贝构造方法，基于已有目录INode创建新节点，可选择是否调整子节点父指针。
   * @param other 待拷贝的源目录INode
   * @param adopt 是否需要将子节点的父指针设置为新节点
   * @param featuresToCopy 需要拷贝到新节点的特性列表，仅做引用拷贝不做深拷贝
   */
  public INodeDirectory(INodeDirectory other, boolean adopt,
      Feature... featuresToCopy) {
    super(other);
    this.children = other.children;
    if (adopt && this.children != null) {
      for (INode child : children) {
        child.setParent(this);
      }
    }
    this.features = featuresToCopy;
    AclFeature aclFeature = getFeature(AclFeature.class);
    if (aclFeature != null) {
      // 对AclFeature做去重处理
      removeFeature(aclFeature);
      addFeature(AclStorage.addAclFeature(aclFeature));
    }
  }

  @Override
  public final boolean isDirectory() {
    return true;
  }

  @Override
  public final INodeDirectory asDirectory() {
    return this;
  }

  @Override
  public byte getLocalStoragePolicyID() {
    XAttrFeature f = getXAttrFeature();
    XAttr xattr = f == null ? null : f.getXAttr(
        BlockStoragePolicySuite.getStoragePolicyXAttrPrefixedName());
    if (xattr != null) {
      return (xattr.getValue())[0];
    }
    return BLOCK_STORAGE_POLICY_ID_UNSPECIFIED;
  }

  @Override
  public byte getStoragePolicyID() {
    byte id = getLocalStoragePolicyID();
    if (id != BLOCK_STORAGE_POLICY_ID_UNSPECIFIED) {
      return id;
    }
    // 当前目录未指定存储策略，向上遍历父节点继承策略
    return getParent() != null ? getParent().getStoragePolicyID() : BLOCK_STORAGE_POLICY_ID_UNSPECIFIED;
  }

  /**
   * 设置目录配额，更新已有配额或创建新的配额特性。
   * @param bsps 块存储策略套件
   * @param nsQuota 名称配额（目录和文件数量上限）
   * @param ssQuota 存储空间配额
   * @param type 存储类型，用于特定存储类型配额
   */
  void setQuota(BlockStoragePolicySuite bsps, long nsQuota, long ssQuota, StorageType type) {
    DirectoryWithQuotaFeature quota = getDirectoryWithQuotaFeature();
    if (quota != null) {
      // 已有配额特性，更新配额值
      if (type != null) {
        quota.setQuota(ssQuota, type);
      } else {
        quota.setQuota(nsQuota, ssQuota);
      }
      if (!isQuotaSet() && !isRoot()) {
        removeFeature(quota);
      }
    } else {
      // 不存在配额特性，计算当前使用量并新建配额特性
      final QuotaCounts c = computeQuotaUsage(bsps);
      DirectoryWithQuotaFeature.Builder builder =
          new DirectoryWithQuotaFeature.Builder().nameSpaceQuota(nsQuota);
      if (type != null) {
        builder.typeQuota(type, ssQuota);
      } else {
        builder.storageSpaceQuota(ssQuota);
      }
      addDirectoryWithQuotaFeature(builder.build()).setSpaceConsumed(c);
    }
  }

  @Override
  public QuotaCounts getQuotaCounts() {
    final DirectoryWithQuotaFeature q = getDirectoryWithQuotaFeature();
    return q != null? q.getQuota(): super.getQuotaCounts();
  }

  @Override
  public void addSpaceConsumed(QuotaCounts counts) {
    super.addSpaceConsumed(counts);

    final DirectoryWithQuotaFeature q = getDirectoryWithQuotaFeature();
    if (q != null && isQuotaSet()) {
      q.addSpaceConsumed2Cache(counts);
    }
  }

  /**
   * 获取目录的配额特性，如果不存在返回null。
   * @return 配额特性对象或null
   */
  public final DirectoryWithQuotaFeature getDirectoryWithQuotaFeature() {
    return getFeature(DirectoryWithQuotaFeature.class);
  }

  /**
   * 检查当前目录是否开启了配额。
   * @return 是否包含配额特性
   */
  final boolean isWithQuota() {
    return getDirectoryWithQuotaFeature() != null;
  }

  /**
   * 添加目录配额特性，校验目录未开启配额。
   * @param q 配额特性对象
   * @return  added 配额特性对象
   */
  DirectoryWithQuotaFeature addDirectoryWithQuotaFeature(
      DirectoryWithQuotaFeature q) {
    Preconditions.checkState(!isWithQuota(), "Directory is already with quota");
    addFeature(q);
    return q;
  }

  /**
   * 在当前目录子节点中二分搜索指定名称的子节点，返回索引位置。
   * @param name 子节点名称字节数组
   * @return 子节点索引，未找到返回-1
   */
  int searchChildren(byte[] name) {
    return children == null? -1: Collections.binarySearch(children, name);
  }
  
  /**
   * 添加目录快照特性，校验目录未开启快照。
   * @param diffs 目录差异列表
   * @return 新建的快照特性对象
   */
  public DirectoryWithSnapshotFeature addSnapshotFeature(
      DirectoryDiffList diffs) {
    Preconditions.checkState(!isWithSnapshot(), 
        "Directory is already with snapshot");
    DirectoryWithSnapshotFeature sf = new DirectoryWithSnapshotFeature(diffs);
    addFeature(sf);
    return sf;
  }
  
  /**
   * 获取目录的快照特性，如果不存在返回null。
   * @return 快照特性对象或null
   */
  public final DirectoryWithSnapshotFeature getDirectoryWithSnapshotFeature() {
    return getFeature(DirectoryWithSnapshotFeature.class);
  }

  /**
   * 检查当前目录是否包含快照特性。
   * @return 是否包含快照特性
   */
  public final boolean isWithSnapshot() {
    return getDirectoryWithSnapshotFeature() != null;
  }

  /**
   * 获取目录的快照差异列表。
   * @return 快照差异列表，无快照特性返回null
   */
  public DirectoryDiffList getDiffs() {
    DirectoryWithSnapshotFeature sf = getDirectoryWithSnapshotFeature();
    return sf != null ? sf.getDiffs() : null;
  }
  
  @Override
  public INodeDirectoryAttributes getSnapshotINode(int snapshotId) {
    DirectoryWithSnapshotFeature sf = getDirectoryWithSnapshotFeature();
    return sf == null ? this : sf.getDiffs().getSnapshotINode(snapshotId, this);
  }
  
  @Override
  public String toDetailString() {
    DirectoryWithSnapshotFeature sf = this.getDirectoryWithSnapshotFeature();
    return super.toDetailString() + (sf == null ? "" : ", " + sf.getDiffs()); 
  }

  /**
   * 获取目录的可快照特性（允许创建快照的根目录特性）。
   * @return 可快照特性对象或null
   */
  public DirectorySnapshottableFeature getDirectorySnapshottableFeature() {
    return getFeature(DirectorySnapshottableFeature.class);
  }

  /**
   * 检查当前目录是否可创建快照。
   * @return 是否可创建快照
   */
  public boolean isSnapshottable() {
    return getDirectorySnapshottableFeature() != null;
  }

  /**
   * 检查当前目录是否是指定快照根目录的后代节点。
   * @param snapshotRootDir 快照根目录
   * @return 是否是后代节点
   */
  public boolean isDescendantOfSnapshotRoot(INodeDirectory snapshotRootDir) {
    Preconditions.checkArgument(snapshotRootDir.isSnapshottable());
    INodeDirectory dir = this;
    while(dir != null) {
      if (dir.equals(snapshotRootDir)) {
        return true;
      }
      dir = dir.getParent();
    }
    return false;
  }

  /**
   * 根据快照名称获取快照对象。
   * @param snapshotName 快照名称字节数组
   * @return 对应快照对象
   */
  public Snapshot getSnapshot(byte[] snapshotName) {
    return getDirectorySnapshottableFeature().getSnapshot(snapshotName);
  }

  /**
   * 设置当前目录允许的最大快照数量配额。
   * @param snapshotQuota 快照数量上限
   */
  public void setSnapshotQuota(int snapshotQuota) {
    getDirectorySnapshottableFeature().setSnapshotQuota(snapshotQuota);
  }

  /**
   * 在当前可快照目录上添加一个新快照。
   * @param snapshotManager 快照管理器
   * @param name 快照名称
   * @param leaseManager 租约管理器
   * @param mtime 快照创建时间
   * @return 新建的快照对象
   * @throws SnapshotException 快照创建失败时抛出
   */
  public Snapshot addSnapshot(SnapshotManager snapshotManager, String name,
      final LeaseManager leaseManager, long mtime)
      throws SnapshotException {
    return getDirectorySnapshottableFeature().addSnapshot(this,
        snapshotManager, name, leaseManager, mtime);
  }

  /**
   * 删除当前目录下指定名称的快照。
   * @param reclaimContext 回收上下文，用于记录需要回收的空间和INode
   * @param snapshotName 待删除快照名称
   * @param mtime 快照删除时间
   * @param snapshotManager 快照管理器
   * @return 被删除的快照对象
   * @throws SnapshotException 快照删除失败时抛出
   */
  public Snapshot removeSnapshot(ReclaimContext reclaimContext,
      String snapshotName, long mtime, SnapshotManager snapshotManager)
      throws SnapshotException {
    return getDirectorySnapshottableFeature().removeSnapshot(
        reclaimContext, this, snapshotName, mtime, snapshotManager);
  }

  /**
   * 重命名当前目录下的快照。
   * @param path 快照根目录路径
   * @param oldName 原快照名称
   * @param newName 新快照名称
   * @param mtime 修改时间
   * @throws SnapshotException 重命名失败时抛出
   */
  public void renameSnapshot(String path, String oldName, String newName,
      long mtime) throws SnapshotException {
    getDirectorySnapshottableFeature().renameSnapshot(path, oldName, newName,
        mtime);
  }

  /**
   * 为当前目录添加可快照特性，允许在该目录上创建快照。
   */
  public void addSnapshottableFeature() {
    Preconditions.checkState(!isSnapshottable(),
        "this is already snapshottable, this=%s", this);
    DirectoryWithSnapshotFeature s = this.getDirectoryWithSnapshotFeature();
    final DirectorySnapshottableFeature snapshottable =
        new DirectorySnapshottableFeature(s);
    if (s != null) {
      this.removeFeature(s);
    }
    this.addFeature(snapshottable);
  }

  /**
   * 移除当前目录的可快照特性，禁止继续在该目录创建快照。
   */
  public void removeSnapshottableFeature() {
    DirectorySnapshottableFeature s = getDirectorySnapshottableFeature();
    Preconditions.checkState(s != null,
        "The dir does not have snapshottable feature: this=%s", this);
    this.removeFeature(s);
    if (s.getDiffs().asList().size() > 0) {
      // 仍存在快照差异，添加回普通快照特性保留差异数据
      DirectoryWithSnapshotFeature sf = new DirectoryWithSnapshotFeature(
          s.getDiffs());
      addFeature(sf);
    }
  }

  /**
   * 替换目录中指定的旧子节点为新子节点，仅用于引用节点替换场景，并更新快照差异和INode映射。
   * @param oldChild 待替换的旧子节点
   * @param newChild 新子节点
   * @param inodeMap INode映射表，用于更新新节点映射
   */
  public void replaceChild(INode oldChild, final INode newChild,
      final INodeMap inodeMap) {
    Preconditions.checkNotNull(children);
    final int i = searchChildren(newChild.getLocalNameBytes());
    Preconditions.checkState(i >= 0);
    Preconditions.checkState(oldChild == children.get(i)
        || oldChild == children.get(i).asReference().getReferredINode()
            .asReference().getReferredINode());
    oldChild = children.get(i);
    
    if (oldChild.isReference() && newChild.isReference()) {
      // 两个都是引用节点，减少原引用节点的引用计数
      final INodeReference.WithCount withCount = 
          (WithCount) oldChild.asReference().getReferredINode();
      withCount.removeReference(oldChild.asReference());
    }
    children.set(i, newChild);
    
    // 更新快照差异创建列表中的节点引用
    DirectoryWithSnapshotFeature sf = this.getDirectoryWithSnapshotFeature();
    if (sf != null) {
      sf.getDiffs().replaceCreatedChild(oldChild, newChild);
    }