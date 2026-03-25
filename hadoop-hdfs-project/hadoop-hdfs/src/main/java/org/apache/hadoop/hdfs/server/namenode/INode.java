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

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.util.Preconditions;
import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableMap;
import org.apache.hadoop.thirdparty.com.google.common.collect.Maps;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockStoragePolicySuite;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockUnderConstructionFeature;
import org.apache.hadoop.hdfs.server.namenode.INodeReference.DstReference;
import org.apache.hadoop.hdfs.server.namenode.INodeReference.WithCount;
import org.apache.hadoop.hdfs.server.namenode.INodeReference.WithName;
import org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot;
import org.apache.hadoop.hdfs.server.namenode.visitor.NamespaceVisitor;
import org.apache.hadoop.hdfs.util.Diff;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.util.ChunkedArrayList;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * 文件系统目录树节点的抽象基类，HDFS NameNode在内存中维护文件/块层级结构的核心数据结构，
 * 封装了文件、目录、符号链接等不同类型节点共有的属性和操作。
 */
@InterfaceAudience.Private
public abstract class INode implements INodeAttributes, Diff.Element<byte[]> {
  public static final Logger LOG = LoggerFactory.getLogger(INode.class);

  /** 父节点，可以是普通目录INodeDirectory，也可以是引用节点INodeReference（用于快照重命名场景）*/
  private INode parent = null;

  INode(INode parent) {
    this.parent = parent;
  }

  /** 获取当前inode的唯一标识ID */
  public abstract long getId();

  /**
   * 检查当前节点是否是根目录节点
   */
  final boolean isRoot() {
    return getLocalNameBytes().length == 0;
  }

  /** 获取指定快照版本下的权限状态 */
  public abstract PermissionStatus getPermissionStatus(int snapshotId);

  /** 获取当前状态下的权限状态 */
  final PermissionStatus getPermissionStatus() {
    return getPermissionStatus(Snapshot.CURRENT_STATE_ID);
  }

  /**
   * 获取指定快照版本下的用户名
   * @param snapshotId 快照ID，如果不是CURRENT_STATE_ID则从指定快照获取，否则从当前节点获取
   * @return 用户名
   */
  abstract String getUserName(int snapshotId);

  @Override
  public final String getUserName() {
    return getUserName(Snapshot.CURRENT_STATE_ID);
  }

  /** 设置用户名 */
  abstract void setUser(String user);

  /**
   * 支持快照版本的用户名设置，先记录修改到快照再更新
   */
  final INode setUser(String user, int latestSnapshotId) {
    recordModification(latestSnapshotId);
    setUser(user);
    return this;
  }
  /**
   * 获取指定快照版本下的用户组名
   * @param snapshotId 快照ID，如果不是CURRENT_STATE_ID则从指定快照获取，否则从当前节点获取
   * @return 用户组名
   */
  abstract String getGroupName(int snapshotId);

  @Override
  public final String getGroupName() {
    return getGroupName(Snapshot.CURRENT_STATE_ID);
  }

  /** 设置用户组 */
  abstract void setGroup(String group);

  /**
   * 支持快照版本的用户组设置，先记录修改到快照再更新
   */
  final INode setGroup(String group, int latestSnapshotId) {
    recordModification(latestSnapshotId);
    setGroup(group);
    return this;
  }

  /**
   * 获取指定快照版本下的权限
   * @param snapshotId 快照ID，如果不是CURRENT_STATE_ID则从指定快照获取，否则从当前节点获取
   * @return 权限对象
   */
  abstract FsPermission getFsPermission(int snapshotId);
  
  @Override
  public final FsPermission getFsPermission() {
    return getFsPermission(Snapshot.CURRENT_STATE_ID);
  }

  /** 设置当前节点权限 */
  abstract void setPermission(FsPermission permission);

  /**
   * 支持快照版本的权限设置，先记录修改到快照再更新
   */
  INode setPermission(FsPermission permission, int latestSnapshotId) {
    recordModification(latestSnapshotId);
    setPermission(permission);
    return this;
  }

  /** 获取指定快照版本下的ACL特性 */
  abstract AclFeature getAclFeature(int snapshotId);

  @Override
  public final AclFeature getAclFeature() {
    return getAclFeature(Snapshot.CURRENT_STATE_ID);
  }

  /** 添加ACL特性 */
  abstract void addAclFeature(AclFeature aclFeature);

  /**
   * 支持快照版本的ACL特性添加，先记录修改到快照再添加
   */
  final INode addAclFeature(AclFeature aclFeature, int latestSnapshotId) {
    recordModification(latestSnapshotId);
    addAclFeature(aclFeature);
    return this;
  }

  /** 移除ACL特性 */
  abstract void removeAclFeature();

  /**
   * 支持快照版本的ACL特性移除，先记录修改到快照再移除
   */
  final INode removeAclFeature(int latestSnapshotId) {
    recordModification(latestSnapshotId);
    removeAclFeature();
    return this;
  }

  /**
   * 获取指定快照版本下的XAttr扩展属性特性
   * @param snapshotId 快照ID，如果不是CURRENT_STATE_ID则从指定快照获取，否则从当前节点获取
   * @return XAttr特性对象
   */  
  abstract XAttrFeature getXAttrFeature(int snapshotId);
  
  @Override
  public final XAttrFeature getXAttrFeature() {
    return getXAttrFeature(Snapshot.CURRENT_STATE_ID);
  }
  
  /** 添加XAttr扩展属性特性 */
  abstract void addXAttrFeature(XAttrFeature xAttrFeature);
  
  /**
   * 支持快照版本的XAttr特性添加，先记录修改到快照再添加
   */
  final INode addXAttrFeature(XAttrFeature xAttrFeature, int latestSnapshotId) {
    recordModification(latestSnapshotId);
    addXAttrFeature(xAttrFeature);
    return this;
  }
  
  /** 移除XAttr扩展属性特性 */
  abstract void removeXAttrFeature();
  
  /**
   * 支持快照版本的XAttr特性移除，先记录修改到快照再移除
   */
  final INode removeXAttrFeature(int lastestSnapshotId) {
    recordModification(lastestSnapshotId);
    removeXAttrFeature();
    return this;
  }
  
  /**
   * 获取指定快照ID对应的inode，如果是当前状态则返回自身，否则返回对应快照版本节点
   * @return 对应版本的inode属性对象
   */
  public INodeAttributes getSnapshotINode(final int snapshotId) {
    return this;
  }

  /** 检查当前节点是否存在于文件系统当前状态（未被删除） */
  public boolean isInCurrentState() {
    if (isRoot()) {
      return true;
    }
    final INodeDirectory parentDir = getParent();
    if (parentDir == null) {
      return false; // 该节点仅存在于快照中，当前状态已被删除
    }
    if (!parentDir.isInCurrentState()) {
      return false;
    }
    final INode child = parentDir.getChild(getLocalNameBytes(),
            Snapshot.CURRENT_STATE_ID);
    if (this == child) {
      return true;
    }
    return child != null && child.isReference() &&
        this.equals(child.asReference().getReferredINode());
  }

  /** 检查当前节点是否存在于指定最新快照中 */
  public final boolean isInLatestSnapshot(final int latestSnapshotId) {
    if (latestSnapshotId == Snapshot.CURRENT_STATE_ID ||
        latestSnapshotId == Snapshot.NO_SNAPSHOT_ID) {
      return false;
    }
    // 如果父节点已经是引用节点，说明经过重命名，直接判定存在
    if (parent != null && parent.isReference()) {
      return true;
    }
    final INodeDirectory parentDir = getParent();
    if (parentDir == null) { // root
      return true;
    }
    if (!parentDir.isInLatestSnapshot(latestSnapshotId)) {
      return false;
    }
    final INode child = parentDir.getChild(getLocalNameBytes(), latestSnapshotId);
    if (this == child) {
      return true;
    }
    return child != null && child.isReference() &&
        this == child.asReference().getReferredINode();
  }
  
  /** 检查给定目录是否是当前节点的祖先目录 */
  public final boolean isAncestorDirectory(final INodeDirectory dir) {
    for(INodeDirectory p = getParent(); p != null; p = p.getParent()) {
      if (p == dir) {
        return true;
      }
    }
    return false;
  }

  /**
   * 判断被引用节点的修改应该记录到源树快照还是目标树快照
   * 用于重命名操作产生的引用节点场景下的快照修改记录规则
   * @param latestInDst 引用节点上方目标树的最新快照ID
   * @return true表示修改记录到源树快照，false表示记录到目标树快照
   */
  public final boolean shouldRecordInSrcSnapshot(final int latestInDst) {
    Preconditions.checkState(!isReference());

    if (latestInDst == Snapshot.CURRENT_STATE_ID) {
      return true;
    }
    INodeReference withCount = getParentReference();
    if (withCount != null) {
      int dstSnapshotId = withCount.getParentReference().getDstSnapshotId();
      if (dstSnapshotId != Snapshot.CURRENT_STATE_ID
          && dstSnapshotId >= latestInDst) {
        return true;
      }
    }
    return false;
  }

  /**
   * 记录inode修改，将修改前版本保存到最新快照中，用于支持快照功能
   * @param latestSnapshotId 最新快照ID，如果没有快照则为CURRENT_STATE_ID
   */
  abstract void recordModification(final int latestSnapshotId);

  /** 检查当前节点是否是引用节点 */
  public boolean isReference() {
    return false;
  }

  /** 将当前节点转换为INodeReference类型，非引用节点调用会抛出异常 */
  public INodeReference asReference() {
    throw new IllegalStateException("Current inode is not a reference: "
        + this.toDetailString());
  }

  /**
   * 检查当前节点是否是文件节点
   */
  public boolean isFile() {
    return false;
  }

  /**
   * 检查当前节点自身是否设置了存储策略
   */
  public boolean isSetStoragePolicy() {
    if (isSymlink()) {
      return false;
    }
    return getLocalStoragePolicyID() != HdfsConstants.BLOCK_STORAGE_POLICY_ID_UNSPECIFIED;
  }

  /** 将当前节点转换为INodeFile类型，非文件节点调用会抛出异常 */
  public INodeFile asFile() {
    throw new IllegalStateException("Current inode is not a file: "
        + this.toDetailString());
  }

  /**
   * 检查当前节点是否是目录节点
   */
  public boolean isDirectory() {
    return false;
  }

  /** 将当前节点转换为INodeDirectory类型，非目录节点调用会抛出异常 */
  public INodeDirectory asDirectory() {
    throw new IllegalStateException("Current inode is not a directory: "
        + this.toDetailString());
  }

  /**
   * 检查当前节点是否是符号链接节点
   */
  public boolean isSymlink() {
    return false;
  }

  /** 将当前节点转换为INodeSymlink类型，非符号链接节点调用会抛出异常 */
  public INodeSymlink asSymlink() {
    throw new IllegalStateException("Current inode is not a symlink: "
        + this.toDetailString());
  }

  /**
   * 清理当前节点下的子树，收集需要删除或更新的块，用于删除操作
   * 不同类型节点和场景有不同的清理规则，处理当前树删除和快照删除两种场景
   * @param reclaimContext 回收上下文，记录需要回收的块和inode
   * @param snapshotId 要删除的快照ID，CURRENT_STATE_ID表示删除当前文件/目录
   * @param priorSnapshotId 被删除快照之前的最新快照ID，删除当前节点时表示最新快照
   */
  public abstract void cleanSubtree(ReclaimContext reclaimContext,
      final int snapshotId, int priorSnapshotId);

  /**
   * 销毁当前节点并收集所有需要删除的块，递归处理子树，清理所有引用和差异列表
   * @param reclaimContext 回收上下文，记录需要回收的块和inode
   */
  public abstract void destroyAndCollectBlocks(ReclaimContext reclaimContext);

  /** 计算当前子树的内容摘要，阻塞调用 */
  public final ContentSummary computeContentSummary(
      BlockStoragePolicySuite bsps) throws AccessControlException {
    return computeAndConvertContentSummary(Snapshot.CURRENT_STATE_ID,
        new ContentSummaryComputationContext(bsps));
  }

  /**
   * 计算完成后转换为ContentSummary对象返回
   */
  public final ContentSummary computeAndConvertContentSummary(int snapshotId,
      ContentSummaryComputationContext summary) throws AccessControlException {
    computeContentSummary(snapshotId, summary);
    final ContentCounts counts = summary.getCounts();
    final ContentCounts snapshotCounts = summary.getSnapshotCounts();
    final QuotaCounts q = getQuotaCounts();
    return new ContentSummary.Builder().
        length(counts.getLength()).
        fileCount(counts.getFileCount() + counts.getSymlinkCount()).
        directoryCount(counts.getDirectoryCount()).
        quota(q.getNameSpace()).
        spaceConsumed(counts.getStoragespace()).
        spaceQuota(q.getStorageSpace()).
        typeConsumed(counts.getTypeSpaces()).
        typeQuota(q.getTypeSpaces().asArray()).
        snapshotLength(snapshotCounts.getLength()).
        snapshotFileCount(snapshotCounts.getFileCount()).
        snapshotDirectoryCount(snapshotCounts.getDirectoryCount()).
        snapshotSpaceConsumed(snapshotCounts.getStoragespace()).
        erasureCodingPolicy(summary.getErasureCodingPolicyName(this)).
        build();
  }

  /**
   * 计算指定快照范围内子树的内容摘要
   * @param snapshotId 计算范围，CURRENT_STATE_ID表示包含当前状态和所有快照，否则仅包含指定快照
   * @param summary 保存计算结果的上下文对象
   * @return 计算上下文对象
   */
  public abstract ContentSummaryComputationContext computeContentSummary(
      int snapshotId, ContentSummaryComputationContext summary)
      throws AccessControlException;


  /**
   * 将空间使用量增量添加到当前节点并向上传播到所有祖先节点
   */
  public void addSpaceConsumed(QuotaCounts counts) {
    if (parent != null) {
      parent.addSpaceConsumed(counts);
    }
  }

  /**
   * 获取当前节点设置的配额信息，未设置则配额值为-1
   * @return 配额计数对象
   */
  public QuotaCounts getQuotaCounts() {
    return new QuotaCounts.Builder().
        nameSpace(HdfsConstants.QUOTA_RESET).
        storageSpace(HdfsConstants.QUOTA_RESET).
        typeSpaces(HdfsConstants.QUOTA_RESET).
        build();
  }

  /** 检查当前节点是否设置了配额 */
  public final boolean isQuotaSet() {
    final QuotaCounts qc = getQuotaCounts();
    return qc.anyNsSsCountGreaterOrEqual(0) || qc.anyTypeSpaceCountGreaterOrEqual(0);
  }

  /**
   * 计算当前子树的命名空间和存储空间配额使用量入口方法，初始化存储策略ID
   */
  public final QuotaCounts computeQuotaUsage(