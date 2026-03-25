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

import java.io.PrintWriter;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockStoragePolicySuite;
import org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot;
import org.apache.hadoop.hdfs.server.namenode.visitor.NamespaceVisitor;

/**
 * 文件目录树中表示符号链接的INode实现，继承INodeWithAdditionalFields提供基础能力
 * HDFS中符号链接仅存储指向目标路径，不存储实际数据块，不支持ACL、XAttr和存储策略
 */
@InterfaceAudience.Private
public class INodeSymlink extends INodeWithAdditionalFields {
  // 符号链接指向的目标URI，字节数组存储
  private final byte[] symlink;

  /**
   * 构造符号链接INode，初始化链接目标
   * @param id INode ID
   * @param name 符号链接名称字节数组
   * @param permissions 权限状态
   * @param mtime 修改时间
   * @param atime 访问时间
   * @param symlink 符号链接指向的目标路径
   */
  INodeSymlink(long id, byte[] name, PermissionStatus permissions,
      long mtime, long atime, String symlink) {
    super(id, name, permissions, mtime, atime);
    this.symlink = DFSUtil.string2Bytes(symlink);
  }
  
  /**
   * 拷贝构造函数，基于已有符号链接创建新的符号链接INode
   * @param that 要拷贝的源符号链接INode
   */
  INodeSymlink(INodeSymlink that) {
    super(that);
    this.symlink = that.symlink;
  }

  /**
   * 记录符号链接修改，用于快照功能处理修改操作
   * 若当前节点已在最新快照中，则将当前节点保存到父目录快照中
   * @param latestSnapshotId 最新快照ID
   */
  @Override
  void recordModification(int latestSnapshotId) {
    if (isInLatestSnapshot(latestSnapshotId)) {
      INodeDirectory parent = getParent();
      parent.saveChild2Snapshot(this, latestSnapshotId, new INodeSymlink(this));
    }
  }

  @Override
  public boolean isSymlink() {
    return true;
  }

  @Override
  public INodeSymlink asSymlink() {
    return this;
  }

  /**
   * 获取符号链接指向的目标路径字符串
   * @return 目标路径字符串
   */
  public String getSymlinkString() {
    return DFSUtil.bytes2String(symlink);
  }

  /**
   * 获取符号链接指向的目标路径字节数组
   * @return 目标路径字节数组
   */
  public byte[] getSymlink() {
    return symlink;
  }
  
  /**
   * 清理符号链接子树，符号链接没有子节点，仅在当前状态且无前序快照时直接销毁
   * @param reclaimContext 回收上下文，用于统计需要回收的资源
   * @param snapshotId 当前快照ID
   * @param priorSnapshotId 前序快照ID
   */
  @Override
  public void cleanSubtree(ReclaimContext reclaimContext, final int snapshotId,
      int priorSnapshotId) {
    if (snapshotId == Snapshot.CURRENT_STATE_ID
        && priorSnapshotId == Snapshot.NO_SNAPSHOT_ID) {
      destroyAndCollectBlocks(reclaimContext);
    }
  }
  
  /**
   * 销毁当前符号链接，收集需要回收的配额空间
   * @param reclaimContext 回收上下文，用于添加回收项
   */
  @Override
  public void destroyAndCollectBlocks(ReclaimContext reclaimContext) {
    reclaimContext.removedINodes.add(this);
    reclaimContext.quotaDelta().add(
        new QuotaCounts.Builder().nameSpace(1).build());
  }

  /**
   * 计算符号链接占用的配额，符号链接仅占用1个命名空间配额
   * @param bsps 块存储策略套件
   * @param blockStoragePolicyId 块存储策略ID
   * @param useCache 是否使用缓存
   * @param lastSnapshotId 最新快照ID
   * @return 计算后的配额计数
   */
  @Override
  public QuotaCounts computeQuotaUsage(BlockStoragePolicySuite bsps,
      byte blockStoragePolicyId, boolean useCache, int lastSnapshotId) {
    return new QuotaCounts.Builder().nameSpace(1).build();
  }

  /**
   * 计算符号链接的目录内容统计，增加1个符号链接计数
   * @param snapshotId 快照ID
   * @param summary 内容统计上下文
   * @return 内容统计上下文
   */
  @Override
  public ContentSummaryComputationContext computeContentSummary(int snapshotId,
      final ContentSummaryComputationContext summary) {
    summary.getCounts().addContent(Content.SYMLINK, 1);
    return summary;
  }

  /**
   * 递归打印树形结构时输出当前符号链接信息，包含目标路径
   * @param out 输出打印流
   * @param prefix 输出前缀
   * @param snapshot 快照ID
   */
  @Override
  public void dumpTreeRecursively(PrintWriter out, StringBuilder prefix,
      final int snapshot) {
    super.dumpTreeRecursively(out, prefix, snapshot);
    out.print(" ~> ");
    out.println(getSymlinkString());
  }

  /**
   * 接受命名空间访问者访问，调用访问者的符号链接访问方法
   * @param visitor 命名空间访问者
   * @param snapshot 快照ID
   */
  @Override
  public void accept(NamespaceVisitor visitor, int snapshot) {
    visitor.visitSymlink(this, snapshot);
  }

  /**
   * 移除ACL特性，符号链接不支持ACL，抛出不支持操作异常
   */
  @Override
  public void removeAclFeature() {
    throw new UnsupportedOperationException("ACLs are not supported on symlinks");
  }

  /**
   * 添加ACL特性，符号链接不支持ACL，抛出不支持操作异常
   */
  @Override
  public void addAclFeature(AclFeature f) {
    throw new UnsupportedOperationException("ACLs are not supported on symlinks");
  }

  /**
   * 获取扩展属性特性，符号链接不支持XAttr，抛出不支持操作异常
   */
  @Override
  final XAttrFeature getXAttrFeature(int snapshotId) {
    throw new UnsupportedOperationException("XAttrs are not supported on symlinks");
  }
  
  /**
   * 移除扩展属性特性，符号链接不支持XAttr，抛出不支持操作异常
   */
  @Override
  public void removeXAttrFeature() {
    throw new UnsupportedOperationException("XAttrs are not supported on symlinks");
  }
  
  /**
   * 添加扩展属性特性，符号链接不支持XAttr，抛出不支持操作异常
   */
  @Override
  public void addXAttrFeature(XAttrFeature f) {
    throw new UnsupportedOperationException("XAttrs are not supported on symlinks");
  }

  /**
   * 获取存储策略ID，符号链接不支持存储策略，抛出不支持操作异常
   */
  @Override
  public byte getStoragePolicyID() {
    throw new UnsupportedOperationException(
        "Storage policy are not supported on symlinks");
  }

  /**
   * 获取本地存储策略ID，符号链接不支持存储策略，抛出不支持操作异常
   */
  @Override
  public byte getLocalStoragePolicyID() {
    throw new UnsupportedOperationException(
        "Storage policy are not supported on symlinks");
  }
}