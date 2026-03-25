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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockStoragePolicySuite;
import org.apache.hadoop.hdfs.server.namenode.snapshot.DirectoryWithSnapshotFeature;
import org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot;

import org.apache.hadoop.util.Preconditions;
import org.apache.hadoop.hdfs.server.namenode.visitor.NamespaceVisitor;
import org.apache.hadoop.security.AccessControlException;

/**
 * 文件系统INode引用抽象基类
 * <p>
 * 本类及其子类用于支持HDFS快照和重命名/移动操作后的多访问路径功能。
 * 当文件/目录被快照保存后又被重命名/移动到其他位置时，会产生多个访问路径指向同一个实际INode。
 * <p>
 * 使用示例说明：
 * (1) 初始路径 /abc/foo，对应INode id=1000。foo是在快照s0之后创建，因此不在s0中，
 *     会被放在/abc针对s0差异条目的创建列表中。
 * (2) 对/abc创建快照s1、s2，此时foo存在于s1和s2中，假设/xyz的最新快照是sDst。
 * (3) 执行mv /abc/foo /xyz/bar，INode id=1000从名称"foo"改名为"bar"，父节点变为/xyz。
 * <p>
 * 此时 /xyz/bar、/abc/.snapshot/s1/foo 和 /abc/.snapshot/s2/foo 是指向同一个INode(id=1000,name=bar)的不同访问路径。
 * 本类通过引用链实现多路径访问：
 * - 原位置/abc/foo的INode被替换为WithName(name=foo,lastSnapshot=s2)，并放入/abc针对s2差异条目的删除列表，
 *   同时也替换原s0创建列表中的对应INode。此时/abc/foo仍存在于s1和s2中，不存在于s0。
 * - 目标位置/xyz添加一个DstReference(dstSnapshot=sDst)，放入/xyz针对sDst差异条目的创建列表，此时/xyz/bar不存在于sDst中。
 * - WithName和DstReference都指向另一个引用WithCount(count=2)。
 * - 最后WithCount指向实际INode(id=1000,name=bar)，该INode名称已经改为bar。
 * <p>
 * 注意事项：
 * 1. 除WithName外，其他引用类型使用被引用INode自身的名称，WithCount和DstReference不保存独立名称。
 * 2. getParent()始终返回当前状态下的父节点，例如inode(id=1000,name=bar).getParent()返回/xyz而非原/abc。
 * 3. {@link INodeReference#getId()}始终返回被引用INode的ID，上述所有引用都返回id=1000。
 */
public abstract class INodeReference extends INode {
  /** 断言当前节点和引用链的关系正确性，供调试验证使用 */
  abstract void assertReferences();

  @Override
  public String toDetailString() {
    final String s = referred == null? null
        : referred.getFullPathAndObjectString();
    return super.toDetailString() + ", ->" + s;
  }

  /**
   * 尝试移除给定INode的一个引用，并返回剩余引用计数
   * 如果给定INode不是引用类型，直接返回-1
   * @param inode 待移除引用的INode
   * @return 移除后剩余引用计数，非引用或被引用节点不是WithCount则返回-1
   */
  public static int tryRemoveReference(INode inode) {
    if (!inode.isReference()) {
      return -1;
    }
    return removeReference(inode.asReference());
  }

  /**
   * 移除给定引用，并返回剩余引用计数
   * 如果被引用INode不是WithCount类型，返回-1
   * @param ref 待移除的引用对象
   * @return 移除后剩余引用计数
   */
  private static int removeReference(INodeReference ref) {
    final INode referred = ref.getReferredINode();
    if (!(referred instanceof WithCount)) {
      return -1;
    }
    
    WithCount wc = (WithCount) referred;
    wc.removeReference(ref);
    return wc.getReferenceCount();
  }

  /**
   * 获取引用节点创建之前的最新快照ID，用于销毁引用节点时清理数据
   * @param ref 待查询的引用节点
   * @return 前置快照ID，无则返回Snapshot.NO_SNAPSHOT_ID
   */
  static int getPriorSnapshot(INodeReference ref) {
    WithCount wc = (WithCount) ref.getReferredINode();
    WithName wn = null;
    if (ref instanceof DstReference) {
      wn = wc.getLastWithName();
    } else if (ref instanceof WithName) {
      wn = wc.getPriorWithName((WithName) ref);
    }
    if (wn != null) {
      INode referred = wc.getReferredINode();
      if (referred.isFile() && referred.asFile().isWithSnapshot()) {
        return referred.asFile().getDiffs().getPrior(wn.lastSnapshotId);
      } else if (referred.isDirectory()) {
        DirectoryWithSnapshotFeature sf = referred.asDirectory()
            .getDirectoryWithSnapshotFeature();
        if (sf != null) {
          return sf.getDiffs().getPrior(wn.lastSnapshotId);
        }
      }
    }
    return Snapshot.NO_SNAPSHOT_ID;
  }
  
  /** 被当前引用指向的目标INode */
  private INode referred;
  
  /**
   * 构造INode引用对象
   * @param parent 父节点INode
   * @param referred 被引用的目标INode
   */
  public INodeReference(INode parent, INode referred) {
    super(parent);
    this.referred = referred;
  }

  /**
   * 获取被引用的目标INode
   * @return 被引用的INode对象
   */
  public final INode getReferredINode() {
    return referred;
  }

  @Override
  public final boolean isReference() {
    return true;
  }
  
  @Override
  public final INodeReference asReference() {
    return this;
  }

  @Override
  public final boolean isFile() {
    return referred.isFile();
  }
  
  @Override
  public final INodeFile asFile() {
    return referred.asFile();
  }
  
  @Override
  public final boolean isDirectory() {
    return referred.isDirectory();
  }
  
  @Override
  public final INodeDirectory asDirectory() {
    return referred.asDirectory();
  }
  
  @Override
  public final boolean isSymlink() {
    return referred.isSymlink();
  }
  
  @Override
  public final INodeSymlink asSymlink() {
    return referred.asSymlink();
  }

  @Override
  public byte[] getLocalNameBytes() {
    return referred.getLocalNameBytes();
  }

  @Override
  public void setLocalName(byte[] name) {
    referred.setLocalName(name);
  }

  @Override
  public final long getId() {
    return referred.getId();
  }
  
  @Override
  public final PermissionStatus getPermissionStatus(int snapshotId) {
    return referred.getPermissionStatus(snapshotId);
  }
  
  @Override
  public final String getUserName(int snapshotId) {
    return referred.getUserName(snapshotId);
  }
  
  @Override
  final void setUser(String user) {
    referred.setUser(user);
  }
  
  @Override
  public final String getGroupName(int snapshotId) {
    return referred.getGroupName(snapshotId);
  }
  
  @Override
  final void setGroup(String group) {
    referred.setGroup(group);
  }
  
  @Override
  public final FsPermission getFsPermission(int snapshotId) {
    return referred.getFsPermission(snapshotId);
  }

  @Override
  final AclFeature getAclFeature(int snapshotId) {
    return referred.getAclFeature(snapshotId);
  }

  @Override
  final void addAclFeature(AclFeature aclFeature) {
    referred.addAclFeature(aclFeature);
  }

  @Override
  final void removeAclFeature() {
    referred.removeAclFeature();
  }
  
  @Override
  final XAttrFeature getXAttrFeature(int snapshotId) {
    return referred.getXAttrFeature(snapshotId);
  }
  
  @Override
  final void addXAttrFeature(XAttrFeature xAttrFeature) {
    referred.addXAttrFeature(xAttrFeature);
  }
  
  @Override
  final void removeXAttrFeature() {
    referred.removeXAttrFeature();
  }

  @Override
  public final short getFsPermissionShort() {
    return referred.getFsPermissionShort();
  }
  
  @Override
  void setPermission(FsPermission permission) {
    referred.setPermission(permission);
  }

  @Override
  public long getPermissionLong() {
    return referred.getPermissionLong();
  }

  @Override
  public final long getModificationTime(int snapshotId) {
    return referred.getModificationTime(snapshotId);
  }
  
  @Override
  public final INode updateModificationTime(long mtime, int latestSnapshotId) {
    return referred.updateModificationTime(mtime, latestSnapshotId);
  }
  
  @Override
  public final void setModificationTime(long modificationTime) {
    referred.setModificationTime(modificationTime);
  }
  
  @Override
  public final long getAccessTime(int snapshotId) {
    return referred.getAccessTime(snapshotId);
  }
  
  @Override
  public final void setAccessTime(long accessTime) {
    referred.setAccessTime(accessTime);
  }

  @Override
  public final byte getStoragePolicyID() {
    return referred.getStoragePolicyID();
  }

  @Override
  public final byte getLocalStoragePolicyID() {
    return referred.getLocalStoragePolicyID();
  }

  @Override
  final void recordModification(int latestSnapshotId) {
    referred.recordModification(latestSnapshotId);
  }

  @Override // used by WithCount
  public void cleanSubtree(
      ReclaimContext reclaimContext, int snapshot, int prior) {
    referred.cleanSubtree(reclaimContext, snapshot, prior);
  }

  @Override // used by WithCount
  public void destroyAndCollectBlocks(ReclaimContext reclaimContext) {
    if (removeReference(this) <= 0) {
      referred.destroyAndCollectBlocks(reclaimContext);
    }
  }

  @Override
  public ContentSummaryComputationContext computeContentSummary(int snapshotId,
      ContentSummaryComputationContext summary) throws AccessControlException {
    return referred.computeContentSummary(snapshotId, summary);
  }

  @Override
  public QuotaCounts computeQuotaUsage(BlockStoragePolicySuite bsps,
      byte blockStoragePolicyId, boolean useCache, int lastSnapshotId) {
    return referred.computeQuotaUsage(bsps, blockStoragePolicyId, useCache,
        lastSnapshotId);
  }

  @Override
  public final INodeAttributes getSnapshotINode(int snapshotId) {
    return referred.getSnapshotINode(snapshotId);
  }

  @Override
  public QuotaCounts getQuotaCounts() {
    return referred.getQuotaCounts();
  }

  @Override
  public final void clear() {
    super.clear();
    referred = null;
  }

  @Override
  public void dumpTreeRecursively(PrintWriter out, StringBuilder prefix,
      final int snapshot) {
    super.dumpTreeRecursively(out, prefix, snapshot);
    // 打印DstReference的快照ID信息
    if (this instanceof DstReference) {
      out.print(", dstSnapshotId=" + ((DstReference) this).dstSnapshotId);
    }
    // 打印WithCount的引用计数信息
    if (this instanceof WithCount) {
      out.print(", " + ((WithCount)this).getCountDetails());
    }
    out.println();
    
    // 构造缩进前缀，指向被引用INode
    final StringBuilder b = new StringBuilder();
    for(int i = 0; i < prefix.length(); i++) {
      b.append(' ');
    }
    b.append("->");
    getReferredINode().dumpTreeRecursively(out, b, snapshot);
  }

  @Override
  public void accept(NamespaceVisitor visitor, int snapshot) {
    visitor.visitReferenceRecursively(this, snapshot);
  }

  /**
   * 获取目标快照ID，默认返回当前状态ID
   * @return 目标快照ID
   */
  public int getDstSnapshotId() {
    return Snapshot.CURRENT_STATE_ID;
  }
  
  /**
   * 带引用计数的匿名引用类，维护指向实际INode的多个引用计数
   * 作为WithName和DstReference共同指向的中间节点，汇总所有引用计数，当计数归零时销毁实际INode
   */
  public static class WithCount extends INodeReference {

    /** 存储所有指向本节点的WithName引用列表，按lastSnapshotId升序排列 */
    private final List<WithName> withNameList = new ArrayList<>();

    /**
     * WithName比较器，按lastSnapshotId升序比较，用于二分查找排序
     */
    public static final Comparator<WithName> WITHNAME_COMPARATOR
        = new Comparator<WithName>() {
      @Override
      public int compare(WithName left, WithName right) {
        return left.lastSnapshotId - right.lastSnapshotId;
      }
    };
    
    /**
     * 构造WithCount引用节点
     * @param parent 父引用，必须为null
     * @param referred 被引用的实际INode，必须不是引用类型
     */
    public WithCount(INodeReference parent, INode referred) {
      super(parent, referred);
      Preconditions.checkArgument(!referred.isReference());
      Preconditions.checkArgument(parent == null);
      referred.setParentReference(this);

      INodeReferenceValidation.add(this, WithCount.class);
    }

    /**
     * 获取引用计数详情字符串，用于调试输出
     * @return 引用计数和WithName列表详情
     */
    public String getCountDetails() {
      final StringBuilder b = new StringBuilder("[");
      if (!withNameList.isEmpty()) {
        final Iterator<WithName> i = withNameList.iterator();
        b.append(i.next().getNameDetails());
        for(; i.hasNext();) {
          b.append(", ").append(i.next().getNameDetails());
        }
      }
      b.append("]");
      return ", count=" + getReferenceCount() + ", names=" + b;
    }

    @Override
    public String toDetailString() {
      return super.toDetailString() + getCountDetails();
    }

    /**
     * 断言父引用必须是DstReference类型，验证引用关系正确性
     * @param parentRef 父引用对象
     */
    private void assertDstReference(INodeReference parentRef) {
      if (parentRef instanceof DstReference) {
        return;
      }
      throw new IllegalArgumentException("Unexpected non-DstReference:"
          + "\n  parentRef: " + parentRef.toDetailString()
          + "\n  withCount: " + this.toDetailString());
    }

    /**
     * 断言引用指向正确，验证指定引用指向当前WithCount
     * @param ref 待验证的引用
     * @param name 引用名称，用于错误信息输出
     */
    private void assertReferredINode(INodeReference ref, String name) {
      if (ref.getReferredINode() == this) {
        return;
      }
      throw new IllegalStateException("Inconsistent Reference:"
          + "\n  " + name + ": " + ref.toDetailString()
          + "\n  withCount: " + this.toDetailString());
    }

    @Override
    void assertReferences() {
      // 验证所有WithName都指向当前节点
      for(WithName withName : withNameList) {
        assertReferredINode(withName, " withName");
      }

      // 验证父引用关系正确性
      final INodeReference parentRef = getParentReference();
      if (parentRef != null) {
        assertDstReference(parent