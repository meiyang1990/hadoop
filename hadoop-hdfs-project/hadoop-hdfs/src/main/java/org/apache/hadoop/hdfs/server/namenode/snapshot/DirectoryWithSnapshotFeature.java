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
package org.apache.hadoop.hdfs.server.namenode.snapshot;

import org.apache.hadoop.util.Preconditions;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockStoragePolicySuite;
import org.apache.hadoop.hdfs.server.namenode.*;
import org.apache.hadoop.hdfs.server.namenode.snapshot.SnapshotFSImageFormat.ReferenceMap;
import org.apache.hadoop.hdfs.util.Diff;
import org.apache.hadoop.hdfs.util.Diff.Container;
import org.apache.hadoop.hdfs.util.Diff.UndoInfo;
import org.apache.hadoop.hdfs.util.ReadOnlyList;
import org.apache.hadoop.security.AccessControlException;

import java.io.DataOutput;
import java.io.IOException;
import java.util.*;

import static org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot.NO_SNAPSHOT_ID;

/**
 * 文件：org.apache.hadoop.hdfs.server.namenode.snapshot.DirectoryWithSnapshotFeature.java
 * 所属模块：HDFS NameNode 快照功能
 * 核心职责：为支持快照功能的目录提供差异数据存储和处理能力，维护目录每次快照后产生的修改差异列表，实现快照视图的快速访问
 */
/**
 * Feature used to store and process the snapshot diff information for a
 * directory. In particular, it contains a directory diff list recording changes
 * made to the directory and its children for each snapshot.
 */
@InterfaceAudience.Private
public class DirectoryWithSnapshotFeature implements INode.Feature {
  /**
   * 存储目录当前状态与历史快照之间子节点列表的差异
   * 记录了快照后新增和删除的子节点信息
   */
  static class ChildrenDiff extends Diff<byte[], INode> {
    ChildrenDiff() {}

    private ChildrenDiff(final List<INode> created, final List<INode> deleted) {
      super(created, deleted);
    }

    /**
     * 在新增列表中替换旧的子节点引用为新节点
     * @return true 替换成功；false 未找到旧节点，替换失败
     */
    private boolean replaceCreated(final INode oldChild, final INode newChild) {
      final List<INode> list = getCreatedUnmodifiable();
      final int i = search(list, oldChild.getLocalNameBytes());
      if (i < 0 || list.get(i).getId() != oldChild.getId()) {
        return false;
      }

      final INode removed = setCreated(i, newChild);
      Preconditions.checkState(removed == oldChild);
      return true;
    }

    /** 销毁新增列表中的所有节点并清空列表 */
    private void destroyCreatedList(INode.ReclaimContext reclaimContext,
        final INodeDirectory currentINode) {
      for (INode c : getCreatedUnmodifiable()) {
        c.destroyAndCollectBlocks(reclaimContext);
        // c should be contained in the children list, remove it
        currentINode.removeChild(c);
      }
      clearCreated();
    }

    /** 销毁删除列表中的所有节点并清空列表 */
    private void destroyDeletedList(INode.ReclaimContext reclaimContext) {
      for (INode d : getDeletedUnmodifiable()) {
        d.destroyAndCollectBlocks(reclaimContext);
      }
      clearDeleted();
    }

    /** 序列化新增列表到输出流 */
    private void writeCreated(DataOutput out) throws IOException {
      final List<INode> created = getCreatedUnmodifiable();
      out.writeInt(created.size());
      for (INode node : created) {
        // For INode in created list, we only need to record its local name
        byte[] name = node.getLocalNameBytes();
        out.writeShort(name.length);
        out.write(name);
      }
    }

    /** 序列化删除列表到输出流 */
    private void writeDeleted(DataOutput out,
        ReferenceMap referenceMap) throws IOException {
      final List<INode> deleted = getDeletedUnmodifiable();
      out.writeInt(deleted.size());
      for (INode node : deleted) {
        FSImageSerialization.saveINode2Image(node, out, true, referenceMap);
      }
    }

    /** 序列化整个ChildrenDiff到输出流 */
    private void write(DataOutput out, ReferenceMap referenceMap
        ) throws IOException {
      writeCreated(out);
      writeDeleted(out, referenceMap);
    }

    /** 将删除列表中所有目录节点添加到结果列表中 */
    private void getDirsInDeleted(List<INodeDirectory> dirList) {
      for (INode node : getDeletedUnmodifiable()) {
        if (node.isDirectory()) {
          dirList.add(node.asDirectory());
        }
      }
    }
  }

  /**
   * 存储单个快照与后一个版本目录状态之间的差异，包含目录属性差异和子节点列表差异
   */
  public static class DirectoryDiff extends
      AbstractINodeDiff<INodeDirectory, INodeDirectoryAttributes, DirectoryDiff> {
    /** 快照创建时刻目录子节点列表的大小 */
    private final int childrenSize;
    /** 子节点列表的差异对象 */
    private final ChildrenDiff diff;
    private boolean isSnapshotRoot = false;
    
    private DirectoryDiff(int snapshotId, INodeDirectory dir) {
      this(snapshotId, dir, new ChildrenDiff());
    }

    public DirectoryDiff(int snapshotId, INodeDirectory dir,
        ChildrenDiff diff) {
      super(snapshotId, null, null);
      this.childrenSize = dir.getChildrenList(Snapshot.CURRENT_STATE_ID).size();
      this.diff = diff;
    }
    /** Constructor used by FSImage loading */
    DirectoryDiff(int snapshotId, INodeDirectoryAttributes snapshotINode,
        DirectoryDiff posteriorDiff, int childrenSize, List<INode> createdList,
        List<INode> deletedList, boolean isSnapshotRoot) {
      super(snapshotId, snapshotINode, posteriorDiff);
      this.childrenSize = childrenSize;
      this.diff = new ChildrenDiff(createdList, deletedList);
      this.isSnapshotRoot = isSnapshotRoot;
    }

    public ChildrenDiff getChildrenDiff() {
      return diff;
    }
    
    void setSnapshotRoot(INodeDirectoryAttributes root) {
      this.snapshotINode = root;
      this.isSnapshotRoot = true;
    }
    
    public boolean isSnapshotRoot() {
      return isSnapshotRoot;
    }

    @Override
    void combinePosteriorAndCollectBlocks(
        final INode.ReclaimContext reclaimContext,
        final INodeDirectory currentDir,
        final DirectoryDiff posterior) {
      // DeletionOrdered: must not combine posterior
      assert !SnapshotManager.isDeletionOrdered();
      diff.combinePosterior(posterior.diff, new Diff.Processor<INode>() {
        /** Collect blocks for deleted files. */
        @Override
        public void process(INode inode) {
          if (inode != null) {
            inode.destroyAndCollectBlocks(reclaimContext);
          }
        }
      });
    }

    /**
     * 获取目录在该快照下的完整子节点列表
     * 基于当前子节点列表应用差异还原出快照视图，快照视图只读
     * @return 快照目录对应的只读子节点列表
     */
    private ReadOnlyList<INode> getChildrenList(final INodeDirectory currentDir) {
      return new ReadOnlyList<INode>() {
        private List<INode> children = null;

        private List<INode> initChildren() {
          if (children == null) {
            final ChildrenDiff combined = new ChildrenDiff();
            DirectoryDiffList directoryDiffList =
                currentDir.getDirectoryWithSnapshotFeature().diffs;
            final int diffIndex =
                directoryDiffList.getDiffIndexById(getSnapshotId());
            List<DirectoryDiff> diffList = directoryDiffList
                .getDiffListBetweenSnapshots(diffIndex,
                    directoryDiffList.asList().size(), currentDir);
            // 合并当前快照之后所有差异得到相对于当前状态的总差异
            for (DirectoryDiff d : diffList) {
              combined.combinePosterior(d.diff, null);
            }
            // 将总差异应用到当前子节点列表还原出快照时刻的子节点列表
            children = combined.apply2Current(ReadOnlyList.Util
                .asList(currentDir.getChildrenList(Snapshot.CURRENT_STATE_ID)));
          }
          return children;
        }

        @Override
        public Iterator<INode> iterator() {
          return initChildren().iterator();
        }

        @Override
        public boolean isEmpty() {
          return childrenSize == 0;
        }

        @Override
        public int size() {
          return childrenSize;
        }

        @Override
        public INode get(int i) {
          return initChildren().get(i);
        }
      };
    }

    /** 
     * 根据名称查找子节点
     * @param name 子节点名称
     * @param checkPosterior 是否继续向后继diff查找
     * @param currentDir 当前目录对象
     * @return 找到的子节点，不存在则返回null
     */
    INode getChild(byte[] name, boolean checkPosterior,
        INodeDirectory currentDir) {
      for(DirectoryDiff d = this; ; d = d.getPosterior()) {
        final Container<INode> returned = d.diff.accessPrevious(name);
        if (returned != null) {
          // 当前diff能够确定节点状态，直接返回结果
          return returned.getElement();
        } else if (!checkPosterior) {
          // 不继续查找，返回未找到
          return null;
        } else if (d.getPosterior() == null) {
          // 没有更多后继diff，从当前目录查找
          return currentDir.getChild(name, Snapshot.CURRENT_STATE_ID);
        }
      }
    }

    @Override
    public String toString() {
      return super.toString() + " childrenSize=" + childrenSize + ", " + diff;
    }

    int getChildrenSize() {
      return childrenSize;
    }

    @Override
    void write(DataOutput out, ReferenceMap referenceMap) throws IOException {
      writeSnapshot(out);
      out.writeInt(childrenSize);

      // 写入是否为快照根目录标记
      out.writeBoolean(isSnapshotRoot);
      if (!isSnapshotRoot) {
        if (snapshotINode != null) {
          out.writeBoolean(true);
          FSImageSerialization.writeINodeDirectoryAttributes(snapshotINode, out);
        } else {
          out.writeBoolean(false);
        }
      }
      // 写入子节点差异，后继diff不需要写入，因为diff本身已经按顺序组织成列表
      diff.write(out, referenceMap);
    }

    @Override
    void destroyDiffAndCollectBlocks(
        INode.ReclaimContext reclaimContext, INodeDirectory currentINode) {
      // 当前diff已被删除，清理删除列表中的节点
      diff.destroyDeletedList(reclaimContext);
      INodeDirectoryAttributes snapshotINode = getSnapshotINode();
      if (snapshotINode != null && snapshotINode.getAclFeature() != null) {
        AclStorage.removeAclFeature(snapshotINode.getAclFeature());
      }
    }
  }

  /**
   * 按快照ID排序（时间顺序）存储目录所有快照差异的列表
   */
  public static class DirectoryDiffList
      extends AbstractINodeDiffList<INodeDirectory, INodeDirectoryAttributes, DirectoryDiff> {

    @Override
    /** 创建新的目录差异对象 */
    DirectoryDiff createDiff(int snapshot, INodeDirectory currentDir) {
      return new DirectoryDiff(snapshot, currentDir);
    }

    @Override
    /** 创建当前目录属性的快照副本 */
    INodeDirectoryAttributes createSnapshotCopy(INodeDirectory currentDir) {
      return currentDir.isQuotaSet()?
          new INodeDirectoryAttributes.CopyWithQuota(currentDir)
        : new INodeDirectoryAttributes.SnapshotCopy(currentDir);
    }

    @Override
    /** 创建新的空差异列表 */
    DiffList<DirectoryDiff> newDiffs() {
      return DirectoryDiffListFactory
          .createDiffList(INodeDirectory.DEFAULT_FILES_PER_DIRECTORY);
    }

    /** 
     * 在所有最新diff的新增列表中替换旧子节点为新节点
     * @return true 找到并替换成功；false 未找到
     */
    public boolean replaceCreatedChild(final INode oldChild,
        final INode newChild) {
      final DiffList<DirectoryDiff> diffList = asList();
      for(int i = diffList.size() - 1; i >= 0; i--) {
        final ChildrenDiff diff = diffList.get(i).diff;
        if (diff.replaceCreated(oldChild, newChild)) {
          return true;
        }
      }
      return false;
    }

    /** 
     * 从所有diff的删除列表中移除指定子节点
     * @return true 找到并移除成功；false 未找到
     */
    public boolean removeDeletedChild(final INode child) {
      final DiffList<DirectoryDiff> diffList = asList();
      for(int i = diffList.size() - 1; i >= 0; i--) {
        final ChildrenDiff diff = diffList.get(i).diff;
        if (diff.removeDeleted(child)) {
          return true;
        }
      }
      return false;
    }

    /**
     * 从后往前查找哪个快照的删除列表包含指定子节点
     * @return 包含该节点的快照ID，如果未找到返回NO_SNAPSHOT_ID
     */
    public int findSnapshotDeleted(final INode child) {
      final DiffList<DirectoryDiff> diffList = asList();
      for(int i = diffList.size() - 1; i >= 0; i--) {
        final DirectoryDiff diff = diffList.get(i);
        if (diff.getChildrenDiff().containsDeleted(child)) {
          return diff.getSnapshotId();
        }
      }
      return NO_SNAPSHOT_ID;
    }

    /**
     * 获取两个快照索引之间的所有差异列表
     * @param fromIndex 较早快照对应的diff索引
     * @param toIndex 较晚快照对应的diff索引
     * @param dir 所属目录对象
     * @return 两个快照之间的差异列表
     */
    List<DirectoryDiff> getDiffListBetweenSnapshots(int fromIndex, int toIndex,
        INodeDirectory dir) {
      return asList().getMinListForRange(fromIndex, toIndex, dir);
    }
  }
  
  private static Map<INode, INode> cloneDiffList(List<INode> diffList) {
    if (diffList == null || diffList.size() == 0) {
      return null;
    }
    Map<INode, INode> map = new HashMap<>(diffList.size());
    for (INode node : diffList) {
      map.put(node, node);
    }
    return map;
  }
  
  /**
   * 销毁DstReference节点下的子树，回收不再被任何快照引用的节点资源
   * @param reclaimContext 回收上下文，收集需要回收的块和节点
   * @param inode 根节点
   * @param snapshot 当前要删除的快照ID
   * @param prior 前一个快照ID
   */
  public static void destroyDstSubtree(INode.ReclaimContext reclaimContext,
      INode inode, final int snapshot, final int prior) {
    Preconditions.checkArgument(prior != NO_SNAPSHOT_ID);
    if (inode.isReference()) {
      if (inode instanceof INodeReference.WithName
          && snapshot != Snapshot.CURRENT_STATE_ID) {
        // 此节点在DstReference子树删除前已经被重命名，直接清理子树
        inode.cleanSubtree(reclaimContext, snapshot, prior);
      } else {
        // DstReference节点，继续处理其引用的实际节点
        destroyDstSubtree(reclaimContext,
            inode.asReference().getReferredINode(), snapshot, prior);
      }
    } else if (inode.isFile()) {
      // 文件节点直接清理子树
      inode.cleanSubtree(reclaimContext, snapshot, prior);
    } else if (inode.isDirectory()) {
      Map<INode, INode> excludedNodes = null;
      INodeDirectory dir = inode.asDirectory();
      DirectoryWithSnapshotFeature sf = dir.getDirectoryWithSnapshotFeature();
      if (sf != null) {
        DirectoryDiffList diffList = sf.getDiffs();
        DirectoryDiff priorDiff = diffList.getDiffById(prior);
        if (priorDiff != null && priorDiff.getSnapshotId() == prior) {
          List<INode> dList = priorDiff.diff.getDeletedUnmodifiable();
          excludedNodes = cloneDiffList(dList);
        }
        
        if (snapshot != Snapshot.CURRENT_STATE_ID) {
          // 删除当前快照对应的diff
          diffList.deleteSnapshotDiff(re