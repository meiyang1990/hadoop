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

import java.util.List;
import java.util.ListIterator;

import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReportListing;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReportListing.DiffReportListingEntry;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeDirectory;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;
import org.apache.hadoop.hdfs.server.namenode.INodeReference;
import org.apache.hadoop.hdfs.server.namenode.snapshot.DirectoryWithSnapshotFeature.ChildrenDiff;

import org.apache.hadoop.util.Preconditions;
import org.apache.hadoop.util.ChunkedArrayList;

/**
 * 文件路径: hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/snapshot/SnapshotDiffListingInfo.java
 * <p>
 * 可快照目录两个快照之间差异的分页信息管理器，用于支持按dif参数限制每次RPC返回的差异条目数，实现分页获取快照差异
 */
class SnapshotDiffListingInfo {
  private final int maxEntries;

  /** 快照所在的根可快照目录 */
  private final INodeDirectory snapshotRoot;
  /**
   *  计算快照差异的范围目录，差异仅统计该目录下的变更
   */
  private final INodeDirectory snapshotDiffScopeDir;
  /** 差异比较的起始快照 */
  private final Snapshot from;
  /** 差异比较的结束快照 */
  private final Snapshot to;

  /** 当前分页最后一条条目的路径，用于下次RPC请求继续遍历 */
  private byte[] lastPath = DFSUtilClient.EMPTY_BYTES;

  /** 当前分页最后一条条目的索引，用于下次RPC请求继续遍历 */
  private int lastIndex = -1;

  /*
   * 单次RPC调用返回的所有修改类型条目列表
   */
  private final List<DiffReportListingEntry> modifiedList =
      new ChunkedArrayList<>();

  private final List<DiffReportListingEntry> createdList =
      new ChunkedArrayList<>();

  private final List<DiffReportListingEntry> deletedList =
      new ChunkedArrayList<>();

  /**
   * 构造快照差异分页信息管理器
   * @param snapshotRootDir 快照根可快照目录
   * @param snapshotDiffScopeDir 差异计算范围目录
   * @param start 起始快照
   * @param end 结束快照
   * @param snapshotDiffReportLimit 单次返回最大条目数限制
   */
  SnapshotDiffListingInfo(INodeDirectory snapshotRootDir,
      INodeDirectory snapshotDiffScopeDir, Snapshot start, Snapshot end,
      int snapshotDiffReportLimit) {
    Preconditions.checkArgument(
        snapshotRootDir.isSnapshottable() && snapshotDiffScopeDir
            .isDescendantOfSnapshotRoot(snapshotRootDir));
    this.snapshotRoot = snapshotRootDir;
    this.snapshotDiffScopeDir = snapshotDiffScopeDir;
    this.from = start;
    this.to = end;
    this.maxEntries = snapshotDiffReportLimit;
  }

  /**
   * 添加目录下的子节点差异到当前分页，达到最大条目数限制时停止添加并返回false
   * @param dirId 当前处理目录ID
   * @param parent 当前目录的相对路径
   * @param diff 当前目录的子节点差异对象
   * @return 是否添加完该目录所有差异，false表示达到最大限制需要分页
   */
  boolean addDirDiff(long dirId, byte[][] parent, ChildrenDiff diff) {
    final Snapshot laterSnapshot = getLater();
    if (lastIndex == -1) {
      if (getTotalEntries() < maxEntries) {
        modifiedList.add(new DiffReportListingEntry(
            dirId, dirId, parent, true, null));
      } else {
        setLastPath(parent);
        setLastIndex(-1);
        return false;
      }
    }

    // 处理新增节点列表
    final List<INode> clist =  diff.getCreatedUnmodifiable();
    if (lastIndex == -1 || lastIndex < clist.size()) {
      ListIterator<INode> iterator = lastIndex != -1 ?
          clist.listIterator(lastIndex): clist.listIterator();
      while (iterator.hasNext()) {
        if (getTotalEntries() < maxEntries) {
          INode created = iterator.next();
          byte[][] path = newPath(parent, created.getLocalNameBytes());
          createdList.add(new DiffReportListingEntry(dirId, created.getId(),
              path, created.isReference(), null));
        } else {
          setLastPath(parent);
          setLastIndex(iterator.nextIndex());
          return false;
        }
      }
      setLastIndex(-1);
    }

    // 处理删除节点列表，包含重命名场景标记
    if (lastIndex == -1 || lastIndex >= clist.size()) {
      final List<INode> dlist =  diff.getDeletedUnmodifiable();
      int size = clist.size();
      ListIterator<INode> iterator = lastIndex != -1 ?
          dlist.listIterator(lastIndex - size): dlist.listIterator();
      while (iterator.hasNext()) {
        if (getTotalEntries() < maxEntries) {
          final INode d = iterator.next();
          byte[][] path = newPath(parent, d.getLocalNameBytes());
          byte[][] target = findRenameTargetPath(d, laterSnapshot);
          final DiffReportListingEntry e = target != null ?
              new DiffReportListingEntry(dirId, d.getId(), path, true, target) :
              new DiffReportListingEntry(dirId, d.getId(), path, false, null);
          deletedList.add(e);
        } else {
          setLastPath(parent);
          // 偏移量计算为新增列表大小 + 删除列表当前索引，下次RPC从当前位置继续遍历
          setLastIndex(size + iterator.nextIndex());
          return false;
        }
      }
      setLastIndex(-1);
    }
    return true;
  }

  /**
   * 查找被重命名节点在新快照中的目标路径，用于标记重命名变更
   * @param deleted 已删除的节点（原位置的引用节点）
   * @param laterSnapshot 较晚版本的快照
   * @return 重命名后的目标路径，不存在则返回null
   */
  private byte[][] findRenameTargetPath(INode deleted, Snapshot laterSnapshot) {
    if (deleted instanceof INodeReference.WithName) {
      return snapshotRoot.getDirectorySnapshottableFeature()
          .findRenameTargetPath(snapshotDiffScopeDir,
              (INodeReference.WithName) deleted,
              Snapshot.getSnapshotId(laterSnapshot));
    }
    return null;
  }

  /**
   * 基于父路径和当前节点名生成完整相对路径
   * @param parent 父路径数组
   * @param name 当前节点名称字节数组
   * @return 拼接后的完整路径数组
   */
  private static byte[][] newPath(byte[][] parent, byte[] name) {
    byte[][] fullPath = new byte[parent.length + 1][];
    System.arraycopy(parent, 0, fullPath, 0, parent.length);
    fullPath[fullPath.length - 1] = name;
    return fullPath;
  }

  /**
   * 获取时间更早的快照
   * @return 较早版本的快照
   */
  Snapshot getEarlier() {
    return isFromEarlier()? from: to;
  }

  /**
   * 获取时间更晚的快照
   * @return 较晚版本的快照
   */
  Snapshot getLater() {
    return isFromEarlier()? to: from;
  }


  /**
   * 设置当前分页最后一条条目的路径
   * @param lastPath 最后一条条目的路径数组
   */
  public void setLastPath(byte[][] lastPath) {
    this.lastPath = DFSUtilClient.byteArray2bytes(lastPath);
  }

  /**
   * 设置当前分页最后一条条目的索引
   * @param idx 最后一条条目的索引
   */
  public void setLastIndex(int idx) {
    this.lastIndex = idx;
  }

  /**
   * 添加文件修改差异到当前分页，达到最大条目数限制时停止添加并返回false
   * @param file 被修改的文件节点
   * @param relativePath 文件的相对路径
   * @return 是否成功添加，false表示达到最大限制需要分页
   */
  boolean addFileDiff(INodeFile file, byte[][] relativePath) {
    if (getTotalEntries() < maxEntries) {
      modifiedList.add(new DiffReportListingEntry(file.getId(),
          file.getId(), relativePath,false, null));
    } else {
      setLastPath(relativePath);
      return false;
    }
    return true;
  }
  /** @return 起始快照from是否比结束快照to时间更早 */
  boolean isFromEarlier() {
    return Snapshot.ID_COMPARATOR.compare(from, to) < 0;
  }


  /**
   * 获取当前分页已添加的总条目数
   * @return 新增、修改、删除三类条目的总数
   */
  private int getTotalEntries() {
    return createdList.size() + modifiedList.size() + deletedList.size();
  }

  /**
   * 基于收集到的差异信息生成最终快照差异分页报告
   *
   * @return 快照差异分页报告对象，包含当前页所有差异条目和分页续读信息
   */
  public SnapshotDiffReportListing generateReport() {
    return new SnapshotDiffReportListing(lastPath, modifiedList, createdList,
        deletedList, lastIndex, isFromEarlier());
  }
}