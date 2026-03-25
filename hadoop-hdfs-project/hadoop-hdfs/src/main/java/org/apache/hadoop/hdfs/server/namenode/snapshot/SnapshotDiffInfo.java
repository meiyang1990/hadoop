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

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport.DiffReportEntry;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport.DiffType;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeDirectory;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;
import org.apache.hadoop.hdfs.server.namenode.INodeReference;
import org.apache.hadoop.hdfs.server.namenode.snapshot.DirectoryWithSnapshotFeature.ChildrenDiff;

import org.apache.hadoop.util.Preconditions;
import org.apache.hadoop.thirdparty.com.google.common.primitives.SignedBytes;
import org.apache.hadoop.util.ChunkedArrayList;

/**
 * 文件路径: hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/snapshot/SnapshotDiffInfo.java
 * 本文件用于存储可快照目录两个快照之间的差异信息，支持生成快照差异报告
 * 描述可快照目录两个快照之间的差异信息
 */
class SnapshotDiffInfo {
  /** 基于INode的完整路径名称比较两个INode的比较器，用于差异排序 */
  public static final Comparator<INode> INODE_COMPARATOR =
      new Comparator<INode>() {
    @Override
    public int compare(INode left, INode right) {
      if (left == null) {
        return right == null ? 0 : -1;
      } else {
        if (right == null) {
          return 1;
        } else {
          // 先比较父节点，父节点相同再比较本地名称字节字典序
          int cmp = compare(left.getParent(), right.getParent());
          return cmp == 0 ? SignedBytes.lexicographicalComparator().compare(
              left.getLocalNameBytes(), right.getLocalNameBytes()) : cmp;
        }
      }
    }
  };

  /**
   * 存储重命名操作的源路径和目标路径信息，用于识别快照差异中的重命名操作
   */
  static class RenameEntry {
    private byte[][] sourcePath;
    private byte[][] targetPath;

    /**
     * 设置重命名操作的源INode和父路径，构建完整源路径
     * @param source 被重命名的源INode
     * @param sourceParentPath 源父目录的相对路径
     */
    void setSource(INode source, byte[][] sourceParentPath) {
      Preconditions.checkState(sourcePath == null);
      sourcePath = new byte[sourceParentPath.length + 1][];
      System.arraycopy(sourceParentPath, 0, sourcePath, 0,
          sourceParentPath.length);
      sourcePath[sourcePath.length - 1] = source.getLocalNameBytes();
    }

    /**
     * 设置重命名操作的目标INode和父路径，构建完整目标路径
     * @param target 重命名后的目标INode
     * @param targetParentPath 目标父目录的相对路径
     */
    void setTarget(INode target, byte[][] targetParentPath) {
      targetPath = new byte[targetParentPath.length + 1][];
      System.arraycopy(targetParentPath, 0, targetPath, 0,
          targetParentPath.length);
      targetPath[targetPath.length - 1] = target.getLocalNameBytes();
    }

    /**
     * 直接设置目标路径
     * @param targetPath 完整目标路径数组
     */
    void setTarget(byte[][] targetPath) {
      this.targetPath = targetPath;
    }

    /**
     * 判断当前条目是否是一个完整的重命名操作
     * @return 源路径和目标路径都不为空则返回true
     */
    boolean isRename() {
      return sourcePath != null && targetPath != null;
    }

    byte[][] getSourcePath() {
      return sourcePath;
    }

    byte[][] getTargetPath() {
      return targetPath;
    }
  }

  /** 快照差异计算的根目录（可快照目录） */
  private final INodeDirectory snapshotRoot;
  /**
   *  快照差异计算的范围目录，差异只计算该目录及其子树
   */
  private final INodeDirectory snapshotDiffScopeDir;
  /** 差异计算的起始快照 */
  private final Snapshot from;
  /** 差异计算的结束快照 */
  private final Snapshot to;
  /**
   * 存储已修改的INode（文件或目录）及其相对于快照根的相对路径
   * 按INode名称排序，用于生成有序的差异报告
   */
  private final SortedMap<INode, byte[][]> diffMap =
      new TreeMap<INode, byte[][]>(INODE_COMPARATOR);
  /**
   * 存储目录子节点变更详情，key是发生子节点变更的目录，value是该目录下的创建/删除变更列表
   */
  private final Map<INodeDirectory, ChildrenDiff> dirDiffMap =
      new HashMap<INodeDirectory, ChildrenDiff>();

  /** 存储重命名操作，key是INode ID，value是重命名条目信息 */
  private final Map<Long, RenameEntry> renameMap =
      new HashMap<Long, RenameEntry>();

  // 已比较的目录总数
  private long totalDirsCompared;

  // 已处理的目录总数
  private long totalDirsProcessed;

  // 已比较的文件总数
  private long totalFilesCompared;

  // 已处理的文件总数
  private long totalFilesProcessed;

  // 子节点列表遍历总耗时（毫秒）
  private long childrenListingTime;

  /**
   * 构造函数，初始化快照差异信息对象
   * @param snapshotRootDir 快照根目录（可快照目录）
   * @param snapshotDiffScopeDir 差异计算范围目录
   * @param start 起始快照
   * @param end 结束快照
   */
  SnapshotDiffInfo(INodeDirectory snapshotRootDir,
      INodeDirectory snapshotDiffScopeDir, Snapshot start, Snapshot end) {
    Preconditions.checkArgument(snapshotRootDir.isSnapshottable() &&
        snapshotDiffScopeDir.isDescendantOfSnapshotRoot(snapshotRootDir));
    this.snapshotRoot = snapshotRootDir;
    this.snapshotDiffScopeDir = snapshotDiffScopeDir;
    this.from = start;
    this.to = end;
    this.totalDirsCompared = 0;
    this.totalDirsProcessed = 0;
    this.totalFilesCompared = 0;
    this.totalFilesProcessed = 0;
  }

  /**
   * 添加一个目录的子节点差异信息，并检测是否为重命名操作
   * @param dir 发生变更的目录
   * @param relativePath 目录相对于快照根的相对路径
   * @param diff 子节点差异对象
   */
  void addDirDiff(INodeDirectory dir, byte[][] relativePath, ChildrenDiff diff) {
    dirDiffMap.put(dir, diff);
    diffMap.put(dir, relativePath);
    // 遍历新建节点检测重命名
    for (INode created : diff.getCreatedUnmodifiable()) {
      if (created.isReference()) {
        RenameEntry entry = getEntry(created.getId());
        if (entry.getTargetPath() == null) {
          entry.setTarget(created, relativePath);
        }
      }
    }
    // 遍历删除节点完善重命名信息
    for (INode deleted : diff.getDeletedUnmodifiable()) {
      if (deleted instanceof INodeReference.WithName) {
        RenameEntry entry = getEntry(deleted.getId());
        entry.setSource(deleted, relativePath);
      }
    }
  }

  Snapshot getFrom() {
    return from;
  }

  Snapshot getTo() {
    return to;
  }


  void incrementDirsCompared() {
    this.totalDirsCompared++;
    incrementDirsProcessed();
  }

  void incrementDirsProcessed() {
    this.totalDirsProcessed++;
  }

  void incrementFilesCompared() {
    this.totalFilesCompared++;
    incrementFilesProcessed();
  }

  void incrementFilesProcessed() {
    this.totalFilesProcessed++;
  }

  /**
   * 累加子节点列表遍历耗时
   * @param millis 本次遍历耗时（毫秒）
   */
  public void addChildrenListingTime(long millis) {
    this.childrenListingTime += millis;
  }

  /**
   * 根据INode ID获取重命名条目，不存在则创建新条目
   * @param inodeId INode ID
   * @return 对应重命名条目
   */
  private RenameEntry getEntry(long inodeId) {
    RenameEntry entry = renameMap.get(inodeId);
    if (entry == null) {
      entry = new RenameEntry();
      renameMap.put(inodeId, entry);
    }
    return entry;
  }

  /**
   * 设置重命名操作的目标路径
   * @param inodeId INode ID
   * @param path 完整目标路径
   */
  void setRenameTarget(long inodeId, byte[][] path) {
    getEntry(inodeId).setTarget(path);
  }

  /**
   * 添加一个被修改文件的差异信息
   * @param file 被修改的文件INode
   * @param relativePath 文件相对于快照根的相对路径
   */
  void addFileDiff(INodeFile file, byte[][] relativePath) {
    diffMap.put(file, relativePath);
  }

  /**
   * 判断起始快照的创建时间是否早于结束快照
   * @return 起始快照更早则返回true
   */
  boolean isFromEarlier() {
    return Snapshot.ID_COMPARATOR.compare(from, to) < 0;
  }

  /**
   * 根据收集的差异信息生成对外的快照差异报告对象
   * @return 包含所有差异条目的快照差异报告
   */
  public SnapshotDiffReport generateReport() {
    List<DiffReportEntry> diffReportList = new ChunkedArrayList<>();
    // 遍历所有差异节点生成报告条目
    for (Map.Entry<INode,byte[][]> drEntry : diffMap.entrySet()) {
      INode node = drEntry.getKey();
      byte[][] path = drEntry.getValue();
      // 添加修改类型条目
      diffReportList.add(new DiffReportEntry(DiffType.MODIFY, path, null));
      if (node.isDirectory()) {
        // 生成目录下子节点的创建/删除/重命名条目
        List<DiffReportEntry> subList = generateReport(dirDiffMap.get(node),
            path, isFromEarlier(), renameMap);
        diffReportList.addAll(subList);
      }
    }

    // 封装统计信息
    SnapshotDiffReport.DiffStats dStats = new SnapshotDiffReport.DiffStats(
        this.totalDirsCompared, this.totalDirsProcessed,
        this.totalFilesCompared, this.totalFilesProcessed,
        this.childrenListingTime);

    // 构建并返回最终差异报告
    return new SnapshotDiffReport(snapshotRoot.getFullPathName(),
        Snapshot.getSnapshotName(from), Snapshot.getSnapshotName(to),
        dStats, diffReportList);
  }

  /**
   * 解析目录子节点差异，生成差异报告条目列表
   * @param dirDiff 目录子节点差异对象
   * @param parentPath 当前目录相对于快照根的相对路径
   * @param fromEarlier 起始快照是否早于结束快照，决定差异方向
   * @param renameMap 重命名操作信息映射表
   * @return 当前目录下所有子节点差异对应的报告条目列表
   */
  private List<DiffReportEntry> generateReport(ChildrenDiff dirDiff,
      byte[][] parentPath, boolean fromEarlier, Map<Long, RenameEntry> renameMap) {
    List<DiffReportEntry> list = new ChunkedArrayList<>();
    // 构建子节点完整路径数组
    byte[][] fullPath = new byte[parentPath.length + 1][];
    System.arraycopy(parentPath, 0, fullPath, 0, parentPath.length);
    // 处理新建节点
    for (INode cnode : dirDiff.getCreatedUnmodifiable()) {
      RenameEntry entry = renameMap.get(cnode.getId());
      // 不是完整重命名则作为创建/删除条目处理
      if (entry == null || !entry.isRename()) {
        fullPath[fullPath.length - 1] = cnode.getLocalNameBytes();
        list.add(new DiffReportEntry(fromEarlier ? DiffType.CREATE
            : DiffType.DELETE, fullPath));
      }
    }
    // 处理删除节点
    for (INode dnode : dirDiff.getDeletedUnmodifiable()) {
      RenameEntry entry = renameMap.get(dnode.getId());
      // 是完整重命名则添加重命名类型条目
      if (entry != null && entry.isRename()) {
        list.add(new DiffReportEntry(DiffType.RENAME,
            fromEarlier ? entry.getSourcePath() : entry.getTargetPath(),
            fromEarlier ? entry.getTargetPath() : entry.getSourcePath()));
      } else {
        // 不是重命名则作为删除/创建条目处理
        fullPath[fullPath.length - 1] = dnode.getLocalNameBytes();
        list.add(new DiffReportEntry(fromEarlier ? DiffType.DELETE
            : DiffType.CREATE, fullPath));
      }
    }
    return list;
  }
}