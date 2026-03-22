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

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.Arrays;

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.hdfs.protocol.SnapshotException;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockStoragePolicySuite;
import org.apache.hadoop.hdfs.server.namenode.Content;
import org.apache.hadoop.hdfs.server.namenode.ContentCounts;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeDirectory;
import org.apache.hadoop.hdfs.server.namenode.INodeDirectory.SnapshotAndINode;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;
import org.apache.hadoop.hdfs.server.namenode.INodeReference;
import org.apache.hadoop.hdfs.server.namenode.INodeReference.WithCount;
import org.apache.hadoop.hdfs.server.namenode.INodeReference.WithName;
import org.apache.hadoop.hdfs.server.namenode.INodesInPath;
import org.apache.hadoop.hdfs.server.namenode.LeaseManager;
import org.apache.hadoop.hdfs.util.ReadOnlyList;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.util.Lists;
import org.apache.hadoop.util.Time;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.util.Preconditions;

/**
 * 文件描述：HDFS快照可快照目录特性实现，继承自DirectoryWithSnapshotFeature
 * 核心职责：维护可快照目录下所有快照的元数据信息，提供快照的增删改查、差异计算等核心能力
 * 所在模块：HDFS NameNode 快照管理模块
 */
@InterfaceAudience.Private
public class DirectorySnapshottableFeature extends DirectoryWithSnapshotFeature {
  /** 单个可快照目录允许的最大快照数量默认值 */
  static final int SNAPSHOT_QUOTA_DEFAULT = 1 << 16;

  /**
   * 当前目录的快照按名称升序排列存储
   * 注意：按快照ID升序排列的快照存储在父类{@link DirectoryWithSnapshotFeature}的diffs私有字段中
   */
  private final List<Snapshot> snapshotsByNames = new ArrayList<Snapshot>();
  /** 当前目录允许的快照数量配额 */
  private int snapshotQuota = SNAPSHOT_QUOTA_DEFAULT;

  /**
   * 通过已有的DirectoryWithSnapshotFeature构造可快照目录特性
   * @param feature 已有的带快照特性的目录对象
   */
  public DirectorySnapshottableFeature(DirectoryWithSnapshotFeature feature) {
    super(feature == null ? null : feature.getDiffs());
  }

  /**
   * 获取当前目录已存在的快照数量
   * @return 当前目录快照总数
   */
  public int getNumSnapshots() {
    return snapshotsByNames.size();
  }

  /**
   * 二分查找指定名称的快照在有序列表中的索引位置
   * @param snapshotName 待查找的快照名称字节数组
   * @return 快照索引，小于0表示未找到
   */
  private int searchSnapshot(byte[] snapshotName) {
    return Collections.binarySearch(snapshotsByNames, snapshotName);
  }

  /**
   * 根据快照名称获取快照对象
   * @param snapshotName 待查找的快照名称字节数组
   * @return 匹配的快照对象，不存在则返回null
   */
  public Snapshot getSnapshot(byte[] snapshotName) {
    final int i = searchSnapshot(snapshotName);
    return i < 0? null: snapshotsByNames.get(i);
  }

  /**
   * 根据快照ID获取快照对象
   * @param sid 待查找的快照ID
   * @return 匹配的快照对象，不存在则返回null
   */
  public Snapshot getSnapshotById(int sid) {
    for (Snapshot s : snapshotsByNames) {
      if (s.getId() == sid) {
        return s;
      }
    }
    return null;
  }

  /**
   * 获取按名称排序的只读快照列表
   * @return 只读快照列表
   */
  public ReadOnlyList<Snapshot> getSnapshotList() {
    return ReadOnlyList.Util.asReadOnlyList(snapshotsByNames);
  }

  /**
   * 重命名指定快照
   * @param path 快照所在目录路径，用于生成异常信息
   * @param oldName 快照原名称
   * @param newName 快照新名称
   * @param mtime 快照修改时间
   * @throws SnapshotException 原快照不存在或新名称已被占用时抛出异常
   */
  public void renameSnapshot(String path, String oldName, String newName,
      long mtime)
      throws SnapshotException {
    final int indexOfOld = searchSnapshot(DFSUtil.string2Bytes(oldName));
    if (indexOfOld < 0) {
      throw new SnapshotException("The snapshot " + oldName
          + " does not exist for directory " + path);
    } else {
      if (newName.equals(oldName)) {
        return;
      }
      final byte[] newNameBytes = DFSUtil.string2Bytes(newName);
      int indexOfNew = searchSnapshot(newNameBytes);
      if (indexOfNew >= 0) {
        throw new SnapshotException("The snapshot " + newName
            + " already exists for directory " + path);
      }
      // 从按名称排序列表中移除原名称条目
      Snapshot snapshot = snapshotsByNames.remove(indexOfOld);
      final INodeDirectory ssRoot = snapshot.getRoot();
      ssRoot.setLocalName(newNameBytes);
      ssRoot.setModificationTime(mtime, Snapshot.CURRENT_STATE_ID);
      indexOfNew = -indexOfNew - 1;
      if (indexOfNew <= indexOfOld) {
        snapshotsByNames.add(indexOfNew, snapshot);
      } else { // 索引计算修正：移除旧元素后位置偏移1
        snapshotsByNames.add(indexOfNew - 1, snapshot);
      }
    }
  }

  /**
   * 获取当前目录快照配额
   * @return 当前目录允许的最大快照数量
   */
  public int getSnapshotQuota() {
    return snapshotQuota;
  }

  /**
   * 设置当前目录快照配额
   * @param snapshotQuota 新的配额值
   */
  public void setSnapshotQuota(int snapshotQuota) {
    if (snapshotQuota < 0) {
      throw new HadoopIllegalArgumentException(
          "Cannot set snapshot quota to " + snapshotQuota + " < 0");
    }
    this.snapshotQuota = snapshotQuota;
  }

  /**
   * 直接添加快照到名称有序列表，仅用于加载fsimage时
   * @param snapshot 待添加的快照对象
   */
  void addSnapshot(Snapshot snapshot) {
    this.snapshotsByNames.add(snapshot);
  }

  /**
   * 创建并添加新快照到当前目录
   * @param snapshotRoot 快照根目录
   * @param snapshotManager 快照管理器实例
   * @param name 快照名称
   * @param leaseManager 租约管理器，用于处理打开的文件
   * @param now 当前时间，作为快照创建时间
   * @return 创建完成的快照对象
   * @throws SnapshotException 同名快照已存在或超出快照配额时抛出异常
   */
  public Snapshot addSnapshot(INodeDirectory snapshotRoot,
                              SnapshotManager snapshotManager, String name,
                              final LeaseManager leaseManager, long now)
      throws SnapshotException {
    int id = snapshotManager.getSnapshotCounter();
    // 检查快照配额是否足够
    final int n = getNumSnapshots();
    if (n + 1 > snapshotQuota) {
      throw new SnapshotException("Failed to add snapshot: there are already "
          + n + " snapshot(s) and the snapshot quota is "
          + snapshotQuota);
    }
    snapshotManager.checkPerDirectorySnapshotLimit(n);
    final Snapshot s = new Snapshot(id, name, snapshotRoot);
    final byte[] nameBytes = s.getRoot().getLocalNameBytes();
    final int i = searchSnapshot(nameBytes);
    if (i >= 0) {
      throw new SnapshotException("Failed to add snapshot: there is already a "
          + "snapshot with the same name \"" + Snapshot.getSnapshotName(s) + "\".");
    }

    final DirectoryDiff d = getDiffs().addDiff(id, snapshotRoot);
    d.setSnapshotRoot(s.getRoot());
    snapshotsByNames.add(-i - 1, s);

    // 修改时间设为快照创建时间
    snapshotRoot.updateModificationTime(now, Snapshot.CURRENT_STATE_ID);
    s.getRoot().setModificationTime(now, Snapshot.CURRENT_STATE_ID);

    if (snapshotManager.captureOpenFiles()) {
      try {
        // 获取所有持有租约的打开文件
        Set<INodesInPath> openFilesIIP =
            leaseManager.getINodeWithLeases(snapshotRoot);
        for (INodesInPath openFileIIP : openFilesIIP) {
          INodeFile openFile = openFileIIP.getLastINode().asFile();
          // 记录打开文件当前状态到快照
          openFile.recordModification(openFileIIP.getLatestSnapshotId());
        }
      } catch (Exception e) {
        throw new SnapshotException("Failed to add snapshot: Unable to " +
            "capture all open files under the snapshot dir " +
            snapshotRoot.getFullPathName() + " for snapshot '" + name + "'", e);
      }
    }
    return s;
  }

  /**
   * 删除指定名称的快照，清理对应的数据差异和INode
   * @param reclaimContext 记录需要回收的块和INode上下文
   * @param snapshotRoot 快照所在目录
   * @param snapshotName 待删除的快照名称
   * @param now 删除时间，用于更新目录修改时间
   * @param snapshotManager 快照管理器实例
   * @return 被删除的快照对象，未找到对应快照且配置允许忽略错误时返回null
   * @throws SnapshotException 快照不存在且不允许忽略错误时抛出异常
   */
  public Snapshot removeSnapshot(
      INode.ReclaimContext reclaimContext, INodeDirectory snapshotRoot,
      String snapshotName, long now, SnapshotManager snapshotManager)
      throws SnapshotException {
    final int i = searchSnapshot(DFSUtil.string2Bytes(snapshotName));
    if (i < 0) {
      // 处理编辑日志重放场景：有序删除关闭且镜像未加载完成时，忽略重复删除错误，避免NameNode启动失败
      if (!snapshotManager.isSnapshotDeletionOrdered() &&
          !snapshotManager.isImageLoaded()) {
        return null;
      }
      throw new SnapshotException("Cannot delete snapshot " + snapshotName
          + " from path " + snapshotRoot.getFullPathName()
          + ": the snapshot does not exist.");
    } else {
      final Snapshot snapshot = snapshotsByNames.get(i);
      int prior = Snapshot.findLatestSnapshot(snapshotRoot, snapshot.getId());
      snapshotManager.assertPrior(snapshotRoot, snapshotName, prior);

      reclaimContext.setSnapshotToBeDeleted(snapshot);
      // 清理快照子树中的所有不可达INode
      snapshotRoot.cleanSubtree(reclaimContext, snapshot.getId(), prior);
      // 子树清理成功后从名称列表移除快照
      snapshotsByNames.remove(i);
      snapshotRoot.updateModificationTime(now, Snapshot.CURRENT_STATE_ID);
      return snapshot;
    }
  }

  /**
   * 计算快照相关的内容摘要统计，累加当前目录快照计数
   * @param bsps 块存储策略套件
   * @param counts 内容计数器，用于累加统计结果
   * @throws AccessControlException 访问权限不足时抛出异常
   */
  @Override
  public void computeContentSummary4Snapshot(final BlockStoragePolicySuite bsps,
      final ContentCounts counts) throws AccessControlException {
    counts.addContent(Content.SNAPSHOT, snapshotsByNames.size());
    counts.addContent(Content.SNAPSHOTTABLE_DIRECTORY, 1);
    super.computeContentSummary4Snapshot(bsps, counts);
  }

  /**
   * 计算两个快照之间（或快照与当前目录之间）的差异
   * @param snapshotRootDir 快照根目录
   * @param snapshotDiffScopeDir 差异计算的范围目录，必须是快照根目录的后代
   * @param from 对比起点快照名称，null表示当前目录
   * @param to 对比终点快照名称，null表示当前目录
   * @return 差异信息对象
   * @throws SnapshotException 起点或终点快照不存在时抛出异常
   */
  SnapshotDiffInfo computeDiff(final INodeDirectory snapshotRootDir,
      final INodeDirectory snapshotDiffScopeDir, final String from,
      final String to) throws SnapshotException {
    Preconditions.checkArgument(snapshotDiffScopeDir
        .isDescendantOfSnapshotRoot(snapshotRootDir));
    Snapshot fromSnapshot = getSnapshotByName(snapshotRootDir, from);
    Snapshot toSnapshot = getSnapshotByName(snapshotRootDir, to);
    // 起点终点相同，返回空差异
    if (from != null && from.equals(to)) {
      return null;
    }
    SnapshotDiffInfo diffs = new SnapshotDiffInfo(snapshotRootDir,
        snapshotDiffScopeDir, fromSnapshot, toSnapshot);
    // 递归计算所有子节点差异
    computeDiffRecursively(snapshotDiffScopeDir, snapshotDiffScopeDir,
        new ArrayList<>(), diffs);
    return diffs;
  }

  /**
   * 分RPC调用分段计算两个快照之间的差异，支持断点续算
   * @param snapshotRootDir 快照根目录
   * @param snapshotDiffScopeDir 差异计算的范围目录，必须是快照根目录的后代
   * @param from 对比起点快照名称，null表示当前目录
   * @param to 对比终点快照名称，null表示当前目录
   * @param startPath 断点续算的起始路径，相对于快照根目录
   * @param index 上次计算停止时的列表索引，-1表示从头开始
   * @param snapshotDiffReportEntriesLimit 单次RPC返回差异条目数量上限
   * @return 分段差异列表信息对象
   * @throws SnapshotException 起点或终点快照不存在时抛出异常
   */
  SnapshotDiffListingInfo computeDiff(final INodeDirectory snapshotRootDir,
      final INodeDirectory snapshotDiffScopeDir, final String from,
      final String to, byte[] startPath, int index,
      int snapshotDiffReportEntriesLimit) throws SnapshotException {
    Preconditions.checkArgument(
        snapshotDiffScopeDir.isDescendantOfSnapshotRoot(snapshotRootDir));
    Snapshot fromSnapshot = getSnapshotByName(snapshotRootDir, from);
    Snapshot toSnapshot = getSnapshotByName(snapshotRootDir, to);
    boolean toProcess = Arrays.equals(startPath, DFSUtilClient.EMPTY_BYTES);
    // 将字节路径转换为路径分段数组
    byte[][] resumePath = DFSUtilClient.bytes2byteArray(startPath);
    if (from.equals(to)) {
      return null;
    }
    SnapshotDiffListingInfo diffs =
        new SnapshotDiffListingInfo(snapshotRootDir, snapshotDiffScopeDir,
            fromSnapshot, toSnapshot, snapshotDiffReportEntriesLimit);
    diffs.setLastIndex(index);
    // 从断点位置递归计算差异
    computeDiffRecursively(snapshotDiffScopeDir, snapshotDiffScopeDir,
        new ArrayList<byte[]>(), diffs, resumePath, 0, toProcess);
    return diffs;
  }

  /**
   * 根据快照名称字符串查找快照对象
   * @param snapshotRoot 快照所在目录
   * @param snapshotName 待查找的快照名称
   * @return 匹配的快照对象，名称为null或空时返回null
   * @throws SnapshotException 名称非空但未找到匹配快照时抛出异常
   */
  public Snapshot getSnapshotByName(INodeDirectory snapshotRoot,
      String snapshotName) throws SnapshotException {
    Snapshot s = null;
    if (snapshotName != null && !snapshotName.isEmpty()) {
      final int index = searchSnapshot(DFSUtil.string2Bytes(snapshotName));
      if (index < 0) {
        throw new SnapshotException("Cannot find the snapshot of directory "
            + snapshotRoot.getFullPathName() + " with name " + snapshotName);
      }
      s = snapshotsByNames.get(index);
    }
    return s;
  }

  /**