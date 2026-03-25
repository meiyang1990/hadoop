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

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.namenode.AclFeature;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.AclStorage;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;
import org.apache.hadoop.hdfs.server.namenode.INodeFileAttributes;
import org.apache.hadoop.hdfs.server.namenode.QuotaCounts;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;

/**
 * 包含快照相关信息的文件INode特性，用于记录文件在不同快照版本间的差异信息
 * 维护了文件的所有快照差异链表，支持快照创建、删除、数据恢复等操作
 */
@InterfaceAudience.Private
public class FileWithSnapshotFeature implements INode.Feature {
  private final FileDiffList diffs;
  private boolean isCurrentFileDeleted = false;
  
  /**
   * 构造文件快照特性对象，初始化差异链表
   * @param diffs 预存的文件差异链表，为空则创建空链表
   */
  public FileWithSnapshotFeature(FileDiffList diffs) {
    this.diffs = diffs != null? diffs: new FileDiffList();
  }

  /**
   * 检查当前文件是否已被删除（仍被快照保留）
   * @return 当前文件已在目录树中删除返回true，否则返回false
   */
  public boolean isCurrentFileDeleted() {
    return isCurrentFileDeleted;
  }
  
  /** 
   * We need to distinguish two scenarios:
   * 1) the file is still in the current file directory, it has been modified 
   *    before while it is included in some snapshot
   * 2) the file is not in the current file directory (deleted), but it is in
   *    some snapshot, thus we still keep this inode
   * For both scenarios the file has snapshot feature. We set 
   * {@link #isCurrentFileDeleted} to true for 2).
   */
  /**
   * 标记当前文件已被目录树删除，仅被快照保留
   */
  public void deleteCurrentFile() {
    isCurrentFileDeleted = true;
  }

  /**
   * 获取所有快照差异链表
   * @return 文件差异列表对象
   */
  public FileDiffList getDiffs() {
    return diffs;
  }
  
  /**
   * 获取所有快照差异中最大副本因子，用于配额计算
   * @param excluded 需要排除的差异项（通常是待删除的差异）
   * @return 最大副本因子
   */
  public short getMaxBlockRepInDiffs(FileDiff excluded) {
    short max = 0;
    for(FileDiff d : getDiffs()) {
      if (d != excluded && d.snapshotINode != null) {
        final short replication = d.snapshotINode.getFileReplication();
        if (replication > max) {
          max = replication;
        }
      }
    }
    return max;
  }

  /**
   * 检查文件在两个指定快照之间是否发生过变更
   * @param file 当前文件INode
   * @param from 起始快照
   * @param to 结束快照
   * @return 发生变更返回true，否则返回false
   */
  boolean changedBetweenSnapshots(INodeFile file, Snapshot from, Snapshot to) {
    // 获取两个快照之间的差异索引范围
    int[] diffIndexPair = diffs.changedBetweenSnapshots(from, to);
    if (diffIndexPair == null) {
      return false;
    }
    int earlierDiffIndex = diffIndexPair[0];
    int laterDiffIndex = diffIndexPair[1];

    final DiffList<FileDiff> diffList = diffs.asList();
    // 获取起始快照的文件长度
    final long earlierLength = diffList.get(earlierDiffIndex).getFileSize();
    // 获取结束快照的文件长度，结束位置为当前则计算当前文件长度
    final long laterLength = laterDiffIndex == diffList.size() ? file
        .computeFileSize(true, false) : diffList.get(laterDiffIndex)
        .getFileSize();
    if (earlierLength != laterLength) { // 文件长度已变更，直接返回true
      return true;
    }

    // 检查元数据是否发生变更
    INodeFileAttributes earlierAttr = null;
    for (int i = earlierDiffIndex; i < laterDiffIndex; i++) {
      FileDiff diff = diffList.get(i);
      if (diff.snapshotINode != null) {
        earlierAttr = diff.snapshotINode;
        break;
      }
    }
    if (earlierAttr == null) { // 无元数据变更，返回false
      return false;
    }
    // 获取结束位置的INode属性
    INodeFileAttributes laterAttr = diffs.getSnapshotINode(
        Math.max(Snapshot.getSnapshotId(from), Snapshot.getSnapshotId(to)),
        file);
    // 比较元数据是否一致
    return !earlierAttr.metadataEquals(laterAttr);
  }

  /**
   * 获取详细调试信息字符串
   * @return 包含删除状态和所有差异的字符串
   */
  public String getDetailedString() {
    return (isCurrentFileDeleted()? "(DELETED), ": ", ") + diffs;
  }
  
  /**
   * 清理文件相关数据，处理删除当前文件或删除快照的场景
   * @param reclaimContext 回收上下文，用于记录配额变更和待回收块
   * @param file 当前文件INode
   * @param snapshotId 待删除快照ID，如果是当前文件删除则为CURRENT_STATE_ID
   * @param priorSnapshotId 前一个快照ID
   * @param storagePolicyId 存储策略ID
   */
  public void cleanFile(INode.ReclaimContext reclaimContext,
      final INodeFile file, final int snapshotId, int priorSnapshotId,
      byte storagePolicyId) {
    // 获取本次实际需要删除的快照ID
    final int snapshotToBeDeleted
        = reclaimContext.getSnapshotIdToBeDeleted(snapshotId, file);
    if (snapshotId == Snapshot.CURRENT_STATE_ID) {
      // 删除当前文件，文件仍被快照保留
      if (!isCurrentFileDeleted()
          && snapshotToBeDeleted == Snapshot.CURRENT_STATE_ID) {
        file.recordModification(priorSnapshotId);
        deleteCurrentFile();
      }
      // 计算清理前的空间配额
      final BlockStoragePolicy policy = reclaimContext.storagePolicySuite()
          .getPolicy(storagePolicyId);
      QuotaCounts old = file.storagespaceConsumed(policy);
      // 收集需要删除的块并清理
      collectBlocksAndClear(reclaimContext, file);
      // 计算清理后的空间配额，更新配额增量
      QuotaCounts current = file.storagespaceConsumed(policy);
      reclaimContext.quotaDelta().add(old.subtract(current));
    } else { // 删除指定快照
      priorSnapshotId = getDiffs().updatePrior(snapshotId, priorSnapshotId);
      diffs.deleteSnapshotDiff(reclaimContext, snapshotId, priorSnapshotId,
          file);
    }
  }
  
  /**
   * 清空所有快照差异
   */
  public void clearDiffs() {
    this.diffs.clear();
  }
  
  /**
   * 删除指定快照差异后，更新配额并收集需要删除的块
   * @param reclaimContext 回收上下文
   * @param file 当前文件INode
   * @param removed 被删除的差异项
   */
  public void updateQuotaAndCollectBlocks(INode.ReclaimContext reclaimContext,
      INodeFile file, FileDiff removed) {
    byte storagePolicyID = file.getStoragePolicyID();
    BlockStoragePolicy bsp = null;
    if (storagePolicyID != HdfsConstants.BLOCK_STORAGE_POLICY_ID_UNSPECIFIED) {
      bsp = reclaimContext.storagePolicySuite().
          getPolicy(file.getStoragePolicyID());
    }

    QuotaCounts oldCounts;
    if (removed.snapshotINode != null) {
      oldCounts = new QuotaCounts.Builder().build();
      // 收集所有当前文件和快照中的唯一块集合
      Set<BlockInfo> allBlocks = new HashSet<BlockInfo>();
      if (file.getBlocks() != null) {
        allBlocks.addAll(Arrays.asList(file.getBlocks()));
      }
      if (removed.getBlocks() != null) {
        allBlocks.addAll(Arrays.asList(removed.getBlocks()));
      }
      // 遍历剩余快照添加块到集合
      for (FileDiff diff : diffs) {
        BlockInfo[] diffBlocks = diff.getBlocks();
        if (diffBlocks != null) {
          allBlocks.addAll(Arrays.asList(diffBlocks));
        }
      }
      // 计算删除前总空间消耗
      for (BlockInfo b: allBlocks) {
        short replication = b.getReplication();
        long blockSize = b.isComplete() ? b.getNumBytes() : file
            .getPreferredBlockSize();

        oldCounts.addStorageSpace(blockSize * replication);

        // 按存储类型累加配额
        if (bsp != null) {
          List<StorageType> oldTypeChosen = bsp.chooseStorageTypes(replication);
          for (StorageType t : oldTypeChosen) {
            if (t.supportTypeQuota()) {
              oldCounts.addTypeSpace(t, blockSize);
            }
          }
        }
      }

      // 移除被删除快照的ACL特性
      AclFeature aclFeature = removed.getSnapshotINode().getAclFeature();
      if (aclFeature != null) {
        AclStorage.removeAclFeature(aclFeature);
      }
    } else {
      oldCounts = file.storagespaceConsumed(null);
    }

    // 合并差异并收集需要删除的块
    getDiffs().combineAndCollectSnapshotBlocks(reclaimContext, file, removed);
    // 更新剩余块的副本因子，基于最大副本重新计算
    if (file.getBlocks() != null) {
      short replInDiff = getMaxBlockRepInDiffs(removed);
      short repl = (short) Math.max(file.getPreferredBlockReplication(),
                                    replInDiff);
      for (BlockInfo b : file.getBlocks()) {
        if (repl != b.getReplication()) {
          reclaimContext.collectedBlocks().addUpdateReplicationFactor(b, repl);
        }
      }
    }
    // 计算删除后空间配额，更新配额增量
    QuotaCounts current = file.storagespaceConsumed(bsp);
    reclaimContext.quotaDelta().add(oldCounts.subtract(current));
  }

  /**
   * 收集不再被任何inode引用的块，更新块列表，清理多余块
   * @param reclaimContext 回收上下文，用于记录待删除块
   * @param file 当前文件INode
   */
  public void collectBlocksAndClear(
      INode.ReclaimContext reclaimContext, final INodeFile file) {
    // 检查当前文件已删除且无剩余快照，直接清空整个文件
    if (isCurrentFileDeleted() && getDiffs().asList().isEmpty()) {
      file.clearFile(reclaimContext);
      return;
    }
    // 计算当前需要保留的最大文件长度
    final long max;
    FileDiff diff = getDiffs().getLast();
    if (isCurrentFileDeleted()) {
      max = diff == null? 0: diff.getFileSize();
    } else {
      max = file.computeFileSize();
    }

    // 收集超过最大长度需要删除的块
    FileDiff last = diffs.getLast();
    BlockInfo[] snapshotBlocks = last == null ? null : last.getBlocks();
    if(snapshotBlocks == null)
      file.collectBlocksBeyondMax(max, reclaimContext.collectedBlocks(), null);
    else
      file.collectBlocksBeyondSnapshot(snapshotBlocks,
                                       reclaimContext.collectedBlocks());
  }

  @Override
  public String toString() {
    return "isCurrentFileDeleted? " + isCurrentFileDeleted + ", " + diffs;
  }
}