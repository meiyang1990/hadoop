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

import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguous;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INode.BlocksMapUpdateInfo;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;
import org.apache.hadoop.hdfs.server.namenode.INodeFileAttributes;

/**
 * 文件快照差异列表，管理INodeFile的多个快照差异记录。
 * 用于存储和维护文件在不同快照之间的变更信息，支持快照块查找、合并、清理等操作。
 */
public class FileDiffList extends
    AbstractINodeDiffList<INodeFile, INodeFileAttributes, FileDiff> {
  
  /**
   * 创建一个新的文件差异对象
   * @param snapshotId 快照ID
   * @param file 当前文件INode
   * @return 新创建的FileDiff实例
   */
  @Override
  FileDiff createDiff(int snapshotId, INodeFile file) {
    return new FileDiff(snapshotId, file);
  }
  
  /**
   * 创建当前文件INode的快照拷贝
   * @param currentINode 当前文件INode
   * @return 当前文件属性的快照拷贝
   */
  @Override
  INodeFileAttributes createSnapshotCopy(INodeFile currentINode) {
    return new INodeFileAttributes.SnapshotCopy(currentINode);
  }

  /**
   * 销毁所有快照差异，并收集所有快照中的块信息用于后续处理
   * @param collectedBlocks 用于收集待删除块的容器
   */
  public void destroyAndCollectSnapshotBlocks(
      BlocksMapUpdateInfo collectedBlocks) {
    for (FileDiff d : asList()) {
      d.destroyAndCollectSnapshotBlocks(collectedBlocks);
    }
  }

  /**
   * 将当前文件状态保存为一个新的快照差异
   * @param latestSnapshotId 最新快照ID
   * @param iNodeFile 当前文件INode
   * @param snapshotCopy 文件属性快照拷贝
   * @param withBlocks 是否需要保存块信息（首次修改时需要保存）
   */
  public void saveSelf2Snapshot(int latestSnapshotId, INodeFile iNodeFile,
      INodeFileAttributes snapshotCopy, boolean withBlocks) {
    final FileDiff diff =
        super.saveSelf2Snapshot(latestSnapshotId, iNodeFile, snapshotCopy);
    if (withBlocks) {  // Store blocks if this is the first update
      BlockInfo[] blks = iNodeFile.getBlocks();
      assert blks != null;
      diff.setBlocks(blks);
    }
  }

  /**
   * 查找早于指定快照的最近一个保存了块信息的快照块列表
   * @param snapshotId 指定快照ID
   * @return 找到的块数组，不存在则返回null
   */
  public BlockInfo[] findEarlierSnapshotBlocks(int snapshotId) {
    assert snapshotId != Snapshot.NO_SNAPSHOT_ID : "Wrong snapshot id";
    if (snapshotId == Snapshot.CURRENT_STATE_ID) {
      return null;
    }
    DiffList<FileDiff> diffs = this.asList();
    int i = diffs.binarySearch(snapshotId);
    BlockInfo[] blocks = null;
    for(i = i >= 0 ? i : -i-2; i >= 0; i--) {
      blocks = diffs.get(i).getBlocks();
      if(blocks != null) {
        break;
      }
    }
    return blocks;
  }

  /**
   * 查找晚于指定快照的最近一个保存了块信息的快照块列表
   * @param snapshotId 指定快照ID
   * @return 找到的块数组，不存在则返回null
   */
  public BlockInfo[] findLaterSnapshotBlocks(int snapshotId) {
    assert snapshotId != Snapshot.NO_SNAPSHOT_ID : "Wrong snapshot id";
    if (snapshotId == Snapshot.CURRENT_STATE_ID) {
      return null;
    }
    DiffList<FileDiff> diffs = this.asList();
    int i = diffs.binarySearch(snapshotId);
    BlockInfo[] blocks = null;
    for (i = i >= 0 ? i+1 : -i-1; i < diffs.size(); i++) {
      blocks = diffs.get(i).getBlocks();
      if (blocks != null) {
        break;
      }
    }
    return blocks;
  }

  /**
   * 合并被删除快照的块信息到相邻快照，并收集未被使用的块用于回收。
   * 当删除快照时，将被删除快照的块信息迁移到更早的快照，同时清理不再被任何快照引用的块。
   * @param reclaimContext 块回收上下文
   * @param file 文件INode
   * @param removed 被删除的快照差异
   */
  void combineAndCollectSnapshotBlocks(
      INode.ReclaimContext reclaimContext, INodeFile file, FileDiff removed) {
    BlockInfo[] removedBlocks = removed.getBlocks();
    if (removedBlocks == null) {
      FileWithSnapshotFeature sf = file.getFileWithSnapshotFeature();
      assert sf != null : "FileWithSnapshotFeature is null";
      if(sf.isCurrentFileDeleted())
        sf.collectBlocksAndClear(reclaimContext, file);
      return;
    }
    // 获取被删除快照之前最近一个有效快照ID
    int p = getPrior(removed.getSnapshotId(), true);
    FileDiff earlierDiff = p == Snapshot.NO_SNAPSHOT_ID ? null : getDiffById(p);
    // 如果更早快照没有块信息，则把被删除快照的块复制给更早快照
    if (earlierDiff != null) {
      earlierDiff.setBlocks(removedBlocks);
    }
    // 获取更早快照已有的块列表
    BlockInfo[] earlierBlocks =
        (earlierDiff == null ? new BlockInfoContiguous[]{} : earlierDiff.getBlocks());
    // 查找晚于被删除快照的最近一个有块信息的快照/当前文件的块列表
    BlockInfo[] laterBlocks = findLaterSnapshotBlocks(removed.getSnapshotId());
    laterBlocks = (laterBlocks == null) ? file.getBlocks() : laterBlocks;
    // 跳过已经被更早或更晚快照引用的块，只处理未被引用的块
    int i = 0;
    for(; i < removedBlocks.length; i++) {
      if(i < earlierBlocks.length && removedBlocks[i] == earlierBlocks[i])
        continue;
      if(i < laterBlocks.length && removedBlocks[i] == laterBlocks[i])
        continue;
      break;
    }
    // 检查截断恢复场景，需要保留截断块不删除
    BlockInfo lastBlock = file.getLastBlock();
    BlockInfo dontRemoveBlock = null;
    if (lastBlock != null && lastBlock.getBlockUCState().equals(
        HdfsServerConstants.BlockUCState.UNDER_RECOVERY)) {
      dontRemoveBlock = lastBlock.getUnderConstructionFeature()
          .getTruncateBlock();
    }
    // 收集所有未被引用的块，加入删除列表，跳过需要保留的截断块
    for (;i < removedBlocks.length; i++) {
      if(dontRemoveBlock == null || !removedBlocks[i].equals(dontRemoveBlock)) {
        reclaimContext.collectedBlocks().addDeleteBlock(removedBlocks[i]);
      }
    }
  }
}