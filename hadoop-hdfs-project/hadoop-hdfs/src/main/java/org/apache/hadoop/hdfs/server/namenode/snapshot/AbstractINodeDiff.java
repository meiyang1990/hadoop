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

import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeAttributes;
import org.apache.hadoop.hdfs.server.namenode.snapshot.SnapshotFSImageFormat.ReferenceMap;

import org.apache.hadoop.util.Preconditions;

/**
 * 文件级概要说明：HDFS快照INode差异抽象基类，用于记录不同快照版本之间INode的变更信息
 *
 * The difference of an inode between in two snapshots.
 * {@link AbstractINodeDiffList} maintains a list of snapshot diffs,
 * <pre>
 *   d_1 -> d_2 -> ... -> d_n -> null,
 * </pre>
 * where -> denotes the {@link AbstractINodeDiff#posteriorDiff} reference. The
 * current directory state is stored in the field of {@link INode}.
 * The snapshot state can be obtained by applying the diffs one-by-one in
 * reversed chronological order.  Let s_1, s_2, ..., s_n be the corresponding
 * snapshots.  Then,
 * <pre>
 *   s_n                     = (current state) - d_n;
 *   s_{n-1} = s_n - d_{n-1} = (current state) - d_n - d_{n-1};
 *   ...
 *   s_k     = s_{k+1} - d_k = (current state) - d_n - d_{n-1} - ... - d_k.
 * </pre>
 *
 * 类级注释：INode差异抽象基类，定义了快照间INode变更存储的公共结构和接口，
 * 核心职责是记录对应快照的INode原始数据，维护diff链表结构，支持通过当前状态反向推导得到历史快照状态
 * @param <N> INode类型
 * @param <A> INode属性类型
 * @param <D> INode差异类型
 */
abstract class AbstractINodeDiff<N extends INode,
                                 A extends INodeAttributes,
                                 D extends AbstractINodeDiff<N, A, D>>
    implements Comparable<Integer> {

  /** The id of the corresponding snapshot. */
  private int snapshotId;
  /** The snapshot inode data.  It is null when there is no change. */
  A snapshotINode;
  /**
   * Posterior diff is the diff happened after this diff.
   * The posterior diff should be first applied to obtain the posterior
   * snapshot and then apply this diff in order to obtain this snapshot.
   * If the posterior diff is null, the posterior state is the current state. 
   */
  private D posteriorDiff;

  /**
   * 构造函数，创建对应快照的INode差异对象
   * @param snapshotId 对应快照的ID
   * @param snapshotINode 快照保存的原始INode属性，无变更时为null
   * @param posteriorDiff 当前diff之后发生的后续diff链表节点
   */
  AbstractINodeDiff(int snapshotId, A snapshotINode, D posteriorDiff) {
    this.snapshotId = snapshotId;
    this.snapshotINode = snapshotINode;
    this.posteriorDiff = posteriorDiff;
  }

  /** Compare diffs with snapshot ID. */
  @Override
  public final int compareTo(final Integer that) {
    return Snapshot.ID_INTEGER_COMPARATOR.compare(this.snapshotId, that);
  }

  /** @return the snapshot object of this diff. */
  public final int getSnapshotId() {
    return snapshotId;
  }
  
  final void setSnapshotId(int snapshot) {
    this.snapshotId = snapshot;
  }

  /** @return the posterior diff. */
  final D getPosterior() {
    return posteriorDiff;
  }

  final void setPosterior(D posterior) {
    posteriorDiff = posterior;
  }

  /** Save the INode state to the snapshot if it is not done already. */
  void saveSnapshotCopy(A snapshotCopy) {
    Preconditions.checkState(snapshotINode == null, "Expected snapshotINode to be null");
    snapshotINode = snapshotCopy;
  }

  /** @return the inode corresponding to the snapshot. */
  A getSnapshotINode() {
    // 从当前diff开始向后查找，找到第一个保存了INode数据的diff并返回
    // 如果遍历到最后都没有保存的数据，返回null表示使用当前INode状态
    for(AbstractINodeDiff<N, A, D> d = this; ; d = d.posteriorDiff) {
      if (d.snapshotINode != null) {
        return d.snapshotINode;
      } else if (d.posteriorDiff == null) {
        return null;
      }
    }
  }

  /** 
   * 合并后续diff并收集待删除块的抽象方法，不同INode类型实现不同合并逻辑
   * @param reclaimContext 回收上下文，用于收集需要回收的块和inode
   * @param currentINode 当前INode对象
   * @param posterior 待合并的后续diff
   */
  abstract void combinePosteriorAndCollectBlocks(
      INode.ReclaimContext reclaimContext, final N currentINode,
      final D posterior);
  
  /**
   * 销毁当前diff并收集需要回收的块和inode，用于清理过期快照
   * @param reclaimContext blocks and inodes that need to be reclaimed
   * @param currentINode The inode where the deletion happens.
   */
  abstract void destroyDiffAndCollectBlocks(INode.ReclaimContext reclaimContext,
      final N currentINode);

  @Override
  public String toString() {
    return getClass().getSimpleName() + ": " + this.getSnapshotId() + " (post="
        + (posteriorDiff == null? null: posteriorDiff.getSnapshotId()) + ")";
  }

  /**
   * 将diff信息写入fsimage输出流，保存快照ID基础信息
   * @param out 数据输出流
   * @throws IOException IO写入异常
   */
  void writeSnapshot(DataOutput out) throws IOException {
    out.writeInt(snapshotId);
  }

  /**
   * 将diff完整信息序列化写入输出流的抽象方法，供不同子类实现
   * @param out 数据输出流
   * @param referenceMap 引用映射表，用于处理重复引用序列化
   * @throws IOException IO写入异常
   */
  abstract void write(DataOutput out, ReferenceMap referenceMap
      ) throws IOException;
}