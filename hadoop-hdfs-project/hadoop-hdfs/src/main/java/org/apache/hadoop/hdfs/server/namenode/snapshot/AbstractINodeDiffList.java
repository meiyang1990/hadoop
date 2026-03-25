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

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeAttributes;
import org.apache.hadoop.hdfs.server.namenode.INodeDirectory;

/**
 * 文件路径: hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/snapshot/AbstractINodeDiffList.java
 * <p>
 * 快照差异列表抽象基类，用于存储INode节点在不同快照之间的差异数据。
 * 该抽象类定义了快照差异列表的通用操作，子类实现具体类型差异的创建逻辑，
 * 按快照ID升序（即时间顺序）存储差异，支持高效查找和修改操作。
 *
 * @param <N> INode节点类型，必须继承自INode
 * @param <A> INode属性类型，必须继承自INodeAttributes
 * @param <D> 差异类型，必须继承自AbstractINodeDiff
 */
abstract class AbstractINodeDiffList<N extends INode,
                                     A extends INodeAttributes,
                                     D extends AbstractINodeDiff<N, A, D>> 
    implements Iterable<D> {

  /** 按快照ID排序（即时间顺序）的差异列表，采用延迟创建避免空列表浪费内存 */
  private DiffList<D> diffs;

  /**
   * @return 返回不可修改的差异列表视图
   */
  public final DiffList<D> asList() {
    return diffs != null ?
        DiffList.unmodifiableList(diffs) : DiffList.emptyList();
  }

  /**
   * @return 判断当前差异列表是否为空
   */
  public boolean isEmpty() {
    return diffs == null || diffs.isEmpty();
  }
  
  /** 清空整个差异列表，释放内存 */
  public void clear() {
    diffs = null;
  }

  /**
   * 创建一个新的差异对象
   * @param snapshotId 快照ID
   * @param currentINode 当前INode节点
   * @return 新建的差异对象
   */
  abstract D createDiff(int snapshotId, N currentINode);

  /**
   * 创建当前INode的快照属性拷贝
   * @param currentINode 当前INode节点
   * @return 当前INode属性的快照拷贝
   */
  abstract A createSnapshotCopy(N currentINode);

  /**
   * 删除指定快照对应的差异，处理差异合并逻辑。
   * 如果待删除差异不是列表第一个，需要将其与前一个差异合并。
   * 本方法不负责同步，同步逻辑在外部完成。
   * 
   * @param reclaimContext 需要回收的块和INode上下文
   * @param snapshot 待删除快照的ID
   * @param prior 待删除快照之前的那个快照的ID
   * @param currentINode 差异所属的当前INode节点
   */
  public final void deleteSnapshotDiff(INode.ReclaimContext reclaimContext,
      final int snapshot, final int prior, final N currentINode) {
    if (diffs == null) {
      return;
    }
    // 通过二分查找定位待删除快照差异在列表中的索引
    final int snapshotIndex = diffs.binarySearch(snapshot);
    // 删除有序检查：只能删除索引为0的元素，且当删除有序时检查索引合法性
    assert !SnapshotManager.isDeletionOrdered()
        || (snapshotIndex <= 0 && prior == Snapshot.NO_SNAPSHOT_ID);

    D removed;
    if (snapshotIndex == 0) {
      // 删除的是第一个差异
      if (prior != Snapshot.NO_SNAPSHOT_ID) {
        // 前面还有快照，只需要更新当前差异的快照ID为前一个快照ID
        diffs.get(snapshotIndex).setSnapshotId(prior);
      } else {
        // 前面没有快照了，直接移除这个差异并销毁回收资源
        removed = diffs.remove(0);
        if (diffs.isEmpty()) {
          diffs = null;
        }
        removed.destroyDiffAndCollectBlocks(reclaimContext, currentINode);
      }
    } else if (snapshotIndex > 0) {
      // 删除的不是第一个差异，获取前一个差异
      final AbstractINodeDiff<N, A, D> previous = diffs.get(snapshotIndex - 1);
      if (previous.getSnapshotId() != prior) {
        // 前一个差异不是删除后的前驱，只更新当前差异快照ID
        diffs.get(snapshotIndex).setSnapshotId(prior);
      } else {
        // 需要将待删除差异与前驱差异合并
        removed = diffs.remove(snapshotIndex);
        if (previous.snapshotINode == null) {
          // 前驱没有保存快照INode，直接使用待删除的快照INode
          previous.snapshotINode = removed.snapshotINode;
        }
        // 合并差异并回收不用的块
        previous.combinePosteriorAndCollectBlocks(reclaimContext, currentINode,
            removed);
        // 更新后继指针
        previous.setPosterior(removed.getPosterior());
        removed.setPosterior(null);
      }
    }
  }

  /**
   * 为指定快照添加一个新的差异到列表末尾
   * @param latestSnapshotId 最新快照ID
   * @param currentINode 当前INode节点
   * @return 新添加的差异对象
   */
  final D addDiff(int latestSnapshotId, N currentINode) {
    return addLast(createDiff(latestSnapshotId, currentINode));
  }

  /**
   * 将差异追加到列表末尾
   * @param diff 待添加的差异
   * @return 追加的差异对象
   */
  private D addLast(D diff) {
    createDiffsIfNeeded();
    final D last = getLast();
    diffs.addLast(diff);
    if (last != null) {
      // 更新原最后一个差异的后继指针指向新差异
      last.setPosterior(diff);
    }
    return diff;
  }
  
  /**
   * 将差异添加到列表开头
   * @param diff 待添加的差异
   */
  final void addFirst(D diff) {
    createDiffsIfNeeded();
    final D first = diffs.isEmpty()? null : diffs.get(0);
    diffs.addFirst(diff);
    diff.setPosterior(first);
  }

  /**
   * @return 获取列表第一个差异
   */
  final D getFirst() {
    return diffs == null || diffs.isEmpty()? null: diffs.get(0);
  }

  /**
   * @return 获取第一个差异中的快照INode属性
   */
  final A getFirstSnapshotINode() {
    final D first = getFirst();
    return first == null? null: first.getSnapshotINode();
  }

  /**
   * @return 获取列表最后一个差异
   */
  public final D getLast() {
    if (diffs == null) {
      return null;
    }
    int n = diffs.size();
    return n == 0 ? null : diffs.get(n - 1);
  }

  /**
   * 创建新的差异列表实例，默认使用ArrayList实现
   * @return 新建的差异列表实例
   */
  DiffList<D> newDiffs() {
    return new DiffListByArrayList<>(
        INodeDirectory.DEFAULT_FILES_PER_DIRECTORY);
  }

  /** 延迟创建差异列表，当列表不存在时初始化 */
  private void createDiffsIfNeeded() {
    if (diffs == null) {
      diffs = newDiffs();
    }
  }

  /**
   * @return 获取最后一个快照的ID
   */
  public final int getLastSnapshotId() {
    final AbstractINodeDiff<N, A, D> last = getLast();
    return last == null ? Snapshot.CURRENT_STATE_ID : last.getSnapshotId();
  }
  
  /**
   * 查找给定快照ID之前的最新快照ID
   * @param anchorId 锚点快照ID，返回的快照ID必须满足小于等于（或小于）该值
   * @param exclusive 是否为开区间，true表示返回的快照ID必须小于锚点，否则可以小于等于
   * @return 满足条件的最新快照ID，如果没有返回NO_SNAPSHOT_ID
   */
  public final int getPrior(int anchorId, boolean exclusive) {
    if (diffs == null) {
      return Snapshot.NO_SNAPSHOT_ID;
    }
    // 如果锚点是当前状态，直接返回最后一个快照ID
    if (anchorId == Snapshot.CURRENT_STATE_ID) {
      int last = getLastSnapshotId();
      if (exclusive && last == anchorId) {
        return Snapshot.NO_SNAPSHOT_ID;
      }
      return last;
    }
    final int i = diffs.binarySearch(anchorId);
    if (exclusive) {
      // 开区间，必须返回锚点之前的那个
      if (i == -1 || i == 0) {
        // i=-1表示所有元素都大于锚点，i=0表示锚点是第一个，前面没有元素
        return Snapshot.NO_SNAPSHOT_ID;
      } else {
        int priorIndex = i > 0 ? i - 1 : -i - 2;
        return diffs.get(priorIndex).getSnapshotId();
      }
    } else {
      // 闭区间，找到锚点直接返回，否则返回前一个
      if (i >= 0) {
        return diffs.get(i).getSnapshotId();
      } else if (i < -1) {
        return diffs.get(-i - 2).getSnapshotId();
      } else {
        // i == -1，所有元素都大于锚点，前面没有
        return Snapshot.NO_SNAPSHOT_ID;
      }
    }
  }
  
  /**
   * 查找给定快照ID之前的最新快照ID（闭区间模式）
   * @param snapshotId 锚点快照ID
   * @return 满足条件的最新快照ID
   */
  public final int getPrior(int snapshotId) {
    return getPrior(snapshotId, false);
  }
  
  /**
   * 更新前驱快照ID，返回比给定prior更大的最新前驱
   * @param snapshot 当前快照ID
   * @param prior 原前驱快照ID
   * @return 更新后的前驱快照ID
   */
  final int updatePrior(int snapshot, int prior) {
    int p = getPrior(snapshot, true);
    if (p != Snapshot.CURRENT_STATE_ID
        && Snapshot.ID_INTEGER_COMPARATOR.compare(p, prior) > 0) {
      return p;
    }
    return prior;
  }
  
  /**
   * 根据快照ID获取对应的差异对象
   * @param snapshotId 快照ID
   * @return 对应差异对象，如果没有找到返回null
   */
  public final D getDiffById(final int snapshotId) {
    if (snapshotId == Snapshot.CURRENT_STATE_ID || diffs == null) {
      return null;
    }
    final int i = diffs.binarySearch(snapshotId);
    if (i >= 0) {
      // 精确匹配，直接返回
      return diffs.get(i);
    } else {
      // 精确匹配不到说明给定快照和下一个状态之间没有变化，返回下一个状态对应的差异
      final int j = -i - 1;
      return j < diffs.size() ? diffs.get(j) : null;
    }
  }
  
  /**
   * 查找不小于给定快照ID的最接近快照ID
   * @param snapshotId 输入快照ID
   * @return 匹配到的快照ID，没有则返回CURRENT_STATE_ID
   */
  public final int getSnapshotById(final int snapshotId) {
    D diff = getDiffById(snapshotId);
    return diff == null ? Snapshot.CURRENT_STATE_ID : diff.getSnapshotId();
  }

  /**
   * 根据快照ID查找对应的差异索引
   * @param snapshotId 快照ID
   * @return 差异索引，如果找不到返回插入点索引
   */
  public final int getDiffIndexById(final int snapshotId) {
    int diffIndex = diffs.binarySearch(snapshotId);
    diffIndex = diffIndex < 0 ? (-diffIndex - 1) : diffIndex;
    return diffIndex;
  }

  /**
   * 计算两个快照之间发生变化的差异索引范围
   * @param from 第一个快照
   * @param to 第二个快照
   * @return 变化范围的起始和结束索引数组，如果没有变化返回null
   */
  final int[] changedBetweenSnapshots(Snapshot from, Snapshot to) {
    if (diffs == null) {
      return null;
    }
    // 保证顺序，earlier是较早的快照，later是较晚的快照
    Snapshot earlier = from;
    Snapshot later = to;
    if (Snapshot.ID_COMPARATOR.compare(from, to) > 0) {
      earlier = to;
      later = from;
    }

    final int size = diffs.size();
    int earlierDiffIndex = getDiffIndexById(earlier.getId());
    int laterDiffIndex = later == null ? size
        : getDiffIndexById(later.getId());
    if (earlierDiffIndex == size) {
      // 较早的快照在所有差异之后，说明没有变化
      return null;
    }
    if (laterDiffIndex == -1 || laterDiffIndex == 0) {
      // 较晚的快照在所有差异之前，说明没有变化
      return null;
    }
    // 返回变化范围索引
    return new int[]{earlierDiffIndex, laterDiffIndex};
  }

  /**
   * 获取指定快照对应的INode属性，如果指定快照和当前状态之间没有变化则返回当前INode属性
   * @param snapshotId 快照ID
   * @param currentINode 当前INode属性
   * @return 指定快照对应的INode属性
   */
  public A getSnapshotINode(final int snapshotId, final A currentINode) {
    final D diff = getDiffById(snapshotId);
    final A inode = diff == null? null: diff.getSnapshotINode();
    return inode == null? currentINode: inode;
  }

  /**
   * 检查最新快照差异是否存在，不存在则添加
   * @param latestSnapshotId 最新快照ID
   * @param currentINode 当前INode节点
   * @return 最新快照差异，不会返回null
   */
  final D checkAndAddLatestSnapshotDiff(int latestSnapshotId, N currentINode) {
    final D last = getLast();
    return (last != null && Snapshot.ID_INTEGER_COMPARATOR
        .compare(last.getSnapshotId(), latestSnapshotId) >= 0) ?
        last : addDiff(latestSnapshotId, currentINode);
  }

  /**
   * 将当前INode的状态保存为快照拷贝到最新快照差异中
   * @param latestSnapshotId 最新快照ID
   * @param currentINode 当前INode节点
   * @param snapshotCopy 已有的快照拷贝，如果为null则新建
   * @return 保存后的快照差异，如果不需要保存返回null
   */
  public D saveSelf2Snapshot(int latestSnapshotId, N currentINode,
      A snapshotCopy) {
    D diff = null;
    if (latestSnapshotId != Snapshot.CURRENT_STATE_ID) {
      diff = checkAndAddLatestSnapshotDiff(latestSnapshotId, currentINode);
      if (diff.snapshotINode == null) {
        if (snapshotCopy == null) {
          // 没有传入拷贝，新建当前INode的快照拷贝
          snapshotCopy = createSnapshotCopy(currentINode);
        }
        diff.saveSnapshotCopy(snapshotCopy);
      }
    }
    return diff;
  }

  @Override
  public Iterator<D> iterator() {
    return diffs != null ? diffs.iterator() : Collections.emptyIterator();
  }

  @Override
  public String toString() {
    if (diffs != null) {
      final StringBuilder b =
          new StringBuilder(getClass().getSimpleName()).append("@")
              .append(Integer.toHexString(hashCode())).append(": ");
      b.append("[");
      for (D d : diffs) {
        b.append(d).append(", ");
      }
      b.setLength(b.length() - 2);
      b.append("]");
      return b.toString();
    } else {
      return "";
    }
  }
}