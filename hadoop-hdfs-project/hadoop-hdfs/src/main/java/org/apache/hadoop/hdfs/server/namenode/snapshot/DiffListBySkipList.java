// 这个文件已经全部加上中文注释
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.namenode.snapshot;

import org.apache.hadoop.util.Preconditions;
import org.apache.hadoop.hdfs.server.namenode.INodeDirectory;
import org.apache.hadoop.hdfs.server.namenode.snapshot.
    DirectoryWithSnapshotFeature.DirectoryDiff;
import org.apache.hadoop.hdfs.server.namenode.snapshot.
    DirectoryWithSnapshotFeature.ChildrenDiff;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Arrays;
import java.util.Collections;
import java.util.Objects;

/**
 * 文件级注释：基于跳表实现的目录差异列表，用于HDFS快照功能中存储多个快照之间的目录变化，
 * 通过跳表分层结构优化差异合并计算和快照删除性能，加快获取指定范围快照差异的效率。
 * <p>
 * SkipList is an implementation of a data structure for storing a sorted list
 * of Directory Diff elements, using a hierarchy of linked lists that connect
 * increasingly sparse subsequences(defined by skip interval here) of the diffs.
 * The elements contained in the tree must be mutually comparable.
 * <p>
 * Consider  a case where we have 10 snapshots for a directory starting from s0
 * to s9 each associated with certain change records in terms of inodes deleted
 * and created after a particular snapshot and before the next snapshot. The
 * sequence will look like this:
 * <p>
 * {@literal s0->s1->s2->s3->s4->s5->s6->s7->s8->s9}.
 * <p>
 * Assuming a skip interval of 3, which means a new diff will be added at a
 * level higher than the current level after we have  ore than 3 snapshots.
 * Next level promotion happens after 9 snapshots and so on.
 * <p>
 * level 2:   {@literal s08------------------------------->s9}
 * level 1:   {@literal S02------->s35-------->s68-------->s9}
 * level 0:  {@literal s0->s1->s2->s3->s4->s5->s6->s7->s8->s9}
 * <p>
 * s02 will be created by combining diffs for s0, s1, s2 once s3 gets created.
 * Similarly, s08 will be created by combining s02, s35 and s68 once s9 gets
 * created.So, for constructing the children list fot s0, we have  to combine
 * s08, s9 and reverse apply to the live fs.
 * <p>
 * Similarly, for constructing the children list for s2, s2, s35, s68 and s9
 * need to get combined(or added) and reverse applied to current fs.
 * <p>
 * This approach will improve the snapshot deletion and snapshot diff
 * calculation.
 * <p>
 * Once a snapshot gets deleted, the list needs to be balanced.
 */
public class DiffListBySkipList implements DiffList<DirectoryDiff> {
  /** 日志记录器 */
  public static final Logger LOG =
      LoggerFactory.getLogger(DiffListBySkipList.class);

  /**
   * 将ChildrenDiff转换为带内存地址标识的字符串，用于调试输出
   * @param diff 要转换的ChildrenDiff对象
   * @return 转换后的字符串
   */
  static String childrenDiff2String(ChildrenDiff diff) {
    if (diff == null) {
      return "null";
    }
    return "@" + Integer.toHexString(System.identityHashCode(diff));
  }

  /**
   * 将跳转节点和对应差异转换为字符串，用于调试输出
   * @param skipTo 跳转目标节点
   * @param diff 对应区间合并差异
   * @return 转换后的字符串
   */
  static String skip2String(SkipListNode skipTo, ChildrenDiff diff) {
    return "->" + skipTo + ":diff=" + childrenDiff2String(diff);
  }

  /**
   * 内部类，存储跳转区间的合并差异和目标节点引用
   */
  private static class SkipDiff {
    static final SkipDiff[] EMPTY_ARRAY = {};

    /**
     * The references to the subsequent nodes.
     */
    private SkipListNode skipTo;
    /**
     * combined diff over a skip Interval.
     */
    private ChildrenDiff diff;

    SkipDiff(ChildrenDiff diff) {
      this.diff = diff;
    }

    public ChildrenDiff getDiff() {
      return diff;
    }

    public SkipListNode getSkipTo() {
      return skipTo;
    }

    public void setSkipTo(SkipListNode node) {
      skipTo = node;
    }

    public void setDiff(ChildrenDiff diff) {
      this.diff = diff;
    }

    @Override
    public String toString() {
      return skip2String(skipTo, diff);
    }
  }

  /**
   * 跳表节点类，存储单个目录差异，并维护不同层级的跳转指针和区间合并差异
   */
  final static class SkipListNode implements Comparable<Integer> {

    /**
     * The data element stored in this node.
     */
    private final DirectoryDiff diff;

    /** Next node in level 0. */
    private SkipListNode next;
    /**
     * Array containing combined children diffs over a skip interval.
     */
    private SkipDiff[] skips;

    /**
     * 构造跳表节点
     *
     * @param diff 该节点存储的目录差异
     * @param level 该节点的最高层级
     */
    SkipListNode(DirectoryDiff diff, int level) {
      this.diff = diff;

      this.skips = level > 0? new SkipDiff[level]: SkipDiff.EMPTY_ARRAY;
      // 初始化每个层级的SkipDiff对象
      for(int i = 0; i < skips.length; i++) {
        skips[i] = new SkipDiff(null);
      }
    }

    /**
     * Returns the level of this SkipListNode.
     */
    public int level() {
      return skips.length;
    }

    /** 修剪节点层级，移除末尾空的层级，节省空间 */
    void trim() {
      int n = skips.length - 1;
      // 从最高层向前查找第一个非空层级
      for (; n >= 0 && skips[n] == null; n--) {
        continue;
      }
      n++;
      // 如果需要修剪，复制数组到新长度
      if (n < skips.length) {
        skips = n > 0 ? Arrays.copyOf(skips, n) : SkipDiff.EMPTY_ARRAY;
      }
    }

    public DirectoryDiff getDiff() {
      return diff;
    }

    /**
     * Compare diffs with snapshot ID.
     */
    @Override
    public int compareTo(Integer that) {
      return diff.compareTo(that);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      SkipListNode that = (SkipListNode) o;
      return Objects.equals(diff, that.diff);
    }

    @Override
    public int hashCode() {
      return Objects.hash(diff);
    }

    /** 设置指定层级的合并差异 */
    public void setSkipDiff(ChildrenDiff cDiff, int level) {
      Preconditions.checkArgument(level > 0);
      // 如果层级超过当前数组大小，先扩容
      resize(level);
      skips[level - 1].setDiff(cDiff);
    }

    /** 更新所有从当前层级开始指向目标节点的跳转差异 */
    void setSkipDiff4Target(
        SkipListNode target, int startLevel, ChildrenDiff childrenDiff) {
      for(int i = startLevel; i <= level(); i++) {
        // 如果当前层级跳转节点已经不是目标，停止更新
        if (getSkipNode(i) != target) {
          return;
        }
        setSkipDiff(childrenDiff, i);
      }
    }

    /** 扩容跳表层级数组，添加新增层级的初始化对象 */
    private void resize(int newLevel) {
      int i = skips.length;
      // 如果当前长度小于目标层级，扩容数组
      if (i < newLevel) {
        skips = Arrays.copyOf(skips, newLevel);
        for (; i < newLevel; i++) {
          skips[i] = new SkipDiff(null);
        }
      }
    }

    /** 设置指定层级的跳转目标节点 */
    public void setSkipTo(SkipListNode node, int level) {
      if (level == 0) {
        next = node;
      } else {
        resize(level);
        skips[level - 1].setSkipTo(node);
      }
    }

    /** 获取指定层级的合并差异 */
    public ChildrenDiff getChildrenDiff(int level) {
      if (level == 0) {
        return diff != null? diff.getChildrenDiff(): null;
      } else {
        return skips[level - 1].getDiff();
      }
    }

    /** 获取指定层级的跳转目标节点 */
    SkipListNode getSkipNode(int level) {
      return level == 0? next
          : level <= skips.length? skips[level - 1].getSkipTo()
          : null;
    }

    @Override
    public String toString() {
      return diff != null ? "" + diff.getSnapshotId() : "?";
    }

    /** 将节点信息追加到StringBuilder，用于调试输出 */
    StringBuilder appendTo(StringBuilder b) {
      b.append(this).append(": ").append(skip2String(next, getChildrenDiff(0)));
      for(int i = 0; i < skips.length; i++) {
        b.append(", ").append(skips[i]);
      }
      return b;
    }
  }

  /**
   * 存储所有跳表节点的线性列表，提供按索引快速访问能力
   * 对外接口的线性列表视图由此提供
   */
  private final List<SkipListNode> skipNodeList;

  /**
   * 跳表头节点，不存储实际差异
   */
  private SkipListNode head;

  /**
   * 构造空的跳表差异列表
   * @param capacity 初始容量
   */
  public DiffListBySkipList(int capacity) {
    skipNodeList = new ArrayList<>(capacity);
    head = new SkipListNode(null, 0);
  }

  /**
   * 在列表头部插入新的目录差异，用于新增快照在最前的场景
   * @param diff 要插入的目录差异
   */
  @Override
  public void addFirst(DirectoryDiff diff) {
    final int nodeLevel = DirectoryDiffListFactory.randomLevel();
    // 存储每个层级插入位置的前驱节点
    final SkipListNode[] nodePath = new SkipListNode[nodeLevel + 1];
    Arrays.fill(nodePath, head);

    final SkipListNode newNode = new SkipListNode(diff, nodeLevel);
    // 按层级从低到高处理插入
    for (int level = 0; level <= nodeLevel; level++) {
      if (level > 0) {
        // 新增节点插入头部后，原有后续节点需要重新合并区间差异
        final SkipListNode nextNode = head.getSkipNode(level);
        if (nextNode != null) {
          ChildrenDiff combined = combineDiff(newNode, nextNode, level);
          if (combined != null) {
            newNode.setSkipDiff(combined, level);
          }
        }
      }
      // 插入到对应层级链表中
      newNode.setSkipTo(nodePath[level].getSkipNode(level), level);
      nodePath[level].setSkipTo(newNode, level);
    }
    skipNodeList.add(0, newNode);
  }

  /**
   * 查找指定节点在每个层级的前驱节点，用于插入/删除操作
   * @param node 目标节点，查找null则表示找最后一个节点
   * @param nodeLevel 目标节点的层级
   * @return 每个层级的前驱节点数组
   */
  private SkipListNode[] findPreviousNodes(SkipListNode node, int nodeLevel) {
    final SkipListNode[] nodePath = new SkipListNode[nodeLevel + 1];
    SkipListNode cur = head;
    final int headLevel = head.level();
    // 从最高层级向下查找
    for (int level = headLevel < nodeLevel ? headLevel : nodeLevel;
         level >= 0; level--) {
      // 一直向后查找直到找到目标节点的前驱
      while (cur.getSkipNode(level) != node) {
        cur = cur.getSkipNode(level);
      }
      nodePath[level] = cur;
    }
    // 超过头节点层级的部分前驱都设为头节点
    for (int level = headLevel + 1; level <= nodeLevel; level++) {
      nodePath[level] = head;
    }
    return nodePath;
  }

  /**
   * 在列表尾部追加新的目录差异，用于新增快照的常规场景
   * @param diff 要插入的目录差异
   * @return 插入成功返回true
   */
  @Override
  public boolean addLast(DirectoryDiff diff) {
    final int nodeLevel = DirectoryDiffListFactory.randomLevel();
    // 查找每个层级的前驱节点
    final SkipListNode[] nodePath = findPreviousNodes(null, nodeLevel);

    final SkipListNode newNode = new SkipListNode(diff, nodeLevel);
    // 按层级处理插入
    for (int level = 0; level <= nodeLevel; level++) {
      if (level > 0 && nodePath[level] != head) {
        // 新增节点插入尾部后，前驱节点需要合并当前区间的差异
        ChildrenDiff combined = combineDiff(nodePath[level], newNode, level);
        if (combined != null) {
          nodePath[level].setSkipDiff(combined, level);
        }
      }
      // 更新链表指针完成插入
      nodePath[level].setSkipTo(newNode, level);
      newNode.setSkipTo(null, level);
    }
    return skipNodeList.add(newNode);
  }

  /**
   * 合并从from节点到to节点之间指定层级的所有差异
   * @param from 起始节点（不包含）
   * @param to 结束节点
   * @param level 当前处理层级
   * @return 合并后的总差异
   */
  private static ChildrenDiff combineDiff(SkipListNode from, SkipListNode to,
      int level) {
    ChildrenDiff combined = null;
    ChildrenDiff first = null;

    SkipListNode cur = from;
    // 从低层级到高层级遍历，合并区间内所有预合并差异
    for (int i = level - 1; i >= 0; i--) {
      while (cur != to) {
        final SkipListNode next = cur.getSkipNode(i);
        if (next == null) {
          break;
        }

        // 第一个差异直接保存，不需要合并
        if (first == null) {
          first = cur.getChildrenDiff(i);
        } else {
          // 后续差异依次向后合并
          if (combined == null) {
            combined = new ChildrenDiff();
            combined.combinePosterior(first, null);
          }
          combined.combinePosterior(cur.getChildrenDiff(i), null);
        }
        cur = next;
      }
    }
    return combined != null? combined: first;
  }

  /**
   * 获取指定索引位置的目录差异
   *
   * @param index 索引位置
   * @return 指定位置的目录差异
   */
  @Override
  public DirectoryDiff get(int index) {
    return skipNodeList.get(index).getDiff();
  }

  /**
   * 获取指定索引位置的跳表节点
   * @param i 索引位置
   * @return 跳表节点
   */
  SkipListNode getSkipListNode(int i) {
    return skipNodeList.get(i);
  }

  /**
   * 删除指定索引位置的目录差异，用于删除快照场景
   *
   * @param index 要删除的索引位置
   * @return 被删除的目录差异
   */
  @Override
  public DirectoryDiff remove(int index) {
    final SkipListNode node = getNode(index);

    int headLevel = head.level();