// 这个文件已经全部加上中文注释
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file unless in compliance
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
package org.apache.hadoop.hdfs.server.namenode.visitor;

import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 文件概述：实现HDFS命名空间遍历访问者，用于统计FSImage中INode的引用次数，验证FSImage完整性
 * 核心职责：遍历整个命名空间树，统计每个INode被引用的次数，用于FSImage正确性校验
 */
/**
 * For validating {@link org.apache.hadoop.hdfs.server.namenode.FSImage}s.
 */
public class INodeCountVisitor implements NamespaceVisitor {
  /**
   * 统计结果获取接口，定义获取指定INode统计次数的方法
   */
  public interface Counts {
    int getCount(INode inode);
  }

  /**
   * 从根INode开始遍历整棵树，统计所有INode的引用次数
   * @param root 命名空间根INode
   * @return 统计结果对象，可查询每个INode的引用次数
   */
  public static Counts countTree(INode root) {
    return new INodeCountVisitor().count(root);
  }

  /**
   * 内部存储元素类，封装单个INode及其引用计数
   */
  private static class SetElement {
    private final INode inode;
    private final AtomicInteger count = new AtomicInteger();

    SetElement(INode inode) {
      this.inode = inode;
    }

    int getCount() {
      return count.get();
    }

    int incrementAndGet() {
      return count.incrementAndGet();
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      } else if (obj == null || getClass() != obj.getClass()) {
        return false;
      }
      final SetElement that = (SetElement) obj;
      // 通过INode ID判断相等性，同一个INode ID视为同一个元素
      return this.inode.getId() == that.inode.getId();
    }

    @Override
    public int hashCode() {
      // 基于INode ID生成哈希码，保证相等对象哈希码相同
      return Long.hashCode(inode.getId());
    }
  }

  /**
   * INode集合统计类，线程安全存储所有INode并维护其引用计数，实现统计结果接口
   */
  static class INodeSet implements Counts {
    // 并发哈希表存储所有INode元素，支持多线程遍历统计
    private final ConcurrentMap<SetElement, SetElement> map
        = new ConcurrentHashMap<>();

    /**
     * 添加一个INode引用，递增其计数
     * @param inode 被访问的INode
     * @param snapshot 快照ID
     * @return 递增后的引用计数
     */
    int put(INode inode, int snapshot) {
      final SetElement key = new SetElement(inode);
      // 原子性插入，如果不存在则插入新元素
      final SetElement previous = map.putIfAbsent(key, key);
      // 已存在则使用已有元素，否则使用新插入的元素
      final SetElement current = previous != null? previous: key;
      // 递增计数并返回结果
      return current.incrementAndGet();
    }

    @Override
    public int getCount(INode inode) {
      final SetElement key = new SetElement(inode);
      final SetElement value = map.get(key);
      // 返回计数，不存在则返回0
      return value != null? value.getCount(): 0;
    }
  }

  // 存储所有遍历过的INode及其计数
  private final INodeSet inodes = new INodeSet();

  /**
   * 获取默认INode访问器，用于统计每个访问到的INode
   * @return 默认访问器实例
   */
  @Override
  public INodeVisitor getDefaultVisitor() {
    return new INodeVisitor() {
      @Override
      public void visit(INode iNode, int snapshot) {
        // 将访问到的INode加入统计，递增引用计数
        inodes.put(iNode, snapshot);
      }
    };
  }

  /**
   * 从根节点开始遍历命名空间，完成INode计数
   * @param root 根INode
   * @return 统计结果
   */
  private Counts count(INode root) {
    // 接受访问者遍历，从当前最新状态开始遍历
    root.accept(this, Snapshot.CURRENT_STATE_ID);
    return inodes;
  }
}