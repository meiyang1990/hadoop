// 这个文件已经全部加上中文注释
/*
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
package org.apache.hadoop.hdfs.server.namenode.visitor;

import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeDirectory;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;
import org.apache.hadoop.hdfs.server.namenode.INodeReference;
import org.apache.hadoop.hdfs.server.namenode.INodeSymlink;
import org.apache.hadoop.hdfs.server.namenode.snapshot.DirectorySnapshottableFeature;
import org.apache.hadoop.hdfs.server.namenode.snapshot.DirectoryWithSnapshotFeature;
import org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot;

import java.util.Iterator;

/**
 * HDFS命名空间树访问器接口，定义了遍历HDFS文件系统目录树（含快照）的统一访问契约
 * 实现访问者模式，对不同类型INode提供不同的访问处理逻辑
 */
public interface NamespaceVisitor {
  /**
   * 通用INode访问器接口，所有INode类型访问的顶层抽象
   */
  interface INodeVisitor {
    INodeVisitor DEFAULT = new INodeVisitor() {};

    /**
     * 访问指定INode节点
     * @param iNode 待访问的INode节点
     * @param snapshot 快照ID，CURRENT_STATE_ID表示当前状态
     */
    default void visit(INode iNode, int snapshot) {
    }
  }

  /**
   * 获取默认非递归INode访问器实例
   * @return 默认访问器实例
   */
  default INodeVisitor getDefaultVisitor() {
    return INodeVisitor.DEFAULT;
  }

  /**
   * 访问文件INode节点
   * @param file 待访问的文件INode
   * @param snapshot 快照ID
   */
  default void visitFile(INodeFile file, int snapshot) {
    getDefaultVisitor().visit(file, snapshot);
  }

  /**
   * 访问符号链接INode节点
   * @param symlink 待访问的符号链接INode
   * @param snapshot 快照ID
   */
  default void visitSymlink(INodeSymlink symlink, int snapshot) {
    getDefaultVisitor().visit(symlink, snapshot);
  }

  /**
   * 非递归访问引用INode节点
   * @param ref 待访问的引用INode
   * @param snapshot 快照ID
   */
  default void visitReference(INodeReference ref, int snapshot) {
    getDefaultVisitor().visit(ref, snapshot);
  }

  /**
   * 递归访问引用INode节点，先访问引用自身，再访问被引用的目标节点
   * @param ref 待访问的引用INode
   * @param snapshot 快照ID
   */
  default void visitReferenceRecursively(INodeReference ref, int snapshot) {
    visitReference(ref, snapshot);

    final INode referred = ref.getReferredINode();
    preVisitReferred(referred);
    referred.accept(this, snapshot);
    postVisitReferred(referred);
  }

  /**
   * 访问被引用INode之前的回调钩子
   * @param referred 即将被访问的被引用INode
   */
  default void preVisitReferred(INode referred) {
  }

  /**
   * 访问被引用INode之后的回调钩子
   * @param referred 刚刚访问完成的被引用INode
   */
  default void postVisitReferred(INode referred) {
  }

  /**
   * 非递归访问目录INode节点
   * @param dir 待访问的目录INode
   * @param snapshot 快照ID
   */
  default void visitDirectory(INodeDirectory dir, int snapshot) {
    getDefaultVisitor().visit(dir, snapshot);
  }

  /**
   * 递归访问目录INode节点，访问顺序：目录自身 -> 子节点 -> 如果是可快照目录则访问所有快照
   * @param dir 待访问的目录INode
   * @param snapshot 快照ID
   */
  default void visitDirectoryRecursively(INodeDirectory dir, int snapshot) {
    visitDirectory(dir, snapshot);
    visitSubs(getChildren(dir, snapshot));

    if (snapshot == Snapshot.CURRENT_STATE_ID) {
      final DirectorySnapshottableFeature snapshottable
          = dir.getDirectorySnapshottableFeature();
      if (snapshottable != null) {
        visitSnapshottable(dir, snapshottable);
        visitSubs(getSnapshots(snapshottable));
      }
    }
  }

  /**
   * 访问子元素之前的回调钩子
   * 子元素可以是目录的子节点，也可以是可快照目录下的快照
   * @param sub 待访问的子元素
   * @param index 子元素在列表中的索引
   * @param isLast 是否是最后一个子元素
   */
  default void preVisitSub(Element sub, int index, boolean isLast) {
  }

  /**
   * 访问子元素之后的回调钩子
   * 子元素可以是目录的子节点，也可以是可快照目录下的快照
   * @param sub 刚刚访问完成的子元素
   * @param index 子元素在列表中的索引
   * @param isLast 是否是最后一个子元素
   */
  default void postVisitSub(Element sub, int index, boolean isLast) {
  }

  /**
   * 访问可快照目录特性对象
   * @param dir 所属目录INode
   * @param snapshottable 可快照特性对象
   */
  default void visitSnapshottable(INodeDirectory dir,
      DirectorySnapshottableFeature snapshottable) {
  }

  /**
   * 递归遍历所有子元素
   * @param subs 待遍历的子元素集合，可以是目录子节点或目录快照
   */
  default void visitSubs(Iterable<Element> subs) {
    if (subs == null) {
      return;
    }
    int index = 0;
    for(final Iterator<Element> i = subs.iterator(); i.hasNext();) {
      final Element e = i.next();
      final boolean isList = !i.hasNext();
      preVisitSub(e, index, isList);
      e.getInode().accept(this, e.getSnapshotId());
      postVisitSub(e, index, isList);
      index++;
    }
  }

  /**
   * 将指定目录在对应快照下的子节点转换为Element可迭代集合
   * @param dir 目标目录
   * @param snapshot 快照ID
   * @return 子节点Element可迭代集合
   */
  static Iterable<Element> getChildren(INodeDirectory dir, int snapshot) {
    final Iterator<INode> i = dir.getChildrenList(snapshot).iterator();
    return new Iterable<Element>() {
      @Override
      public Iterator<Element> iterator() {
        return new Iterator<Element>() {
          @Override
          public boolean hasNext() {
            return i.hasNext();
          }

          @Override
          public Element next() {
            return new Element(snapshot, i.next());
          }

          @Override
          public void remove() {
            throw new UnsupportedOperationException();
          }
        };
      }
    };
  }

  /**
   * 将可快照目录的所有快照转换为Element可迭代集合
   * @param snapshottable 可快照特性对象
   * @return 快照根节点Element可迭代集合
   */
  static Iterable<Element> getSnapshots(
      DirectorySnapshottableFeature snapshottable) {
    final Iterator<DirectoryWithSnapshotFeature.DirectoryDiff> i
        = snapshottable.getDiffs().iterator();
    return new Iterable<Element>() {
      @Override
      public Iterator<Element> iterator() {
        return new Iterator<Element>() {
          private DirectoryWithSnapshotFeature.DirectoryDiff next = findNext();

          private DirectoryWithSnapshotFeature.DirectoryDiff findNext() {
            for(; i.hasNext();) {
              final DirectoryWithSnapshotFeature.DirectoryDiff diff = i.next();
              if (diff.isSnapshotRoot()) {
                return diff;
              }
            }
            return null;
          }

          @Override
          public boolean hasNext() {
            return next != null;
          }

          @Override
          public Element next() {
            final int id = next.getSnapshotId();
            final Element e = new Element(id,
                snapshottable.getSnapshotById(id).getRoot());
            next = findNext();
            return e;
          }

          @Override
          public void remove() {
            throw new UnsupportedOperationException();
          }
        };
      }
    };
  }

  /**
   * 元素封装类，绑定INode节点与对应的快照ID，用于统一遍历当前节点和快照节点
   */
  class Element {
    private final int snapshotId;
    private final INode inode;

    Element(int snapshot, INode inode) {
      this.snapshotId = snapshot;
      this.inode = inode;
    }

    INode getInode() {
      return inode;
    }

    int getSnapshotId() {
      return snapshotId;
    }
  }
}