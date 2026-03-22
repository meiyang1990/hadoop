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

import org.apache.hadoop.util.Preconditions;
import org.apache.hadoop.hdfs.server.namenode.DirectoryWithQuotaFeature;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeDirectory;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;
import org.apache.hadoop.hdfs.server.namenode.INodeReference;
import org.apache.hadoop.hdfs.server.namenode.INodeSymlink;
import org.apache.hadoop.hdfs.server.namenode.snapshot.DirectorySnapshottableFeature;
import org.apache.hadoop.hdfs.server.namenode.snapshot.DirectoryWithSnapshotFeature;
import org.apache.hadoop.hdfs.server.namenode.snapshot.DirectoryWithSnapshotFeature.DirectoryDiff;
import org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot;

import java.io.PrintWriter;
import java.io.StringWriter;

/**
 * 文件级注释：HDFS命名空间树打印访问器，用于测试场景递归输出目录树结构
 *
 * 输出示例：
 *      \- foo   (INodeDirectory@33dd2717)
 *        \- sub1   (INodeDirectory@442172)
 *          +- file1   (INodeFile@78392d4)
 *          +- file2   (INodeFile@78392d5)
 *          +- sub11   (INodeDirectory@8400cff)
 *            \- file3   (INodeFile@78392d6)
 *          \- z_file4   (INodeFile@45848712)
 *
 * 该类实现NamespaceVisitor接口，遍历命名空间节点并格式化输出树结构，主要用于调试和测试
 */
public final class NamespacePrintVisitor implements NamespaceVisitor {
  /** 非最后一个子节点前缀标记 */
  static final String NON_LAST_ITEM = "+-";
  /** 最后一个子节点前缀标记 */
  static final String LAST_ITEM = "\\-";

  /**
   * 从指定FSNamesystem生成命名空间树的字符串表示
   * @param ns FSNamesystem对象，包含整个HDFS命名空间
   * @return 格式化后的命名空间树字符串
   */
  public static String print2Sting(FSNamesystem ns) {
    return print2Sting(ns.getFSDirectory().getRoot());
  }

  /**
   * 从指定根节点生成命名空间树的字符串表示
   * @param root 遍历起始根INode节点
   * @return 格式化后的命名空间树字符串
   */
  public static String print2Sting(INode root) {
    final StringWriter out = new StringWriter();
    new NamespacePrintVisitor(new PrintWriter(out)).print(root);
    return out.getBuffer().toString();
  }

  /** 输出打印流 */
  private final PrintWriter out;
  /** 当前行前缀，用于维护树结构缩进 */
  private final StringBuilder prefix = new StringBuilder();

  private NamespacePrintVisitor(PrintWriter out) {
    this.out = out;
  }

  /**
   * 从指定根节点开始打印命名空间树
   * @param root 起始根INode节点
   */
  private void print(INode root) {
    root.accept(this, Snapshot.CURRENT_STATE_ID);
  }

  /**
   * 通用打印INode节点基础信息
   * @param iNode 待打印INode节点
   * @param snapshot 快照ID
   */
  private void printINode(INode iNode, int snapshot) {
    iNode.dumpINode(out, prefix, snapshot);
  }

  @Override
  /**
   * 访问文件节点时，打印文件节点详细信息
   * @param file 待访问的文件INode
   * @param snapshot 快照ID
   */
  public void visitFile(INodeFile file, int snapshot) {
    file.dumpINodeFile(out, prefix, snapshot);
  }

  @Override
  /**
   * 访问符号链接节点时，打印符号链接基础信息和目标路径
   * @param symlink 待访问的符号链接INode
   * @param snapshot 快照ID
   */
  public void visitSymlink(INodeSymlink symlink, int snapshot) {
    printINode(symlink, snapshot);
    out.print(" ~> ");
    out.println(symlink.getSymlinkString());
  }

  @Override
  /**
   * 访问引用节点时，打印引用节点基础信息和引用详细属性
   * @param ref 待访问的引用INode
   * @param snapshot 快照ID
   */
  public void visitReference(INodeReference ref, int snapshot) {
    printINode(ref, snapshot);

    // 根据引用类型打印不同属性
    if (ref instanceof INodeReference.DstReference) {
      out.print(", dstSnapshotId=" + ref.getDstSnapshotId());
    } else if (ref instanceof INodeReference.WithCount) {
      out.print(", " + ((INodeReference.WithCount)ref).getCountDetails());
    }
    out.println();
  }

  @Override
  /**
   * 访问被引用节点前，调整缩进前缀指向引用目标
   * @param referred 被引用的INode节点
   */
  public void preVisitReferred(INode referred) {
    prefix.setLength(prefix.length() - 2);
    prefix.append("  ->");
  }

  @Override
  /**
   * 访问被引用节点后，恢复缩进前缀
   * @param referred 被引用的INode节点
   */
  public void postVisitReferred(INode referred) {
    prefix.setLength(prefix.length() - 2);
  }

  @Override
  /**
   * 访问目录节点时，打印目录基础信息、配额、快照等属性
   * @param dir 待访问的目录INode
   * @param snapshot 快照ID
   */
  public void visitDirectory(INodeDirectory dir, int snapshot) {
    printINode(dir, snapshot);

    // 输出子节点数量
    out.print(", childrenSize=" + dir.getChildrenList(snapshot).size());
    // 如果有配额特性，输出配额信息
    final DirectoryWithQuotaFeature q = dir.getDirectoryWithQuotaFeature();
    if (q != null) {
      out.print(", " + q);
    }
    // 如果是快照根目录，输出快照ID
    if (dir instanceof Snapshot.Root) {
      out.print(", snapshotId=" + snapshot);
    }
    out.println();

    // 调整缩进前缀，为子节点做准备
    if (prefix.length() >= 2) {
      prefix.setLength(prefix.length() - 2);
      prefix.append("  ");
    }

    // 如果目录包含快照特性，输出快照特性信息
    final DirectoryWithSnapshotFeature snapshotFeature
        = dir.getDirectoryWithSnapshotFeature();
    if (snapshotFeature != null) {
      out.print(prefix);
      out.print(snapshotFeature);
    }
    out.println();
  }

  @Override
  /**
   * 访问可快照目录时，打印快照数量和配额信息
   * @param dir 可快照目录INode
   * @param snapshottable 可快照特性对象
   */
  public void visitSnapshottable(INodeDirectory dir,
      DirectorySnapshottableFeature snapshottable) {
    out.println();
    out.print(prefix);

    out.print("Snapshot of ");
    // 处理根目录名称为空的情况
    final String name = dir.getLocalName();
    out.print(name != null && name.isEmpty()? "/": name);
    out.print(": quota=");
    out.print(snapshottable.getSnapshotQuota());

    // 统计快照根节点数量
    int n = 0;
    for(DirectoryDiff diff : snapshottable.getDiffs()) {
      if (diff.isSnapshotRoot()) {
        n++;
      }
    }
    // 校验统计结果与实际快照数量一致
    final int numSnapshots = snapshottable.getNumSnapshots();
    Preconditions.checkState(n == numSnapshots,
        "numSnapshots = " + numSnapshots + " != " + n);
    out.print(", #snapshot=");
    out.println(n);
  }

  @Override
  /**
   * 访问子节点前，添加对应位置前缀标记（非最后/最后节点）
   * @param sub 子节点元素
   * @param index 子节点索引
   * @param isLast 是否为当前目录最后一个子节点
   */
  public void preVisitSub(Element sub, int index, boolean isLast) {
    prefix.append(isLast? LAST_ITEM : NON_LAST_ITEM);
  }

  @Override
  /**
   * 访问子节点后，移除前缀标记，恢复缩进
   * @param sub 子节点元素
   * @param index 子节点索引
   * @param isLast 是否为当前目录最后一个子节点
   */
  public void postVisitSub(Element sub, int index, boolean isLast) {
    prefix.setLength(prefix.length() - 2);
  }
}