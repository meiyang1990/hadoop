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

package org.apache.hadoop.hdfs.server.namenode;

import java.util.Arrays;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.hdfs.util.ReferenceCountMap.ReferenceCounter;

import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableList;

/**
 * 文件节点ACL访问控制列表功能类，作为INode的扩展特性存储访问控制规则
 * 存储编码后的ACL条目，支持引用计数实现共享复用
 */
@InterfaceAudience.Private
public class AclFeature implements INode.Feature, ReferenceCounter {
  public static final ImmutableList<AclEntry> EMPTY_ENTRY_LIST =
    ImmutableList.of();
  private int refCount = 0;

  // 存储编码后的ACL条目数组，每个整数代表一个ACL条目
  private final int [] entries;

  /**
   * 构造AclFeature，使用给定的编码后ACL条目数组初始化
   * @param entries 编码后的ACL条目数组
   */
  public AclFeature(int[] entries) {
    this.entries = entries;
  }

  /**
   * 获取当前ACL包含的条目总数
   * @return ACL条目数量
   */
  int getEntriesSize() {
    return entries.length;
  }

  /**
   * 获取指定位置的编码后ACL条目
   * @param pos 要获取的条目的索引位置
   * @return 整数编码表示的AclEntry
   * @throws IndexOutOfBoundsException 如果索引超出范围
   */
  int getEntryAt(int pos) {
    if (pos < 0 || pos > entries.length) {
      throw new IndexOutOfBoundsException("Invalid position for AclEntry");
    }
    return entries[pos];
  }

  @Override
  public boolean equals(Object o) {
    if (o == null) {
      return false;
    }
    if (getClass() != o.getClass()) {
      return false;
    }
    // 按数组内容比较两个AclFeature是否相等
    return Arrays.equals(entries, ((AclFeature) o).entries);
  }

  @Override
  public String toString() {
    // 输出AclFeature的哈希码和条目数量信息
    return "AclFeature : " + Integer.toHexString(hashCode()) + " Size of entries : " + entries.length;
  }

  @Override
  public int hashCode() {
    // 基于条目数组计算哈希码
    return Arrays.hashCode(entries);
  }

  @Override
  public synchronized int getRefCount() {
    // 获取当前对象的引用计数
    return refCount;
  }

  @Override
  public synchronized int incrementAndGetRefCount() {
    // 引用计数自增后返回新值，用于引用新增共享
    return ++refCount;
  }

  @Override
  public synchronized int decrementAndGetRefCount() {
    // 引用计数自减后返回新值，引用计数不会小于0，用于释放共享
    return (refCount > 0) ? --refCount : 0;
  }
}