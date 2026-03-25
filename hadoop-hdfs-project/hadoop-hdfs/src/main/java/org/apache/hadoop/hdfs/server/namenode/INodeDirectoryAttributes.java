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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.util.EnumCounters;

import org.apache.hadoop.util.Preconditions;

/**
 * HDFS目录INode的属性接口，定义目录节点需要提供的属性能力
 * 继承自INodeAttributes，扩展了配额相关的属性方法，用于支持目录配额管理
 */
@InterfaceAudience.Private
public interface INodeDirectoryAttributes extends INodeAttributes {
  /**
   * 获取目录的配额统计信息，包含命名空间、存储空间和存储类型配额
   * @return 目录当前的配额统计对象
   */
  public QuotaCounts getQuotaCounts();

  /**
   * 比较当前目录与另一个目录属性的元数据是否相等
   * @param other 待比较的另一个目录属性对象
   * @return 元数据相等返回true，否则返回false
   */
  public boolean metadataEquals(INodeDirectoryAttributes other);
  
  /**
   * 目录属性的快照副本实现类，用于保存目录属性的快照版本
   * 仅保留基础目录属性，默认不保存实际配额信息，配额返回非法值标识
   */
  public static class SnapshotCopy extends INodeAttributes.SnapshotCopy
      implements INodeDirectoryAttributes {
    public SnapshotCopy(byte[] name, PermissionStatus permissions,
        AclFeature aclFeature, long modificationTime, 
        XAttrFeature xAttrsFeature) {
      super(name, permissions, aclFeature, modificationTime, 0L, xAttrsFeature);
    }

    public SnapshotCopy(INodeDirectory dir) {
      super(dir);
    }

    @Override
    public QuotaCounts getQuotaCounts() {
      return new QuotaCounts.Builder().nameSpace(-1).
          storageSpace(-1).typeSpaces(-1).build();
    }

    public boolean isDirectory() {
      return true;
    }

    @Override
    public boolean metadataEquals(INodeDirectoryAttributes other) {
      return other != null
          && getQuotaCounts().equals(other.getQuotaCounts())
          && getPermissionLong() == other.getPermissionLong()
          && getAclFeature() == other.getAclFeature()
          && getXAttrFeature() == other.getXAttrFeature();
    }
  }

  /**
   * 带配额信息的目录属性副本实现类，用于保存包含实际配额的目录属性副本
   * 继承SnapshotCopy，额外存储目录的配额信息，支持配额持久化与恢复
   */
  public static class CopyWithQuota extends INodeDirectoryAttributes.SnapshotCopy {
    // 存储目录配额信息
    private QuotaCounts quota;

    public CopyWithQuota(byte[] name, PermissionStatus permissions,
        AclFeature aclFeature, long modificationTime, long nsQuota,
        long dsQuota, EnumCounters<StorageType> typeQuotas, XAttrFeature xAttrsFeature) {
      super(name, permissions, aclFeature, modificationTime, xAttrsFeature);
      // 根据传入的配额参数构建配额统计对象
      this.quota = new QuotaCounts.Builder().nameSpace(nsQuota).
          storageSpace(dsQuota).typeSpaces(typeQuotas).build();
    }

    public CopyWithQuota(INodeDirectory dir) {
      super(dir);
      // 校验传入目录已经设置了配额
      Preconditions.checkArgument(dir.isQuotaSet());
      final QuotaCounts q = dir.getQuotaCounts();
      // 从原目录复制配额信息构建新的配额统计对象
      this.quota = new QuotaCounts.Builder().quotaCount(q).build();
    }

    @Override
    public QuotaCounts getQuotaCounts() {
      // 返回配额对象的深度拷贝，避免外部修改内部状态
      return new QuotaCounts.Builder().quotaCount(quota).build();
    }
  }
}