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
package org.apache.hadoop.hdfs.protocol;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.protocol.proto.AclProtos.FsPermissionProto;

/**
 * @fileoverview HDFS快照元信息容器类，用于在客户端和服务端之间传输快照基本信息
 * HDFS快照功能中保存单个快照的基础元数据，包含名称、根目录、创建时间、权限信息等
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class SnapshotInfo {
  private final String snapshotName;
  private final String snapshotRoot;
  private final String createTime;
  private final FsPermissionProto permission;
  private final String owner;
  private final String group;

  /**
   * 构造快照元信息对象
   * @param sname 快照名称
   * @param sroot 快照根目录路径
   * @param ctime 快照创建时间字符串
   * @param permission 快照权限信息Protobuf对象
   * @param owner 快照所有者用户名
   * @param group 快照所属用户组名称
   */
  public SnapshotInfo(String sname, String sroot, String ctime,
      FsPermissionProto permission, String owner, String group) {
    this.snapshotName = sname;
    this.snapshotRoot = sroot;
    this.createTime = ctime;
    this.permission = permission;
    this.owner = owner;
    this.group = group;
  }

  /**
   * 获取快照名称
   * @return 快照名称字符串
   */
  final public String getSnapshotName() {
    return snapshotName;
  }

  /**
   * 获取快照根目录路径
   * @return 快照根目录路径字符串
   */
  final public String getSnapshotRoot() {
    return snapshotRoot;
  }

  /**
   * 获取快照创建时间
   * @return 创建时间字符串
   */
  final public String getCreateTime() {
    return createTime;
  }
  
  /**
   * 获取快照权限信息
   * @return 文件系统权限Protobuf对象
   */
  final public FsPermissionProto getPermission() {
    return permission;
  }
  
  /**
   * 获取快照所有者用户名
   * @return 所有者用户名
   */
  final public String getOwner() {
    return owner;
  }
  
  /**
   * 获取快照所属用户组名称
   * @return 用户组名称
   */
  final public String getGroup() {
    return group;
  }
  
  @Override
  public String toString() {
    return getClass().getSimpleName()
        + "{snapshotName=" + snapshotName
        + "; snapshotRoot=" + snapshotRoot
        + "; createTime=" + createTime
        + "; permission=" + permission
        + "; owner=" + owner
        + "; group=" + group
        + "}";
  }

  /**
   * 快照摘要信息Java Bean，用于对外展示轻量级快照列表信息
   * 包含快照ID、目录路径、修改时间和状态等基础信息
   */
  public static class Bean {
    private final int snapshotID;
    private final String snapshotDirectory;
    private final long modificationTime;
    private final String status;

    /**
     * 构造快照摘要Bean对象
     * @param snapshotID 快照唯一ID
     * @param snapshotDirectory 快照目录路径
     * @param modificationTime 快照修改时间戳
     * @param isMarkedAsDeleted 快照是否已被标记删除
     */
    public Bean(int snapshotID, String snapshotDirectory,
        long modificationTime, boolean isMarkedAsDeleted) {
      this.snapshotID = snapshotID;
      this.snapshotDirectory = snapshotDirectory;
      this.modificationTime = modificationTime;
      this.status = isMarkedAsDeleted ? "DELETED" : "ACTIVE";
    }

    /**
     * 获取快照ID
     * @return 快照唯一ID
     */
    public int getSnapshotID() {
      return snapshotID;
    }

    /**
     * 获取快照目录路径
     * @return 快照目录路径字符串
     */
    public String getSnapshotDirectory() {
      return snapshotDirectory;
    }

    /**
     * 获取快照修改时间戳
     * @return 修改时间戳（毫秒）
     */
    public long getModificationTime() {
      return modificationTime;
    }

    /**
     * 获取快照状态
     * @return 状态字符串：ACTIVE（活跃）或DELETED（已删除）
     */
    public String getStatus() {
      return status;
    }
  }
}