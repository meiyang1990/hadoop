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
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.server.namenode.INodeWithAdditionalFields.PermissionStatusFormat;
import org.apache.hadoop.hdfs.server.namenode.XAttrFeature;

/**
 * 文件系统INode属性接口，定义了INode必须提供的基础属性访问方法。
 * 包含文件/目录类型、名称、权限、时间戳、扩展属性等核心元数据的访问契约。
 */
@InterfaceAudience.Private
public interface INodeAttributes {

  /**
   * 判断当前INode是否为目录类型。
   * @return true表示是目录，false表示是文件
   */
  public boolean isDirectory();

  /**
   * 获取INode本地名称的字节数组形式。
   * @return null如果本地名称为空，否则返回本地名称字节数组
   */
  public byte[] getLocalNameBytes();

  /**
   * 获取INode所属用户名。
   * @return 用户名字符串
   */
  public String getUserName();

  /**
   * 获取INode所属组名。
   * @return 组名字符串
   */
  public String getGroupName();
  
  /**
   * 获取INode的文件权限对象。
   * @return 文件权限对象
   */
  public FsPermission getFsPermission();

  /**
   * 获取INode的文件权限短整型表示。
   * @return 短整型格式的文件权限
   */
  public short getFsPermissionShort();
  
  /**
   * 获取INode权限信息的长整型打包格式。
   * @return 长整型打包的用户、组、权限信息
   */
  public long getPermissionLong();

  /**
   * 获取INode的ACL特性对象。
   * @return ACL特性对象，无ACL则返回null
   */
  public AclFeature getAclFeature();
  
  /**
   * 获取INode的扩展属性特性对象。
   * @return 扩展属性特性对象，无扩展属性则返回null
   */
  public XAttrFeature getXAttrFeature();

  /**
   * 获取INode的修改时间戳。
   * @return 修改时间（毫秒数）
   */
  public long getModificationTime();

  /**
   * 获取INode的访问时间戳。
   * @return 访问时间（毫秒数）
   */
  public long getAccessTime();

  /**
   * 快照使用的只读INode属性副本抽象类。
   * 用于保存INode在快照生成时刻的所有属性快照，实现快照的只读特性。
   * 所有属性均在构造时初始化，后续不可修改。
   */
  public static abstract class SnapshotCopy implements INodeAttributes {
    private final byte[] name;
    private final long permission;
    private final AclFeature aclFeature;
    private final long modificationTime;
    private final long accessTime;
    private XAttrFeature xAttrFeature;

    /**
     * 从分散的属性构造INode属性快照。
     * @param name INode名称字节数组
     * @param permissions 权限状态对象
     * @param aclFeature ACL特性对象
     * @param modificationTime 修改时间戳
     * @param accessTime 访问时间戳
     * @param xAttrFeature 扩展属性特性对象
     */
    SnapshotCopy(byte[] name, PermissionStatus permissions,
        AclFeature aclFeature, long modificationTime, long accessTime, 
        XAttrFeature xAttrFeature) {
      this.name = name;
      this.permission = PermissionStatusFormat.toLong(permissions);
      if (aclFeature != null) {
        aclFeature = AclStorage.addAclFeature(aclFeature);
      }
      this.aclFeature = aclFeature;
      this.modificationTime = modificationTime;
      this.accessTime = accessTime;
      this.xAttrFeature = xAttrFeature;
    }

    /**
     * 从现有INode对象构造属性快照。
     * @param inode 源INode对象，提取其所有属性生成只读快照
     */
    SnapshotCopy(INode inode) {
      this.name = inode.getLocalNameBytes();
      this.permission = inode.getPermissionLong();
      if (inode.getAclFeature() != null) {
        aclFeature = AclStorage.addAclFeature(inode.getAclFeature());
      } else {
        aclFeature = null;
      }
      this.modificationTime = inode.getModificationTime();
      this.accessTime = inode.getAccessTime();
      this.xAttrFeature = inode.getXAttrFeature();
    }

    @Override
    public final byte[] getLocalNameBytes() {
      return name;
    }

    @Override
    public final String getUserName() {
      return PermissionStatusFormat.getUser(permission);
    }

    @Override
    public final String getGroupName() {
      return PermissionStatusFormat.getGroup(permission);
    }

    @Override
    public final FsPermission getFsPermission() {
      return new FsPermission(getFsPermissionShort());
    }

    @Override
    public final short getFsPermissionShort() {
      return PermissionStatusFormat.getMode(permission);
    }
    
    @Override
    public long getPermissionLong() {
      return permission;
    }

    @Override
    public AclFeature getAclFeature() {
      return aclFeature;
    }

    @Override
    public final long getModificationTime() {
      return modificationTime;
    }

    @Override
    public final long getAccessTime() {
      return accessTime;
    }
    
    @Override
    public final XAttrFeature getXAttrFeature() {
      return xAttrFeature;
    }
  }
}