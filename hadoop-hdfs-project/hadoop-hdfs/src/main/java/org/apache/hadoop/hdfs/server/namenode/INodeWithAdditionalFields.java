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
import org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot;
import org.apache.hadoop.hdfs.util.LongBitFormat;
import org.apache.hadoop.util.LightWeightGSet.LinkedElement;

import org.apache.hadoop.util.Preconditions;

/**
 * 文件元数据inode基类，扩展基础INode，增加id、名称、权限、访问时间、修改时间等扩展字段
 * 是文件和目录inode的共同父类，同时实现LinkedElement接口支持轻量级哈希表存储
 */
@InterfaceAudience.Private
public abstract class INodeWithAdditionalFields extends INode
    implements LinkedElement {
  // Note: this format is used both in-memory and on-disk.  Changes will be
  // incompatible.
  /**
   * 权限状态比特位编码枚举，将用户、用户组、权限模式编码到一个long类型中
   * 用于节省内存和磁盘存储空间，该格式同时用于内存和磁盘，修改会导致不兼容
   */
  enum PermissionStatusFormat implements LongBitFormat.Enum {
    MODE(null, 16),
    GROUP(MODE.BITS, 24),
    USER(GROUP.BITS, 24);

    final LongBitFormat BITS;

    private PermissionStatusFormat(LongBitFormat previous, int length) {
      BITS = new LongBitFormat(name(), previous, length, 0);
    }

    /**
     * 从编码后的权限长整型中解析出用户名
     * @param permission 编码后的权限长整型
     * @return 用户名字符串
     */
    static String getUser(long permission) {
      final int n = (int)USER.BITS.retrieve(permission);
      String s = SerialNumberManager.USER.getString(n);
      assert s != null;
      return s;
    }

    /**
     * 从编码后的权限长整型中解析出用户组名
     * @param permission 编码后的权限长整型
     * @return 用户组名字符串
     */
    static String getGroup(long permission) {
      final int n = (int)GROUP.BITS.retrieve(permission);
      return SerialNumberManager.GROUP.getString(n);
    }
    
    /**
     * 从编码后的权限长整型中解析出权限模式
     * @param permission 编码后的权限长整型
     * @return 权限模式短整型
     */
    static short getMode(long permission) {
      return (short)MODE.BITS.retrieve(permission);
    }

    /** 
     * 将PermissionStatus对象编码为长整型
     * @param ps 权限状态对象
     * @return 编码后的长整型
     */
    static long toLong(PermissionStatus ps) {
      long permission = 0L;
      final int user = SerialNumberManager.USER.getSerialNumber(
          ps.getUserName());
      assert user != 0;
      permission = USER.BITS.combine(user, permission);
      // ideally should assert on group but inodes are created with null
      // group and then updated only when added to a directory.
      final int group = SerialNumberManager.GROUP.getSerialNumber(
          ps.getGroupName());
      permission = GROUP.BITS.combine(group, permission);
      final int mode = ps.getPermission().toShort();
      permission = MODE.BITS.combine(mode, permission);
      return permission;
    }

    /**
     * 将编码后的长整型解码为PermissionStatus对象
     * @param id 编码后的权限长整型
     * @param stringTable 字符串表用于序列号反查
     * @return 解码后的权限状态对象
     */
    static PermissionStatus toPermissionStatus(long id,
        SerialNumberManager.StringTable stringTable) {
      int uid = (int)USER.BITS.retrieve(id);
      int gid = (int)GROUP.BITS.retrieve(id);
      return new PermissionStatus(
          SerialNumberManager.USER.getString(uid, stringTable),
          SerialNumberManager.GROUP.getString(gid, stringTable),
          new FsPermission(getMode(id)));
    }

    @Override
    public int getLength() {
      return BITS.getLength();
    }
  }

  /** inode全局唯一ID */
  final private long id;
  /**
   * inode名称字节数组，使用Java UTF8编码
   * 客户端协议返回的名称编码需要保持与此一致，修改编码需要同步修改客户端解码逻辑
   */
  private byte[] name = null;
  /** 
   * 编码后的权限信息，使用PermissionStatusFormat编码
   * 除clonePermissionStatus和updatePermissionStatus方法外，其他代码不应直接修改
   */
  private long permission = 0L;
  /** 最后修改时间戳 */
  private long modificationTime = 0L;
  /** 最后访问时间戳 */
  private long accessTime = 0L;

  /** 用于LinkedElement实现，链表下一个节点引用 */
  private LinkedElement next = null;
  /** 空特性数组默认值 */
  private static final Feature[] EMPTY_FEATURE = new Feature[0];
  /** inode扩展特性数组，用于存储ACL、XAttr等可选扩展特性 */
  protected Feature[] features = EMPTY_FEATURE;

  /**
   * 私有构造函数，全参数构造inode对象
   * @param parent 父inode引用
   * @param id inode全局ID
   * @param name inode名称字节数组
   * @param permission 编码后的权限长整型
   * @param modificationTime 最后修改时间
   * @param accessTime 最后访问时间
   */
  private INodeWithAdditionalFields(INode parent, long id, byte[] name,
      long permission, long modificationTime, long accessTime) {
    super(parent);
    this.id = id;
    this.name = name;
    this.permission = permission;
    this.modificationTime = modificationTime;
    this.accessTime = accessTime;
  }

  /**
   * 构造函数，从权限状态对象创建新的inode
   * @param id inode全局ID
   * @param name inode名称字节数组
   * @param permissions 权限状态对象
   * @param modificationTime 最后修改时间
   * @param accessTime 最后访问时间
   */
  INodeWithAdditionalFields(long id, byte[] name, PermissionStatus permissions,
      long modificationTime, long accessTime) {
    this(null, id, name, PermissionStatusFormat.toLong(permissions),
        modificationTime, accessTime);
  }
  
  /**
   * 拷贝构造函数，从另一个inode复制所有字段创建新inode
   * @param other 待拷贝的源inode
   */
  INodeWithAdditionalFields(INodeWithAdditionalFields other) {
    this(other.getParentReference() != null ? other.getParentReference()
        : other.getParent(), other.getId(), other.getLocalNameBytes(),
        other.permission, other.modificationTime, other.accessTime);
  }

  @Override
  public void setNext(LinkedElement next) {
    this.next = next;
  }
  
  @Override
  public LinkedElement getNext() {
    return next;
  }

  /**
   * 获取inode全局ID
   * @return inode全局ID
   */
  @Override
  public final long getId() {
    return this.id;
  }

  @Override
  public final byte[] getLocalNameBytes() {
    return name;
  }
  
  @Override
  public final void setLocalName(byte[] name) {
    this.name = name;
  }

  /**
   * 克隆另一个inode的权限信息到当前inode
   * @param that 源inode
   */
  final void clonePermissionStatus(INodeWithAdditionalFields that) {
    this.permission = that.permission;
  }

  @Override
  public final PermissionStatus getPermissionStatus(int snapshotId) {
    if (snapshotId != Snapshot.CURRENT_STATE_ID) {
      return getSnapshotINode(snapshotId).getPermissionStatus();
    }
    return new PermissionStatus(getUserName(snapshotId), getGroupName(snapshotId),
        getFsPermission(snapshotId));
  }

  /**
   * 更新权限编码中对应字段的值
   * @param f 比特段格式
   * @param n 新值
   */
  private final void updatePermissionStatus(PermissionStatusFormat f, long n) {
    this.permission = f.BITS.combine(n, permission);
  }

  @Override
  final String getUserName(int snapshotId) {
    if (snapshotId != Snapshot.CURRENT_STATE_ID) {
      return getSnapshotINode(snapshotId).getUserName();
    }
    return PermissionStatusFormat.getUser(permission);
  }

  @Override
  final void setUser(String user) {
    int n = SerialNumberManager.USER.getSerialNumber(user);
    updatePermissionStatus(PermissionStatusFormat.USER, n);
  }

  @Override
  final String getGroupName(int snapshotId) {
    if (snapshotId != Snapshot.CURRENT_STATE_ID) {
      return getSnapshotINode(snapshotId).getGroupName();
    }
    return PermissionStatusFormat.getGroup(permission);
  }

  @Override
  final void setGroup(String group) {
    int n = SerialNumberManager.GROUP.getSerialNumber(group);
    updatePermissionStatus(PermissionStatusFormat.GROUP, n);
  }

  @Override
  final FsPermission getFsPermission(int snapshotId) {
    if (snapshotId != Snapshot.CURRENT_STATE_ID) {
      return getSnapshotINode(snapshotId).getFsPermission();
    }

    return new FsPermission(getFsPermissionShort());
  }

  @Override
  public final short getFsPermissionShort() {
    return PermissionStatusFormat.getMode(permission);
  }

  @Override
  void setPermission(FsPermission permission) {
    final short mode = permission.toShort();
    updatePermissionStatus(PermissionStatusFormat.MODE, mode);
  }

  @Override
  public long getPermissionLong() {
    return permission;
  }

  @Override
  public final AclFeature getAclFeature(int snapshotId) {
    if (snapshotId != Snapshot.CURRENT_STATE_ID) {
      return getSnapshotINode(snapshotId).getAclFeature();
    }

    return getFeature(AclFeature.class);
  }

  @Override
  final long getModificationTime(int snapshotId) {
    if (snapshotId != Snapshot.CURRENT_STATE_ID) {
      return getSnapshotINode(snapshotId).getModificationTime();
    }

    return this.modificationTime;
  }


  /**
   * 仅当新修改时间大于当前值时更新目录inode的修改时间
   * @param mtime 新修改时间
   * @param latestSnapshotId 最新快照ID
   * @return 更新后的inode对象
   */
  @Override
  public final INode updateModificationTime(long mtime, int latestSnapshotId) {
    Preconditions.checkState(isDirectory());
    if (mtime <= modificationTime) {
      return this;
    }
    return setModificationTime(mtime, latestSnapshotId);
  }

  /**
   * 克隆另一个inode的修改时间到当前inode
   * @param that 源inode
   */
  final void cloneModificationTime(INodeWithAdditionalFields that) {
    this.modificationTime = that.modificationTime;
  }

  @Override
  public final void setModificationTime(long modificationTime) {
    this.modificationTime = modificationTime;
  }

  @Override
  final long getAccessTime(int snapshotId) {
    if (snapshotId != Snapshot.CURRENT_STATE_ID) {
      return getSnapshotINode(snapshotId).getAccessTime();
    }
    return accessTime;
  }

  /**
   * 设置inode最后访问时间
   * @param accessTime 新访问时间戳
   */
  @Override
  public final void setAccessTime(long accessTime) {
    this.accessTime = accessTime;
  }

  /**
   * 添加扩展特性到当前inode
   * @param f 待添加的特性对象
   */
  protected void addFeature(Feature f) {
    int size = features.length;
    Feature[] arr = new Feature[size + 1];
    if (size != 0) {
      System.arraycopy(features, 0, arr, 0, size);
    }
    arr[size] = f;
    features = arr;
  }

  /**
   * 从当前inode移除指定扩展特性
   * @param f 待移除的特性对象
   */
  protected void removeFeature(Feature f) {
    int size = features.length;
    if (size == 0) {
      throwFeatureNotFoundException(f);
    }

    if (size == 1) {
      if (features[0] != f) {
        throwFeatureNotFoundException(f);
      }
      features = EMPTY_FEATURE;
      return;
    }

    Feature[] arr = new Feature[size - 1];
    int j = 0;
    boolean overflow = false;
    for (Feature f1 : features) {
      if (f1 != f) {
        if (j == size - 1) {
          overflow = true;
          break;
        } else {
          arr[j++] = f1;
        }
      }
    }

    if (overflow || j != size - 1) {
      throwFeatureNotFoundException(f);
    }
    features = arr;
  }

  /**
   * 抛出特性不存在异常
   * @param f 未找到的特性对象
   */
  private void throwFeatureNotFoundException(Feature f) {
    throw new IllegalStateException(
        "Feature " + f.getClass().getSimpleName() + " not found.");
  }

  /**
   * 根据类型获取指定扩展特性
   * @param clazz 特性类型类对象
   * @return 匹配的特性对象，未找到返回null
   */
  protected <T extends Feature> T getFeature(Class<? extends Feature> clazz) {
    Preconditions.checkArgument(clazz != null);
    final int size = features.length;
    for (int i=0; i < size; i++) {
      Feature f = features[i];
      if (clazz.isAssignableFrom(f.getClass())) {
        @SuppressWarnings("unchecked")
        T ret = (T) f;
        return ret;
      }
    }
    return null;
  }

  /**
   * 移除当前inode的ACL特性
   */
  public void removeAclFeature() {
    AclFeature f = getAclFeature();
    Preconditions.checkNotNull(f);
    removeFeature(f);
    AclStorage.removeAclFeature(f);
  }

  /**
   * 添加ACL特性到当前inode
   * @param f 待添加的ACL特性对象
   */
  public void addAclFeature(AclFeature f) {
    AclFeature f1 = getAclFeature();
    if (f1 != null)
      throw new IllegalStateException("Duplicated ACLFeature");

    addFeature(AclStorage.addAclFeature(f));
  }
  
  @Override
  XAttrFeature getXAttrFeature(int snapshotId) {
    if (snapshotId != Snapshot.CURRENT_STATE_ID) {
      return getSnapshotINode(snapshotId).getXAttrFeature();
    }

    return getFeature(XAttrFeature.class);
  }
  
  @Override
  public void removeXAttrFeature() {
    XAttrFeature f = getXAttrFeature();
    Preconditions.checkNotNull(f);
    removeFeature(f);
  }
  
  @Override
  public void addXAttrFeature(XAttrFeature f) {
    XAttrFeature f1 = getXAttrFeature();
    Preconditions.checkState(f1 == null, "Duplicated XAttrFeature");
    
    addFeature(f);
  }

  /**
   * 获取当前inode所有扩展特性数组
   * @return 扩展特性数组
   */
  public final Feature[] getFeatures() {
    return features;
  }
}