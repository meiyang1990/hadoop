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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Date;
import java.util.Objects;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.namenode.AclFeature;
import org.apache.hadoop.hdfs.server.namenode.INodeDirectoryAttributes;
import org.apache.hadoop.hdfs.server.namenode.ContentSummaryComputationContext;
import org.apache.hadoop.hdfs.server.namenode.DirectoryWithQuotaFeature;
import org.apache.hadoop.hdfs.server.namenode.FSImageFormat;
import org.apache.hadoop.hdfs.server.namenode.FSImageSerialization;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeDirectory;
import org.apache.hadoop.hdfs.server.namenode.QuotaCounts;
import org.apache.hadoop.hdfs.server.namenode.XAttrFeature;
import org.apache.hadoop.hdfs.util.ReadOnlyList;

import org.apache.hadoop.security.AccessControlException;

import static org.apache.hadoop.hdfs.server.common.HdfsServerConstants.XATTR_SNAPSHOT_DELETED;

/**
 * 文件级注释：HDFS快照元数据实现，保存一个快照目录子树的完整快照信息
 * 
 * Snapshot of a sub-tree in the namesystem.
 */
@InterfaceAudience.Private
public class Snapshot implements Comparable<byte[]> {
  /**
   * 当前状态ID，用于表示非快照的当前文件系统状态
   * This id is used to indicate the current state (vs. snapshots)
   */
  public static final int CURRENT_STATE_ID = Integer.MAX_VALUE - 1;
  /** 表示不存在快照的ID */
  public static final int NO_SNAPSHOT_ID = -1;
  
  /**
   * 默认快照名称的生成格式，示例：s20130412-151029.033
   * The pattern for generating the default snapshot name.
   * E.g. s20130412-151029.033
   */
  private static final String DEFAULT_SNAPSHOT_NAME_PATTERN = "'s'yyyyMMdd-HHmmss.SSS";
  
  /**
   * 生成默认格式的快照名称，基于当前时间生成
   * @return 默认格式的快照名称字符串
   */
  public static String generateDefaultSnapshotName() {
    return new SimpleDateFormat(DEFAULT_SNAPSHOT_NAME_PATTERN).format(new Date());
  }

  /**
   * 生成已删除快照的名称，在原名称后附加快照ID
   * @param s 被删除的快照对象
   * @return 带ID后缀的已删除快照名称
   */
  public static String generateDeletedSnapshotName(Snapshot s) {
    return getSnapshotName(s) + "#" + s.getId();
  }

  /**
   * 根据可快照目录和快照相对路径拼接生成完整快照路径
   * @param snapshottableDir 可快照目录路径
   * @param snapshotRelativePath 快照相对路径
   * @return 完整的快照路径字符串
   */
  public static String getSnapshotPath(String snapshottableDir,
      String snapshotRelativePath) {
    final StringBuilder b = new StringBuilder(snapshottableDir);
    // 确保路径末尾有分隔符
    if (b.charAt(b.length() - 1) != Path.SEPARATOR_CHAR) {
      b.append(Path.SEPARATOR);
    }
    return b.append(HdfsConstants.DOT_SNAPSHOT_DIR)
        .append(Path.SEPARATOR)
        .append(snapshotRelativePath)
        .toString();
  }
  
  /**
   * 获取给定快照的名称
   * @param s 待获取名称的快照对象
   * @return 快照名称，如果输入为null则返回空字符串
   */
  static String getSnapshotName(Snapshot s) {
    return s != null ? s.getRoot().getLocalName() : "";
  }
  
  /**
   * 获取给定快照的ID，如果输入为null返回当前状态ID
   * @param s 待获取ID的快照对象
   * @return 快照ID，输入为null返回CURRENT_STATE_ID
   */
  public static int getSnapshotId(Snapshot s) {
    return s == null ? CURRENT_STATE_ID : s.getId();
  }

  /**
   * 根据快照ID生成快照状态的可读字符串
   * @param snapshot 快照ID
   * @return 对应状态的可读描述字符串
   */
  public static String getSnapshotString(int snapshot) {
    return snapshot == CURRENT_STATE_ID? "<CURRENT_STATE>"
        : snapshot == NO_SNAPSHOT_ID? "<NO_SNAPSHOT>"
        : "Snapshot #" + snapshot;
  }

  /**
   * 快照ID比较器，null（当前状态）比任何非null快照大
   * Compare snapshot with IDs, where null indicates the current status thus
   * is greater than any non-null snapshot.
   */
  public static final Comparator<Snapshot> ID_COMPARATOR
      = new Comparator<Snapshot>() {
    @Override
    public int compare(Snapshot left, Snapshot right) {
      return ID_INTEGER_COMPARATOR.compare(Snapshot.getSnapshotId(left),
          Snapshot.getSnapshotId(right));
    }
  };

  /**
   * 整数快照ID比较器，CURRENT_STATE_ID为最大值
   * Compare snapshot with IDs, where null indicates the current status thus
   * is greater than any non-null ID.
   */
  public static final Comparator<Integer> ID_INTEGER_COMPARATOR
      = new Comparator<Integer>() {
    @Override
    public int compare(Integer left, Integer right) {
      // Snapshot.CURRENT_STATE_ID means the current state, thus should be the 
      // largest
      return left - right;
    }
  };

  /**
   * 查找最晚创建、且早于给定锚点ID、并覆盖给定inode的快照
   * 
   * @param inode 需要被快照覆盖的inode
   * @param anchor 锚点ID，返回的快照需要创建于此ID之前
   * @return 符合条件的最晚快照ID，无符合条件返回NO_SNAPSHOT_ID
   */
  public static int findLatestSnapshot(INode inode, final int anchor) {
    int latest = NO_SNAPSHOT_ID;
    // 从当前inode向上遍历父目录查找所有包含快照的目录
    for(; inode != null; inode = inode.getParent()) {
      if (inode.isDirectory()) {
        final INodeDirectory dir = inode.asDirectory();
        if (dir.isWithSnapshot()) {
          // 更新找到的最大符合条件的快照ID
          latest = dir.getDiffs().updatePrior(anchor, latest);
        }
      }
    }
    return latest;
  }
  
  /**
   * 从FSImage输入流反序列化读取快照对象
   * @param in FSImage输入流
   * @param loader FSImage加载器
   * @return 反序列化得到的快照对象
   * @throws IO异常
   */
  static Snapshot read(DataInput in, FSImageFormat.Loader loader)
      throws IOException {
    final int snapshotId = in.readInt();
    final INode root = loader.loadINodeWithLocalName(false, in, false);
    return new Snapshot(snapshotId, root.asDirectory(), null);
  }

  /**
   * 快照根目录类，保存快照对应可快照目录的元数据拷贝
   * The root directory of the snapshot.
   */
  static public class Root extends INodeDirectory {
    /**
     * 从原有目录构造快照根目录，仅保留ACL、扩展属性和配额特性
     * @param other 原可快照目录
     */
    Root(INodeDirectory other) {
      // Always preserve ACL, XAttr and Quota.
      super(other, false,
          Arrays.stream(other.getFeatures()).filter(feature ->
              feature instanceof AclFeature
                  || feature instanceof XAttrFeature
                  || feature instanceof DirectoryWithQuotaFeature
          ).map(feature -> {
            // 配额特性需要拷贝，避免原目录更新影响快照
            if (feature instanceof DirectoryWithQuotaFeature) {
              // Return copy if feature is quota because a ref could be updated
              final QuotaCounts quota =
                  ((DirectoryWithQuotaFeature) feature).getSpaceAllowed();
              return new DirectoryWithQuotaFeature.Builder()
                  .nameSpaceQuota(quota.getNameSpace())
                  .storageSpaceQuota(quota.getStorageSpace())
                  .typeQuotas(quota.getTypeSpaces())
                  .build();
            } else {
              // 不可变特性直接复用引用
              return feature;
            }
          }).toArray(Feature[]::new));
    }

    /**
     * 检查该快照根是否已被标记为删除
     * @return 是否为已删除快照
     */
    boolean isMarkedAsDeleted() {
      final XAttrFeature f = getXAttrFeature();
      return f != null && f.getXAttr(XATTR_SNAPSHOT_DELETED) != null;
    }

    @Override
    public ReadOnlyList<INode> getChildrenList(int snapshotId) {
      // 委托给父目录获取对应快照的子节点列表
      return getParent().getChildrenList(snapshotId);
    }

    @Override
    public INode getChild(byte[] name, int snapshotId) {
      // 委托给父目录获取对应快照的子节点
      return getParent().getChild(name, snapshotId);
    }

    @Override
    public ContentSummaryComputationContext computeContentSummary(
        int snapshotId, ContentSummaryComputationContext summary)
        throws AccessControlException {
      // 计算快照目录的内容汇总
      return computeDirectoryContentSummary(summary, snapshotId);
    }

    @Override
    public boolean metadataEquals(INodeDirectoryAttributes other) {
      // 比较元数据是否相等，ACL直接比较引用因为无修改时复用同一对象
      return other != null && getQuotaCounts().equals(other.getQuotaCounts())
          && getPermissionLong() == other.getPermissionLong()
          // Acl feature maintains a reference counted map, thereby
          // every snapshot copy should point to the same Acl object unless
          // there is no change in acl values.
          // Reference equals is hence intentional here.
          && getAclFeature() == other.getAclFeature()
          && Objects.equals(getXAttrFeature(), other.getXAttrFeature());
    }

    @Override
    public String getFullPathName() {
      // 拼接生成快照根目录在/.snapshot下的完整路径
      return getSnapshotPath(getParent().getFullPathName(), getLocalName());
    }

    /**
     * 获取原可快照目录的完整路径（即快照根对应的原始目录路径）
     * @return 原可快照目录的完整路径
     */
    public String getRootFullPathName() {
      return getParent().getFullPathName();
    }
  }

  /** 快照唯一ID */
  private final int id;
  /** 快照根目录 */
  private final Root root;

  /**
   * 构造快照对象，指定ID、名称和原始目录
   * @param id 快照ID
   * @param name 快照名称
   * @param dir 原始可快照目录
   */
  Snapshot(int id, String name, INodeDirectory dir) {
    this(id, dir, dir);
    this.root.setLocalName(DFSUtil.string2Bytes(name));
  }

  /**
   * 构造快照对象，指定ID、原始目录和父目录
   * @param id 快照ID
   * @param dir 原始可快照目录
   * @param parent 父目录
   */
  Snapshot(int id, INodeDirectory dir, INodeDirectory parent) {
    this.id = id;
    this.root = new Root(dir);
    this.root.setParent(parent);
  }
  
  /**
   * 获取快照ID
   * @return 快照唯一ID
   */
  public int getId() {
    return id;
  }

  /**
   * 获取快照根目录
   * @return 快照根目录对象
   */
  public Root getRoot() {
    return root;
  }

  @Override
  public int compareTo(byte[] bytes) {
    return root.compareTo(bytes);
  }
  
  @Override
  public boolean equals(Object that) {
    if (this == that) {
      return true;
    } else if (!(that instanceof Snapshot)) {
      return false;
    }
    // 快照ID唯一标识快照，仅比较ID
    return this.id == ((Snapshot)that).id;
  }
  
  @Override
  public int hashCode() {
    // hashCode直接使用ID
    return id;
  }
  
  @Override
  public String toString() {
    return getClass().getSimpleName() + "." + root.getLocalName() + "(id=" + id + ")";
  }

  /**
   * 将快照序列化到FSImage输出流
   * @param out FSImage输出流
   * @throws IO异常
   */
  void write(DataOutput out) throws IOException {
    out.writeInt(id);
    // write root
    FSImageSerialization.writeINodeDirectory(root, out);
  }
}