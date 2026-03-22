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

import java.util.Collections;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclEntryScope;
import org.apache.hadoop.fs.permission.AclEntryType;
import org.apache.hadoop.fs.permission.AclUtil;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.ScopedAclEntries;
import org.apache.hadoop.hdfs.protocol.AclException;
import org.apache.hadoop.hdfs.protocol.QuotaExceededException;
import org.apache.hadoop.hdfs.util.ReferenceCountMap;
import org.apache.hadoop.util.Lists;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableList;

/**
 * 文件系统ACL存储工具类，定义ACL数据在HDFS命名空间中的存储规则与读写方法
 * 
 * 如果一个inode开启了ACL，则会在inode的{@link FsPermission}中设置ACL标志位，同时inode包含一个{@link AclFeature}存储扩展ACL信息。
 * 对于访问ACL，所有者和其他用户条目与FsPermission中存储的权限位一致，因此直接复用。访问掩码条目存储在FsPermission的组权限位中，
 * 这与其他文件系统的ACL实现一致，避免了代码库中大量特殊处理逻辑。例如当用户对带有ACL的文件执行chmod修改组权限时，
 * 实际上会修改ACL的掩码条目，通过将掩码存储在组权限位，chmod无需特殊修改即可正确工作。
 * 其余访问条目（命名用户和命名组）和所有默认ACL条目都存储在AclFeature内部的列表中。
 * 
 * 本类封装了从正确位置读写ACL条目的所有规则，输入的ACL条目列表已经过{@link AclTransformation}验证和排序。
 */
@InterfaceAudience.Private
public final class AclStorage {

  // 全局唯一ACL特征引用计数缓存，实现相同ACL特征复用，节省内存
  private final static ReferenceCountMap<AclFeature> UNIQUE_ACL_FEATURES =
      new ReferenceCountMap<AclFeature>();

  /**
   * 如果父目录定义了默认ACL，则将默认ACL复制到新创建的子文件或子目录
   *
   * @param child 新创建的子inode
   * @return boolean 是否成功复制了ACL
   */
  public static boolean copyINodeDefaultAcl(INode child) {
    INodeDirectory parent = child.getParent();
    AclFeature parentAclFeature = parent.getAclFeature();
    if (parentAclFeature == null || !(child.isFile() || child.isDirectory())) {
      return false;
    }

    // 拆分父ACL的访问条目和默认条目
    List<AclEntry> featureEntries = getEntriesFromAclFeature(parent
        .getAclFeature());
    ScopedAclEntries scopedEntries = new ScopedAclEntries(featureEntries);
    List<AclEntry> parentDefaultEntries = scopedEntries.getDefaultEntries();

    // 父目录只有访问ACL没有默认ACL，无需复制
    if (parentDefaultEntries.isEmpty()) {
      return false;
    }

    // 预分配访问条目列表容量
    List<AclEntry> accessEntries = Lists.newArrayListWithCapacity(
      parentDefaultEntries.size());

    FsPermission childPerm = child.getFsPermission();

    // 逐个将父默认ACL条目转换为子节点的访问ACL条目
    boolean parentDefaultIsMinimal = AclUtil.isMinimalAcl(parentDefaultEntries);
    for (AclEntry entry: parentDefaultEntries) {
      AclEntryType type = entry.getType();
      String name = entry.getName();
      AclEntry.Builder builder = new AclEntry.Builder()
        .setScope(AclEntryScope.ACCESS)
        .setType(type)
        .setName(name);

      // 子节点初始权限位作为mode参数，过滤复制过来的owner、mask、other权限
      final FsAction permission;
      if (type == AclEntryType.USER && name == null) {
        permission = entry.getPermission().and(childPerm.getUserAction());
      } else if (type == AclEntryType.GROUP && parentDefaultIsMinimal) {
        // 默认ACL是最小ACL（仅包含owner、group、other三个条目）时，过滤组权限
        permission = entry.getPermission().and(childPerm.getGroupAction());
      } else if (type == AclEntryType.MASK) {
        // 使用mode的组权限位过滤掩码权限
        permission = entry.getPermission().and(childPerm.getGroupAction());
      } else if (type == AclEntryType.OTHER) {
        permission = entry.getPermission().and(childPerm.getOtherAction());
      } else {
        permission = entry.getPermission();
      }

      builder.setPermission(permission);
      accessEntries.add(builder.build());
    }

    // 如果子节点是目录，同时复制父默认ACL作为自身的默认ACL
    List<AclEntry> defaultEntries = child.isDirectory() ? parentDefaultEntries :
      Collections.<AclEntry>emptyList();

    final FsPermission newPerm;
    if (!AclUtil.isMinimalAcl(accessEntries) || !defaultEntries.isEmpty()) {
      // 需要保存扩展ACL到子节点
      child.addAclFeature(createAclFeature(accessEntries, defaultEntries));
      newPerm = createFsPermissionForExtendedAcl(accessEntries, childPerm);
    } else {
      // 仅需要保存最小ACL
      newPerm = createFsPermissionForMinimalAcl(accessEntries, childPerm);
    }

    child.setPermission(newPerm);
    return true;
  }

  /**
   * 读取inode已有的扩展ACL条目，支持按快照ID读取指定快照的ACL，仅返回存储在AclFeature中的扩展条目
   *
   * @param inode 目标inode
   * @param snapshotId 要读取的快照ID
   * @return {@literal List<AclEntry>} 扩展ACL条目列表，无ACL则返回空列表
   */
  public static List<AclEntry> readINodeAcl(INode inode, int snapshotId) {
    AclFeature f = inode.getAclFeature(snapshotId);
    return getEntriesFromAclFeature(f);
  }

  /**
   * 从INodeAttributes对象读取扩展ACL条目
   *
   * @param inodeAttr 目标inode属性对象
   * @return {@code List<AclEntry>} 扩展ACL条目列表，无ACL则返回空列表
   */
  public static List<AclEntry> readINodeAcl(INodeAttributes inodeAttr) {
    AclFeature f = inodeAttr.getAclFeature();
    return getEntriesFromAclFeature(f);
  }

  /**
   * 从{@link AclFeature}构建完整AclEntry列表
   * @param aclFeature 目标AclFeature对象
   * @return 完整AclEntry列表
   */
  @VisibleForTesting
  static ImmutableList<AclEntry> getEntriesFromAclFeature(AclFeature aclFeature) {
    if (aclFeature == null) {
      return ImmutableList.<AclEntry> of();
    }
    ImmutableList.Builder<AclEntry> b = new ImmutableList.Builder<AclEntry>();
    // 遍历存储的整数编码条目，转换为AclEntry对象
    for (int pos = 0, entry; pos < aclFeature.getEntriesSize(); pos++) {
      entry = aclFeature.getEntryAt(pos);
      b.add(AclEntryStatusFormat.toAclEntry(entry));
    }
    return b.build();
  }

  /**
   * 读取inode的完整逻辑ACL，合并FsPermission中的隐式条目和AclFeature中的扩展条目，返回完整ACL
   * 每个inode逻辑上都存在ACL，未显式设置的也会返回包含owner、group、other三个条目的最小ACL。
   * 本方法仅读取inode当前状态，不支持按快照ID读取，主要用于ACL修改API场景。
   *
   * @param inode 目标inode
   * @return {@code List<AclEntry>} 完整逻辑ACL条目列表
   */
  public static List<AclEntry> readINodeLogicalAcl(INode inode) {
    FsPermission perm = inode.getFsPermission();
    AclFeature f = inode.getAclFeature();
    if (f == null) {
      return AclUtil.getMinimalAcl(perm);
    }

    final List<AclEntry> existingAcl;
    // 拆分AclFeature中的访问条目和默认条目
    List<AclEntry> featureEntries = getEntriesFromAclFeature(f);
    ScopedAclEntries scoped = new ScopedAclEntries(featureEntries);
    List<AclEntry> accessEntries = scoped.getAccessEntries();
    List<AclEntry> defaultEntries = scoped.getDefaultEntries();

    // 预分配容量：特征中的条目 + 3个隐式条目（owner、group、other）
    existingAcl = Lists.newArrayListWithCapacity(featureEntries.size() + 3);

    if (!accessEntries.isEmpty()) {
      // 从用户权限位添加隐式owner条目
      existingAcl.add(new AclEntry.Builder().setScope(AclEntryScope.ACCESS)
          .setType(AclEntryType.USER).setPermission(perm.getUserAction())
          .build());

      // 添加特征中存储的所有命名用户和组条目
      existingAcl.addAll(accessEntries);

      // 从组权限位添加隐式mask条目
      existingAcl.add(new AclEntry.Builder().setScope(AclEntryScope.ACCESS)
          .setType(AclEntryType.MASK).setPermission(perm.getGroupAction())
          .build());

      // 从其他权限位添加隐式other条目
      existingAcl.add(new AclEntry.Builder().setScope(AclEntryScope.ACCESS)
          .setType(AclEntryType.OTHER).setPermission(perm.getOtherAction())
          .build());
    } else {
      // 仅有默认ACL无访问ACL，添加权限位生成的最小访问ACL
      existingAcl.addAll(AclUtil.getMinimalAcl(perm));
    }

    // 在访问条目之后添加所有默认条目
    existingAcl.addAll(defaultEntries);

    // 添加过程已经保证顺序，无需重新排序
    return existingAcl;
  }

  /**
   * 更新inode的ACL，将完整逻辑ACL条目存储到inode的{@link FsPermission}和{@link AclFeature}
   *
   * @param inode 要更新的inode
   * @param newAcl 新的完整ACL条目列表
   * @param snapshotId inode最新快照ID
   * @throws AclException 如果ACL对当前inode无效
   * @throws QuotaExceededException 如果超出配额限制
   */
  public static void updateINodeAcl(INode inode, List<AclEntry> newAcl,
      int snapshotId) throws AclException, QuotaExceededException {
    assert newAcl.size() >= 3;
    FsPermission perm = inode.getFsPermission();
    final FsPermission newPerm;
    if (!AclUtil.isMinimalAcl(newAcl)) {
      // 处理扩展ACL，拆分访问和默认条目
      ScopedAclEntries scoped = new ScopedAclEntries(newAcl);
      List<AclEntry> accessEntries = scoped.getAccessEntries();
      List<AclEntry> defaultEntries = scoped.getDefaultEntries();

      // 仅目录可以拥有默认ACL
      if (!defaultEntries.isEmpty() && !inode.isDirectory()) {
        throw new AclException(
          "Invalid ACL: only directories may have a default ACL. "
            + "Path: " + inode.getFullPathName());
      }

      // 移除旧ACL特征，添加新ACL特征
      if (inode.getAclFeature() != null) {
        inode.removeAclFeature(snapshotId);
      }
      inode.addAclFeature(createAclFeature(accessEntries, defaultEntries),
        snapshotId);
      newPerm = createFsPermissionForExtendedAcl(accessEntries, perm);
    } else {
      // 处理最小ACL，移除已有的ACL特征
      if (inode.getAclFeature() != null) {
        inode.removeAclFeature(snapshotId);
      }
      newPerm = createFsPermissionForMinimalAcl(newAcl, perm);
    }

    inode.setPermission(newPerm, snapshotId);
  }

  /**
   * 工具类禁止实例化
   */
  private AclStorage() {
  }

  /**
   * 根据访问ACL和默认ACL条目创建AclFeature对象
   *
   * @param accessEntries 访问ACL条目列表
   * @param defaultEntries 默认ACL条目列表
   * @return 创建好的AclFeature对象
   */
  private static AclFeature createAclFeature(List<AclEntry> accessEntries,
      List<AclEntry> defaultEntries) {
    // 预分配容量：所有条目减去存储在权限位的3个隐式条目，加上所有默认条目
    List<AclEntry> featureEntries = Lists.newArrayListWithCapacity(
      (accessEntries.size() - 3) + defaultEntries.size());

    // 访问ACL仅需要存储命名用户和命名组条目，这些条目在已排序ACL中位于固定区间
    if (!AclUtil.isMinimalAcl(accessEntries)) {
      featureEntries.addAll(
        accessEntries.subList(1, accessEntries.size() - 2));
    }

    // 添加所有默认条目到特征
    featureEntries.addAll(defaultEntries);
    return new AclFeature(AclEntryStatusFormat.toInt(featureEntries));
  }

  /**
   * 为扩展ACL创建新的FsPermission，将掩码权限存入组权限位，保留粘滞位，开启ACL标志位
   * 符合POSIX ACL模型，掩码以组权限形式对外呈现，保持chmod等命令兼容性
   *
   * @param accessEntries 访问ACL条目列表
   * @param existingPerm 原有权限对象
   * @return 新的权限对象
   */
  private static FsPermission createFsPermissionForExtendedAcl(
      List<AclEntry> accessEntries, FsPermission existingPerm) {
    return new FsPermission(accessEntries.get(0).getPermission(),
      accessEntries.get(accessEntries.size() - 2).getPermission(),
      accessEntries.get(accessEntries.size() - 1).getPermission(),
      existingPerm.getStickyBit());
  }

  /**
   * 为最小ACL创建新的FsPermission，直接使用ACL中三个条目的权限，保留粘滞位，关闭ACL标志位
   *
   * @param accessEntries 访问ACL条目列表
   * @param existingPerm 原有权限对象
   * @return 新的权限对象
   */
  private static FsPermission createFsPermissionForMinimalAcl(
      List<AclEntry> accessEntries, FsPermission existingPerm) {
    return new FsPermission(accessEntries.get(0).getPermission(),
      accessEntries.get(1).getPermission(),
      accessEntries.get(2).getPermission(),
      existingPerm.getStickyBit());
  }

  @VisibleForTesting
  public static ReferenceCountMap<AclFeature> getUniqueAclFeatures() {
    return UNIQUE_ACL_FEATURES;
  }

  /**
   * 添加AclFeature引用，复用全局缓存中已有的相同ACL特征
   * 
   * @param aclFeature 要添加引用的AclFeature
   * @return 缓存中复用的AclFeature
   */
  public static AclFeature addAclFeature(AclFeature aclFeature) {
    return UNIQUE_ACL_FEATURES.put(aclFeature);
  }

  /**
   * 移除AclFeature引用，引用计数为0时自动回收
   * 
   * @param aclFeature 要移除引用的AclFeature
   */
  public static void removeAclFeature(AclFeature aclFeature) {
    UNIQUE_ACL_FEATURES.remove(aclFeature);
  }
}