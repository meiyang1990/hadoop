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

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_SNAPSHOT_CAPTURE_OPENFILES;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_SNAPSHOT_CAPTURE_OPENFILES_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_SNAPSHOT_SKIP_CAPTURE_ACCESSTIME_ONLY_CHANGE;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_SNAPSHOT_SKIP_CAPTURE_ACCESSTIME_ONLY_CHANGE_DEFAULT;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import javax.management.ObjectName;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.XAttr;
import org.apache.hadoop.fs.XAttrSetFlag;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.hdfs.XAttrHelper;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReportListing;
import org.apache.hadoop.hdfs.protocol.SnapshotException;
import org.apache.hadoop.hdfs.protocol.SnapshotInfo;
import org.apache.hadoop.hdfs.protocol.SnapshottableDirectoryStatus;
import org.apache.hadoop.hdfs.protocol.SnapshotStatus;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport.DiffReportEntry;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.namenode.*;
import org.apache.hadoop.hdfs.server.namenode.FSDirectory.DirOp;
import org.apache.hadoop.hdfs.server.namenode.FSImageFormat;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeDirectory;
import org.apache.hadoop.hdfs.server.namenode.INodesInPath;
import org.apache.hadoop.hdfs.server.namenode.LeaseManager;
import org.apache.hadoop.hdfs.util.ReadOnlyList;
import org.apache.hadoop.metrics2.util.MBeans;
import org.apache.hadoop.util.Lists;

import org.apache.hadoop.util.Preconditions;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 文件概述：HDFS快照管理器，负责管理所有可快照目录和快照的生命周期
 * 
 * 核心职责：
 * 1. 管理可快照目录的注册、取消注册
 * 2. 处理快照的创建、删除、重命名操作
 * 3. 提供快照差异计算功能
 * 4. 持久化和加载快照元数据到FSImage
 * 5. 暴露JMX监控统计信息
 * 
 * 锁机制说明：
 * 1. 进入此类方法前，调用方必须已经获取FSNamesystem全局锁
 * 2. 必要时此类方法会进一步获取FSDirectory目录锁
 */
public class SnapshotManager implements SnapshotStatsMXBean {
  public static final Logger LOG =
      LoggerFactory.getLogger(SnapshotManager.class);

  // 以下是私有配置参数
  static final String DFS_NAMENODE_SNAPSHOT_DELETION_ORDERED
      = "dfs.namenode.snapshot.deletion.ordered";
  static final boolean DFS_NAMENODE_SNAPSHOT_DELETION_ORDERED_DEFAULT
      = false;
  static final String DFS_NAMENODE_SNAPSHOT_DELETION_ORDERED_GC_PERIOD_MS
      = "dfs.namenode.snapshot.deletion.ordered.gc.period.ms";
  static final long DFS_NAMENODE_SNAPSHOT_DELETION_ORDERED_GC_PERIOD_MS_DEFAULT
      = 5 * 60_000L; //5 minutes

  private static final ThreadLocal<Boolean> DELETION_ORDERED
      = new ThreadLocal<>();

  /**
   * 获取当前线程是否启用有序删除模式
   * @return true为启用有序删除，false为普通删除
   */
  static boolean isDeletionOrdered() {
    final Boolean b = DELETION_ORDERED.get();
    return b != null? b: false;
  }

  /**
   * 初始化当前线程的有序删除配置，从全局配置复制到线程局部变量
   */
  public void initThreadLocals() {
    DELETION_ORDERED.set(isSnapshotDeletionOrdered());
  }

  private final FSDirectory fsdir;
  private boolean captureOpenFiles;
  /**
   * If skipCaptureAccessTimeOnlyChange is set to true, if accessTime
   * of a file changed but there is no other modification made to the file,
   * it will not be captured in next snapshot. However, if there is other
   * modification made to the file, the last access time will be captured
   * together with the modification in next snapshot.
   */
  private boolean skipCaptureAccessTimeOnlyChange = false;
  /**
   * If snapshotDiffAllowSnapRootDescendant is set to true, snapshot diff
   * operation can be run for any descendant directory under a snapshot root
   * directory and the diff calculation will be scoped to the descendant
   * directory.
   */
  private final boolean snapshotDiffAllowSnapRootDescendant;

  private final AtomicInteger numSnapshots = new AtomicInteger();
  private static final int SNAPSHOT_ID_BIT_WIDTH = 28;

  private boolean allowNestedSnapshots = false;
  private final boolean snapshotDeletionOrdered;
  private int snapshotCounter = 0;
  private final int maxSnapshotLimit;
  private final int maxSnapshotFSLimit;
  
  /** 存储文件系统中所有可快照目录，key为目录inode id，value为目录inode对象 */
  private final Map<Long, INodeDirectory> snapshottables =
      new ConcurrentHashMap<>();

  /**
   * 构造快照管理器，从配置加载参数并初始化工厂
   * @param conf Hadoop配置对象
   * @param fsdir 命名空间目录管理器引用
   * @throws SnapshotException 当配置参数非法时抛出异常
   */
  public SnapshotManager(final Configuration conf, final FSDirectory fsdir)
      throws SnapshotException {
    this.fsdir = fsdir;
    // 加载是否捕获打开文件的配置
    this.captureOpenFiles = conf.getBoolean(
        DFS_NAMENODE_SNAPSHOT_CAPTURE_OPENFILES,
        DFS_NAMENODE_SNAPSHOT_CAPTURE_OPENFILES_DEFAULT);
    // 加载是否跳过仅访问时间变更的捕获配置
    this.skipCaptureAccessTimeOnlyChange = conf.getBoolean(
        DFS_NAMENODE_SNAPSHOT_SKIP_CAPTURE_ACCESSTIME_ONLY_CHANGE,
        DFS_NAMENODE_SNAPSHOT_SKIP_CAPTURE_ACCESSTIME_ONLY_CHANGE_DEFAULT);
    // 加载是否允许在快照根目录子目录执行diff的配置
    this.snapshotDiffAllowSnapRootDescendant = conf.getBoolean(
        DFSConfigKeys.DFS_NAMENODE_SNAPSHOT_DIFF_ALLOW_SNAP_ROOT_DESCENDANT,
        DFSConfigKeys.
            DFS_NAMENODE_SNAPSHOT_DIFF_ALLOW_SNAP_ROOT_DESCENDANT_DEFAULT);
    // 加载单个目录最大快照数限制
    this.maxSnapshotLimit = conf.getInt(
        DFSConfigKeys.
            DFS_NAMENODE_SNAPSHOT_MAX_LIMIT,
        DFSConfigKeys.
            DFS_NAMENODE_SNAPSHOT_MAX_LIMIT_DEFAULT);
    // 加载整个文件系统最大快照数限制
    this.maxSnapshotFSLimit = conf.getInt(
        DFSConfigKeys.DFS_NAMENODE_SNAPSHOT_FILESYSTEM_LIMIT,
        DFSConfigKeys.DFS_NAMENODE_SNAPSHOT_FILESYSTEM_LIMIT_DEFAULT);
    // 打印加载的配置信息到日志
    LOG.info("Loaded config captureOpenFiles: " + captureOpenFiles
        + ", skipCaptureAccessTimeOnlyChange: "
        + skipCaptureAccessTimeOnlyChange
        + ", snapshotDiffAllowSnapRootDescendant: "
        + snapshotDiffAllowSnapRootDescendant
        + ", maxSnapshotFSLimit: "
        + maxSnapshotFSLimit
        + ", maxSnapshotLimit: "
        + maxSnapshotLimit);

    // 加载是否启用有序删除的配置
    this.snapshotDeletionOrdered = conf.getBoolean(
        DFS_NAMENODE_SNAPSHOT_DELETION_ORDERED,
        DFS_NAMENODE_SNAPSHOT_DELETION_ORDERED_DEFAULT);
    LOG.info("{} = {}", DFS_NAMENODE_SNAPSHOT_DELETION_ORDERED,
        snapshotDeletionOrdered);

    // 加载跳表相关配置，用于目录差异链表实现
    final int maxLevels = conf.getInt(
        DFSConfigKeys.DFS_NAMENODE_SNAPSHOT_SKIPLIST_MAX_LEVELS,
        DFSConfigKeys.DFS_NAMENODE_SNAPSHOT_SKIPLIST_MAX_SKIP_LEVELS_DEFAULT);
    final int skipInterval = conf.getInt(
        DFSConfigKeys.DFS_NAMENODE_SNAPSHOT_SKIPLIST_SKIP_INTERVAL,
        DFSConfigKeys.DFS_NAMENODE_SNAPSHOT_SKIPLIST_SKIP_INTERVAL_DEFAULT);
    // 校验单目录限制不能大于文件系统总限制
    if (maxSnapshotLimit > maxSnapshotFSLimit) {
      final String errMsg = DFSConfigKeys.
          DFS_NAMENODE_SNAPSHOT_MAX_LIMIT
          + " cannot be greater than " +
          DFSConfigKeys.DFS_NAMENODE_SNAPSHOT_FILESYSTEM_LIMIT;
      throw new SnapshotException(errMsg);
    }
    // 初始化目录差异列表工厂，传入跳表配置
    DirectoryDiffListFactory.init(skipInterval, maxLevels, LOG);
  }

  /**
   * 获取是否启用有序删除模式
   * @return 有序删除模式开关
   */
  public boolean isSnapshotDeletionOrdered() {
    return snapshotDeletionOrdered;
  }

  @VisibleForTesting
  void setCaptureOpenFiles(boolean captureOpenFiles) {
    this.captureOpenFiles = captureOpenFiles;
  }

  /**
   * 获取是否跳过仅访问时间变更的捕获配置
   * @return 跳过开关
   */
  public boolean getSkipCaptureAccessTimeOnlyChange() {
    return skipCaptureAccessTimeOnlyChange;
  }

  /**
   * Used in tests only
   * 设置是否允许嵌套快照目录，仅用于测试
   */
  void setAllowNestedSnapshots(boolean allowNestedSnapshots) {
    this.allowNestedSnapshots = allowNestedSnapshots;
  }

  /**
   * 获取是否允许嵌套快照目录配置
   * @return 允许嵌套开关
   */
  public boolean isAllowNestedSnapshots() {
    return allowNestedSnapshots;
  }

  /**
   * 检查新增可快照目录是否违反嵌套限制
   * @param dir 待检测目录
   * @param path 目录路径字符串
   * @throws SnapshotException 违反嵌套限制时抛出异常
   */
  private void checkNestedSnapshottable(INodeDirectory dir, String path)
      throws SnapshotException {
    if (allowNestedSnapshots) {
      return;
    }
    // 遍历已有可快照目录，检查是否存在祖孙关系
    for(INodeDirectory s : snapshottables.values()) {
      if (s.isAncestorDirectory(dir)) {
        throw new SnapshotException(
            "Nested snapshottable directories not allowed: path=" + path
            + ", the subdirectory " + s.getFullPathName()
            + " is already a snapshottable directory.");
      }
      if (dir.isAncestorDirectory(s)) {
        throw new SnapshotException(
            "Nested snapshottable directories not allowed: path=" + path
            + ", the ancestor " + s.getFullPathName()
            + " is already a snapshottable directory.");
      }
    }
  }

  /**
   * 将指定目录设置为可快照目录，若已是可快照则更新配额
   * @param path 目录路径
   * @param checkNestedSnapshottable 是否检查嵌套限制
   * @throws IOException 路径不存在或违反嵌套规则时抛出异常
   */
  public void setSnapshottable(final String path, boolean checkNestedSnapshottable)
      throws IOException {
    // 解析路径得到inode链表
    final INodesInPath iip = fsdir.getINodesInPath(path, DirOp.WRITE);
    // 获取最后一个节点，即目标目录
    final INodeDirectory d = INodeDirectory.valueOf(iip.getLastINode(), path);
    if (checkNestedSnapshottable) {
      checkNestedSnapshottable(d, path);
    }

    if (d.isSnapshottable()) {
      // 目录已是可快照，仅更新默认配额
      d.setSnapshotQuota(DirectorySnapshottableFeature.SNAPSHOT_QUOTA_DEFAULT);
    } else {
      // 添加可快照特性到目录
      d.addSnapshottableFeature();
    }
    // 添加到管理器的可快照目录集合
    addSnapshottable(d);
  }
  
  /**
   * 将给定可快照目录添加到全局可快照集合
   * @param dir 可快照目录
   */
  public void addSnapshottable(INodeDirectory dir) {
    Preconditions.checkArgument(dir.isSnapshottable());
    snapshottables.put(dir.getId(), dir);
  }

  /**
   * 从全局可快照集合移除指定可快照目录
   * @param s 待移除目录
   */
  private void removeSnapshottable(INodeDirectory s) {
    snapshottables.remove(s.getId());
  }
  
  /**
   * 批量移除可快照目录
   * @param toRemove 待移除目录列表
   */
  public void removeSnapshottable(List<INodeDirectory> toRemove) {
    if (toRemove != null) {
      for (INodeDirectory s : toRemove) {
        removeSnapshottable(s);
      }
    }
  }

  /**
   * 将指定可快照目录取消为不可快照
   * @param path 目录路径
   * @throws SnapshotException 目录仍存在快照时抛出异常
   * @throws IOException 路径不存在时抛出异常
   */
  public void resetSnapshottable(final String path) throws IOException {
    // 解析路径得到inode链表
    final INodesInPath iip = fsdir.getINodesInPath(path, DirOp.WRITE);
    // 获取目标目录
    final INodeDirectory d = INodeDirectory.valueOf(iip.getLastINode(), path);
    // 获取可快照特性
    DirectorySnapshottableFeature sf = d.getDirectorySnapshottableFeature();
    if (sf == null) {
      // 已经是非可快照，直接返回
      return;
    }
    // 若目录仍有快照，不允许取消，抛出异常
    if (sf.getNumSnapshots() > 0) {
      throw new SnapshotException("The directory " + path + " has snapshot(s). "
          + "Please redo the operation after removing all the snapshots.");
    }

    if (d == fsdir.getRoot()) {
      // 根目录仅清除配额，不删除特性
      d.setSnapshotQuota(0);
    } else {
      // 移除可快照特性
      d.removeSnapshottableFeature();
    }
    // 从全局集合移除目录
    removeSnapshottable(d);
  }

  /**
   * 根据给定路径获取可快照根目录，要求路径本身必须是可快照目录
   * @param iip 路径解析后的inode链表
   * @return 可快照根目录对象
   * @throws IOException 路径不存在或不是可快照目录时抛出异常
   */
  public INodeDirectory getSnapshottableRoot(final INodesInPath iip)
      throws IOException {
    final String path = iip.getPath();
    final INodeDirectory dir = INodeDirectory.valueOf(iip.getLastINode(), path);
    if (!dir.isSnapshottable()) {
      throw new SnapshotException(
          "Directory is not a snapshottable directory: " + path);
    }
    return dir;
  }

  /**
   * 断言指定快照