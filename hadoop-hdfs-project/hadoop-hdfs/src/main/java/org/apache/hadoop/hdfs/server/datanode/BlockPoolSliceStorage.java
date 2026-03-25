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

package org.apache.hadoop.hdfs.server.datanode;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.HardLink;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.protocol.LayoutVersion;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.NodeType;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.StartupOption;
import org.apache.hadoop.hdfs.server.common.InconsistentFSStateException;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.Lists;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.util.Preconditions;

/**
 * 文件说明：DataNode上单个块池的存储管理器，负责管理该块池在当前DataNode上所有存储目录的生命周期，
 * 支持格式化、升级、回滚、完成升级、回收站清理等存储状态转换操作，是HDFS联邦架构下DataNode存储分层管理的核心组件。
 * 
 * 本类支持以下核心功能：
 * <ul>
 * <li>格式化新的块池存储</li>
 * <li>从异常存储状态恢复（如果可能）</li>
 * <li>升级时创建块池存储快照</li>
 * <li>将块池回滚到之前的快照版本</li>
 * <li>删除升级快照完成升级流程</li>
 * </ul>
 * 
 * @see Storage
 */
@InterfaceAudience.Private
public class BlockPoolSliceStorage extends Storage {
  /** 回收站根目录名称 */
  static final String TRASH_ROOT_DIR = "trash";

  /**
   * 滚动升级进行中标记文件。当滚动升级进行中时，每个块池根目录会创建该标记。
   * 由于NameNode不会主动通知DataNode滚动升级已完成，DataNode通过该标记判断升级状态：
   * 1. 如果标记不存在但存在previous目录，说明滚动升级已完成，可以删除previous目录
   * 2. 如果标记不存在，说明可能是常规升级进行中，不要删除previous目录
   */
  static final String ROLLING_UPGRADE_MARKER_FILE = "RollingUpgradeInProgress";

  private static final String BLOCK_POOL_ID_PATTERN_BASE =
      Pattern.quote(File.separator) +
      "BP-\\d+-\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}-\\d+" +
      Pattern.quote(File.separator);

  /** 匹配块池路径的正则表达式 */
  private static final Pattern BLOCK_POOL_PATH_PATTERN = Pattern.compile(
      "^(.*)(" + BLOCK_POOL_ID_PATTERN_BASE + ")(.*)$");

  /** 匹配块池current目录路径的正则表达式 */
  private static final Pattern BLOCK_POOL_CURRENT_PATH_PATTERN = Pattern.compile(
      "^(.*)(" + BLOCK_POOL_ID_PATTERN_BASE + ")(" + STORAGE_DIR_CURRENT + ")(.*)$");

  /** 匹配块池trash目录路径的正则表达式 */
  private static final Pattern BLOCK_POOL_TRASH_PATH_PATTERN = Pattern.compile(
      "^(.*)(" + BLOCK_POOL_ID_PATTERN_BASE + ")(" + TRASH_ROOT_DIR + ")(.*)$");

  /** 当前管理的块池ID */
  private String blockpoolID = "";
  /** 回收站清理后台线程 */
  private Daemon trashCleaner;

  /**
   * 构造函数，使用已有存储信息和块池ID创建块池存储管理器
   * @param storageInfo 基础存储信息
   * @param bpid 块池ID
   */
  public BlockPoolSliceStorage(StorageInfo storageInfo, String bpid) {
    super(storageInfo);
    blockpoolID = bpid;
  }

  /**
   * 这两个集合用于性能优化，避免每次心跳都执行文件系统操作检查标记文件是否存在
   * 缓存已经存在滚动升级标记的存储目录
   */
  private static Set<String> storagesWithRollingUpgradeMarker;
  /** 缓存不存在滚动升级标记的存储目录 */
  private static Set<String> storagesWithoutRollingUpgradeMarker;

  /**
   * 构造函数，使用指定参数创建块池存储管理器
   * @param namespaceID 命名空间ID
   * @param bpID 块池ID
   * @param cTime 创建时间
   * @param clusterId 集群ID
   */
  BlockPoolSliceStorage(int namespaceID, String bpID, long cTime,
      String clusterId) {
    super(NodeType.DATA_NODE);
    this.namespaceID = namespaceID;
    this.blockpoolID = bpID;
    this.cTime = cTime;
    this.clusterID = clusterId;
    storagesWithRollingUpgradeMarker = Collections.newSetFromMap(
        new ConcurrentHashMap<String, Boolean>());
    storagesWithoutRollingUpgradeMarker = Collections.newSetFromMap(
        new ConcurrentHashMap<String, Boolean>());
  }

  /**
   * 私有默认构造函数，内部使用，初始化缓存集合
   */
  private BlockPoolSliceStorage() {
    super(NodeType.DATA_NODE);
    storagesWithRollingUpgradeMarker = Collections.newSetFromMap(
        new ConcurrentHashMap<String, Boolean>());
    storagesWithoutRollingUpgradeMarker = Collections.newSetFromMap(
        new ConcurrentHashMap<String, Boolean>());
  }

  /**
   * 添加存储目录到当前块池存储管理，暴露给VolumeBuilder#commit()使用
   * @param sd 要添加的存储目录
   */
  // Expose visibility for VolumeBuilder#commit().
  public void addStorageDir(StorageDirectory sd) {
    super.addStorageDir(sd);
  }

  /**
   * 加载单个存储目录，必要时从之前的状态转换中恢复
   * @param nsInfo 命名空间信息
   * @param location 存储目录根路径
   * @param startOpt 启动选项
   * @param callables 异步执行的存储目录任务列表
   * @param conf 配置对象
   * @return 加载完成的存储目录
   * @throws IOException 加载过程中发生IO异常
   */
  private StorageDirectory loadStorageDirectory(NamespaceInfo nsInfo,
      StorageLocation location, StartupOption startOpt,
      List<Callable<StorageDirectory>> callables, Configuration conf)
          throws IOException {
    // 创建块池存储目录对象
    StorageDirectory sd = new StorageDirectory(
        nsInfo.getBlockPoolID(), null, true, location);
    try {
      // 分析存储目录当前状态
      StorageState curState = sd.analyzeStorage(startOpt, this, true);
      // 此时sd已加锁但未打开
      switch (curState) {
      case NORMAL:
        // 状态正常，直接继续
        break;
      case NON_EXISTENT:
        // 目录不存在，抛出异常
        LOG.info("Block pool storage directory for location {} and block pool"
            + " id {} does not exist", location, nsInfo.getBlockPoolID());
        throw new IOException("Storage directory for location " + location +
            " and block pool id " + nsInfo.getBlockPoolID() +
            " does not exist");
      case NOT_FORMATTED:
        // 目录未格式化，执行格式化
        LOG.info("Block pool storage directory for location {} and block pool"
                + " id {} is not formatted. Formatting ...", location,
            nsInfo.getBlockPoolID());
        format(sd, nsInfo);
        break;
      default:
        // 其他状态需要恢复，执行恢复流程
        sd.doRecover(curState);
      }

      // 执行存储状态转换
      // 每个存储目录独立处理，启动时部分目录可能需要升级或回滚，其他已经是最新状态可以直接启动
      if (!doTransition(sd, nsInfo, startOpt, callables, conf)) {
        // 转换完成，检查CTime是否匹配Namenode
        if (getCTime() != nsInfo.getCTime()) {
          throw new IOException("Datanode CTime (=" + getCTime()
              + ") is not equal to namenode CTime (=" + nsInfo.getCTime() + ")");
        }
        // 更新布局版本并写入属性文件
        setServiceLayoutVersion(getServiceLayoutVersion());
        writeProperties(sd);
      }

      return sd;
    } catch (IOException ioe) {
      // 发生异常，解锁目录后抛出
      sd.unlock();
      throw ioe;
    }
  }

  /**
   * 分析并加载块池存储目录，必要时从之前的状态转换中恢复
   * 块池存储要么全部加载成功，要么全部不加载，任何一个目录加载失败都会导致整个数据卷失效
   * @param nsInfo 命名空间信息
   * @param location 块池存储目录位置
   * @param startOpt 启动选项
   * @param callables 异步执行的存储目录任务列表
   * @param conf 配置对象
   * @return 加载完成的存储目录数组
   * @throws IOException 加载过程中发生IO异常
   */
  List<StorageDirectory> loadBpStorageDirectories(NamespaceInfo nsInfo,
      StorageLocation location, StartupOption startOpt,
      List<Callable<StorageDirectory>> callables, Configuration conf)
          throws IOException {
    List<StorageDirectory> succeedDirs = Lists.newArrayList();
    try {
      // 检查是否已经加载过该位置的块池
      if (containsStorageDir(location, nsInfo.getBlockPoolID())) {
        throw new IOException(
            "BlockPoolSliceStorage.recoverTransitionRead: " +
                "attempt to load an used block storage: " + location);
      }
      // 加载单个存储目录
      final StorageDirectory sd = loadStorageDirectory(
          nsInfo, location, startOpt, callables, conf);
      succeedDirs.add(sd);
    } catch (IOException e) {
      // 加载失败，记录日志后抛出
      LOG.warn("Failed to analyze storage directories for block pool {}",
          nsInfo.getBlockPoolID(), e);
      throw e;
    }
    return succeedDirs;
  }

  /**
   * 分析块池存储目录，从之前的状态转换中恢复，完成后将目录添加到本管理器
   * 块池存储要么全部加载成功，要么全部不加载，任何一个目录加载失败都会导致整个数据卷失效
   * @param nsInfo 命名空间信息
   * @param location 块池存储目录位置
   * @param startOpt 启动选项
   * @param callables 异步执行的存储目录任务列表
   * @param conf 配置对象
   * @return 加载完成的存储目录列表
   * @throws IOException 加载过程中发生IO异常
   */
  List<StorageDirectory> recoverTransitionRead(NamespaceInfo nsInfo,
      StorageLocation location, StartupOption startOpt,
      List<Callable<StorageDirectory>> callables, Configuration conf)
          throws IOException {
    LOG.info("Analyzing storage directories for bpid {}", nsInfo
        .getBlockPoolID());
    // 加载存储目录
    final List<StorageDirectory> loaded = loadBpStorageDirectories(
        nsInfo, location, startOpt, callables, conf);
    // 将加载成功的目录添加到本管理器
    for (StorageDirectory sd : loaded) {
      addStorageDir(sd);
    }
    return loaded;
  }

  /**
   * 格式化指定DataNode当前目录下的块池存储
   * @param dnCurDir DataNode当前目录
   * @param nsInfo 命名空间信息
   * @throws IO异常
   */
  void format(File dnCurDir, NamespaceInfo nsInfo) throws IOException {
    File curBpDir = getBpRoot(nsInfo.getBlockPoolID(), dnCurDir);
    StorageDirectory bpSdir = new StorageDirectory(curBpDir);
    format(bpSdir, nsInfo);
  }

  /**
   * 格式化指定的块池存储目录
   * @param bpSdir 块池存储目录对象
   * @param nsInfo 命名空间信息
   * @throws IO异常
   */
  private void format(StorageDirectory bpSdir, NamespaceInfo nsInfo) throws IOException {
    LOG.info("Formatting block pool {} directory {}", blockpoolID, bpSdir
        .getCurrentDir());
    // 清空目录并重新创建
    bpSdir.clearDirectory();
    // 初始化存储版本信息
    this.layoutVersion = DataNodeLayoutVersion.getCurrentLayoutVersion();
    this.cTime = nsInfo.getCTime();
    this.namespaceID = nsInfo.getNamespaceID();
    this.blockpoolID = nsInfo.getBlockPoolID();
    // 写入版本属性文件
    writeProperties(bpSdir);
  }

  /**
   * 移除指定绝对路径对应的块池级存储目录
   * @param absPathToRemove 要移除的块池存储根目录绝对路径
   */
  void remove(File absPathToRemove) {
    Preconditions.checkArgument(absPathToRemove.isAbsolute());
    LOG.info("Removing block level storage: {}", absPathToRemove);
    // 遍历存储目录找到匹配项并移除
    for (Iterator<StorageDirectory> it = getStorageDirs().iterator();
         it.hasNext(); ) {
      StorageDirectory sd = it.next();
      if (sd.getRoot().getAbsoluteFile().equals(absPathToRemove)) {
        getStorageDirs().remove(sd);
        break;
      }
    }
  }

  /**
   * 将当前块池存储的布局版本、命名空间ID、块池ID、创建时间写入属性文件
   */
  @Override
  protected void setPropertiesFromFields(Properties props, StorageDirectory sd)
      throws IOException {
    props.setProperty("layoutVersion", String.valueOf(layoutVersion));
    props.setProperty("namespaceID", String.valueOf(namespaceID));
    props.setProperty("blockpoolID", blockpoolID);
    props.setProperty("cTime", String.valueOf(cTime));
  }

  /**
   * 验证并设置块池ID，检查一致性
   * @param storage 存储目录文件
   * @param bpid 从属性文件读取的块池ID
   * @throws InconsistentFSStateException 块池ID不一致或为空时抛出
   */
  private void setBlockPoolID(File storage, String bpid)
      throws InconsistentFSStateException {
    if (bpid == null || bpid.equals("")) {
      throw new InconsistentFSStateException(storage, "file "
          + STORAGE_FILE_VERSION + " is invalid.");
    }
    
    if (!blockpoolID.equals("") && !blockpoolID.equals(bpid)) {
      throw new InconsistentFSStateException(storage,
          "Unexpected blockpoolID " + bpid + ". Expected " + blockpoolID);
    }
    blockpoolID = bpid;
  }
  
  /**
   * 从属性文件读取并设置各个存储字段
   */
  @Override
  protected void setFieldsFromProperties(Properties props, StorageDirectory sd)
      throws IOException {
    setLayoutVersion(props, sd);
    setNamespaceID(props, sd);
    setcTime(props, sd);
    
    String sbpid = props.getProperty("blockpoolID");
    setBlockPoolID(sd.getRoot(), sbpid);
  }

  /**
   * 检查是否需要进行块池状态转换，如果需要则执行转换
   * 
   * 状态转换判断逻辑：
   * <br>
   * 回滚条件：previous布局版本 >= 当前布局版本 且 previous创建时间 <= Namenode创建时间
   * <br>
   * 升级条件：当前布局版本 > 当前软件布局版本 或 当前创建时间 < Namenode创建时间
   * <br>
   * 常规启动条件：当前布局版本 = 当前软件布局版本 且 当前创建时间 = Namenode创建时间
   * 
   * @param sd 存储目录 {@literal <SD>/current/<bpid>}
   * @param nsInfo 命名空间信息
   * @param startOpt 启动选项
   * @param callables 异步执行的存储目录任务列表
   * @param conf 配置对象
   * @return 如果已经写入新属性返回true，否则返回false
   */
  private boolean doTransition