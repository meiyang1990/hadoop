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

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.URI;
import java.net.UnknownHostException;
import java.nio.channels.ClosedByInterruptException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.LayoutVersion;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.NodeType;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.StartupOption;
import org.apache.hadoop.hdfs.server.common.InconsistentFSStateException;
import org.apache.hadoop.hdfs.server.common.IncorrectVersionException;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.common.StorageErrorReporter;
import org.apache.hadoop.hdfs.server.common.Util;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.util.PersistentLongFile;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.net.DNS;
import org.apache.hadoop.util.Lists;
import org.apache.hadoop.util.Time;
import org.eclipse.jetty.util.ajax.JSON;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.util.Preconditions;

/**
 * @fileoverview NNStorage 负责管理NameNode使用的所有存储目录，维护fsimage和edits日志的存储位置、状态和元信息。
 * 核心职责包括：存储目录初始化、错误检测与故障转移、存储目录恢复、版本信息管理、元数据文件路径生成等。
 */
@InterfaceAudience.Private
public class NNStorage extends Storage implements Closeable,
    StorageErrorReporter {
  static final String DEPRECATED_MESSAGE_DIGEST_PROPERTY = "imageMD5Digest";
  static final String LOCAL_URI_SCHEME = "file";

  /**
   * NameNode存储文件类型枚举，定义了各类元数据文件的基础文件名。
   */
  public enum NameNodeFile {
    IMAGE     ("fsimage"),
    TIME      ("fstime"), // from "old" pre-HDFS-1073 format
    SEEN_TXID ("seen_txid"),
    EDITS     ("edits"),
    IMAGE_NEW ("fsimage.ckpt"),
    IMAGE_ROLLBACK("fsimage_rollback"),
    EDITS_NEW ("edits.new"), // from "old" pre-HDFS-1073 format
    EDITS_INPROGRESS ("edits_inprogress"),
    EDITS_TMP ("edits_tmp"),
    IMAGE_LEGACY_OIV ("fsimage_legacy_oiv");  // For pre-PB format

    private String fileName = null;
    NameNodeFile(String name) {
      this.fileName = name;
    }

    @VisibleForTesting
    public String getName() {
      return fileName;
    }
  }

  /**
   * NameNode存储目录类型枚举，定义存储目录承担的功能：仅存储fsimage、仅存储edits日志、或同时存储两者。
   */
  @VisibleForTesting
  public enum NameNodeDirType implements StorageDirType {
    UNDEFINED,
    IMAGE,
    EDITS,
    IMAGE_AND_EDITS;

    @Override
    public StorageDirType getStorageDirType() {
      return this;
    }

    @Override
    public boolean isOfType(StorageDirType type) {
      return (this == IMAGE_AND_EDITS) && (type == IMAGE || type == EDITS) ||
          this == type;
    }
  }

  protected String blockpoolID = ""; // 块池ID，联邦场景下标识当前NameNode所属块池

  /**
   * 标识是否尝试恢复已失败的存储目录。
   */
  private boolean restoreFailedStorage = false;
  private final Object restorationLock = new Object();
  private boolean disablePreUpgradableLayoutCheck = false;
  private final Configuration conf;

  /**
   * 最近一次检查点完成时包含的最大事务ID，不包含检查点之后写入edits的事务。
   */
  protected volatile long mostRecentCheckpointTxId =
      HdfsServerConstants.INVALID_TXID;
  
  /**
   * 最近一次检查点完成的时间，单位：从纪元开始的毫秒数。
   */
  private long mostRecentCheckpointTime = 0;

  /**
   * 已失败并被移除出服务的存储目录列表。
   */
  final protected List<StorageDirectory> removedStorageDirs
      = new CopyOnWriteArrayList<>();

  /**
   * 旧版本布局中提取的废弃属性，仅在升级过程中需要。
   */
  private HashMap<String, String> deprecatedProperties;

  /**
   * 存储目录大小映射表，用于监控指标。
   */
  private Map<String, Long> nameDirSizeMap = new HashMap<>();

  /**
   * 构造NNStorage对象，初始化fsimage和edits日志存储目录。
   * @param conf NameNode配置对象
   * @param imageDirs fsimage存储目录URI集合
   * @param editsDirs edits日志存储目录URI集合
   * @throws IOException 如果目录初始化失败则抛出异常
   */
  public NNStorage(Configuration conf, 
                   Collection<URI> imageDirs, Collection<URI> editsDirs) 
      throws IOException {
    super(NodeType.NAME_NODE);
    this.conf = conf;

    // this may modify the editsDirs, so copy before passing in
    setStorageDirectories(imageDirs, 
                          Lists.newArrayList(editsDirs),
                          FSNamesystem.getSharedEditsDirs(conf));
    //NameNode启动后更新存储目录大小监控指标
    updateNameDirSize();
  }

  @Override // Storage
  /**
   * 检查存储目录是否是可预升级的布局版本
   * @param sd 待检查的存储目录
   * @return 如果是可预升级布局返回true，否则返回false
   * @throws IOExceptions 读取文件失败抛出异常
   */
  public boolean isPreUpgradableLayout(StorageDirectory sd) throws IOException {
    if (disablePreUpgradableLayoutCheck) {
      return false;
    }

    File oldImageDir = new File(sd.getRoot(), "image");
    if (!oldImageDir.exists()) {
      return false;
    }
    // check the layout version inside the image file
    File oldF = new File(oldImageDir, "fsimage");
    RandomAccessFile oldFile = new RandomAccessFile(oldF, "rws");
    try {
      oldFile.seek(0);
      int oldVersion = oldFile.readInt();
      oldFile.close();
      oldFile = null;
      if (oldVersion < LAST_PRE_UPGRADE_LAYOUT_VERSION) {
        return false;
      }
    } finally {
      IOUtils.cleanupWithLogger(LOG, oldFile);
    }
    return true;
  }

  @Override // Closeable
  /**
   * 关闭NNStorage，解锁所有存储目录并清空目录列表。
   * @throws IOException 解锁失败抛出异常
   */
  public void close() throws IOException {
    unlockAll();
    getStorageDirs().clear();
  }

  /**
   * 设置是否在下次机会尝试恢复失败存储目录的标志。
   * @param val true表示需要尝试恢复，false表示不尝试
   */
  void setRestoreFailedStorage(boolean val) {
    LOG.warn("set restore failed storage to {}", val);
    restoreFailedStorage = val;
  }

  /**
   * 获取是否需要恢复失败存储目录的标志。
   * @return true表示需要尝试恢复，false表示不尝试
   */
  boolean getRestoreFailedStorage() {
    return restoreFailedStorage;
  }

  /**
   * 尝试恢复所有已移除的失败存储目录，如果目录重新可写则将其重新加入服务。
   */
  void attemptRestoreRemovedStorage() {
    // if directory is "alive" - copy the images there...
    if(!restoreFailedStorage || removedStorageDirs.size() == 0) {
      return; //nothing to restore
    }
    /* We don't want more than one thread trying to restore at a time */
    synchronized (this.restorationLock) {
      LOG.info("NNStorage.attemptRestoreRemovedStorage: check removed(failed) "+
               "storage. removedStorages size = {}", removedStorageDirs.size());
      for (StorageDirectory sd : this.removedStorageDirs) {
        File root = sd.getRoot();
        LOG.info("currently disabled dir {}; type={} ;canwrite={}", root
                .getAbsolutePath(), sd.getStorageDirType(),
            FileUtil.canWrite(root));
        if (root.exists() && FileUtil.canWrite(root)) {
          LOG.info("restoring dir {}", sd.getRoot().getAbsolutePath());
          this.addStorageDir(sd); // 恢复目录到可用列表
          this.removedStorageDirs.remove(sd);
        }
      }
    }
  }

  /**
   * 获取已失败被移除的存储目录列表。
   * @return 已移除存储目录列表
   */
  List<StorageDirectory> getRemovedStorageDirs() {
    return this.removedStorageDirs;
  }
  
  /**
   * 设置存储目录，仅用于测试。
   * @param fsNameDirs fsimage存储目录集合
   * @param fsEditsDirs edits存储目录集合
   * @throws IOException 初始化失败抛出异常
   */
  @VisibleForTesting
  synchronized void setStorageDirectories(Collection<URI> fsNameDirs,
                                          Collection<URI> fsEditsDirs)
      throws IOException {
    setStorageDirectories(fsNameDirs, fsEditsDirs, new ArrayList<>());
  }

  /**
   * 初始化并设置NameNode使用的存储目录，区分fsimage目录和edits目录，处理共享edits目录。
   * 该方法需要同步，避免多线程同时初始化存储目录导致冲突。
   * @param fsNameDirs fsimage存储目录URI集合
   * @param fsEditsDirs edits日志存储目录URI集合
   * @param sharedEditsDirs 共享edits目录URI集合
   * @throws IOException 目录初始化失败抛出异常
   */
  @VisibleForTesting
  synchronized void setStorageDirectories(Collection<URI> fsNameDirs,
                                          Collection<URI> fsEditsDirs,
                                          Collection<URI> sharedEditsDirs)
      throws IOException {
    getStorageDirs().clear();
    this.removedStorageDirs.clear();

   // Add all name dirs with appropriate NameNodeDirType
    for (URI dirName : fsNameDirs) {
      checkSchemeConsistency(dirName);
      boolean isAlsoEdits = false;
      for (URI editsDirName : fsEditsDirs) {
        if (editsDirName.compareTo(dirName) == 0) {
          isAlsoEdits = true;
          fsEditsDirs.remove(editsDirName);
          break;
        }
      }
      NameNodeDirType dirType = (isAlsoEdits) ?
                          NameNodeDirType.IMAGE_AND_EDITS :
                          NameNodeDirType.IMAGE;
      // Add to the list of storage directories, only if the
      // URI is of type file://
      if (dirName.getScheme().compareTo("file") == 0) {
        // Don't lock the dir if it's shared.
        StorageDirectory sd = new StorageDirectory(new File(dirName.getPath()),
            dirType,
            sharedEditsDirs.contains(dirName),
            new FsPermission(conf.get(
                DFSConfigKeys.DFS_NAMENODE_NAME_DIR_PERMISSION_KEY,
                DFSConfigKeys.DFS_NAMENODE_NAME_DIR_PERMISSION_DEFAULT)));

        this.addStorageDir(sd);
      }
    }

    // Add edits dirs if they are different from name dirs
    for (URI dirName : fsEditsDirs) {
      checkSchemeConsistency(dirName);
      // Add to the list of storage directories, only if the
      // URI is of type file://
      if (dirName.getScheme().compareTo("file") == 0) {
        this.addStorageDir(new StorageDirectory(new File(dirName.getPath()),
            NameNodeDirType.EDITS, sharedEditsDirs.contains(dirName),
            new FsPermission(conf.get(
                DFSConfigKeys.DFS_NAMENODE_NAME_DIR_PERMISSION_KEY,
                DFSConfigKeys.DFS_NAMENODE_NAME_DIR_PERMISSION_DEFAULT)));
      }
    }
  }

  /**
   * 根据URI查找对应的存储目录。
   * @param uri 存储目录URI
   * @return 匹配的StorageDirectory，如果未找到返回null
   */
  public StorageDirectory getStorageDirectory(URI uri) {
    try {
      uri = Util.fileAsURI(new File(uri));
      Iterator<StorageDirectory> it = dirIterator();
      while (it.hasNext()) {
        StorageDirectory sd = it.next();
        if (Util.fileAsURI(sd.getRoot()).equals(uri)) {
          return sd;
        }
      }
    } catch (IOException ioe) {
      LOG.warn("Error converting file to URI", ioe);
    }
    return null;
  }

  /**
   * 检查URI的scheme一致性，确保URI已定义scheme。
   * @param u 待检查的URI
   * @throws IOException 如果URI未定义scheme则抛出异常
   */
  private static void checkSchemeConsistency(URI u) throws IOException {
    String scheme = u.getScheme();
    // the URI should have a proper scheme
    if(scheme == null) {
      throw new IOException("Undefined scheme for " + u);
    }
  }

  /**
   * 获取所有IMAGE类型的存储目录URI集合。
   * @return 存储目录URI集合
   * @throws IOException URI转换错误抛出异常
   */
  Collection<URI> getImageDirectories() throws IOException {
    return getDirectories(NameNodeDirType.IMAGE);
  }

  /**
   * 获取所有EDITS类型的存储目录URI集合。
   * @return 存储目录URI集合
   * @throws IOException URI转换错误抛出异常
   */
  Collection<URI> getEditsDirectories() throws IOException {
    return getDirectories(NameNodeDirType.EDITS);
  }

  /**
   * 获取指定类型的存储目录数量。
   * @param dirType 目录类型
   * @return 对应类型的存储目录数量
   */
  int getNumStorageDirs(NameNodeDirType dirType) {
    if(dirType == null) {
      return getNumStorageDirs();
    }
    Iterator<StorageDirectory> it = dirIterator(dirType);
    int numDirs = 0;
    for(; it.hasNext(); it.next()) {
      numDirs++;
    }
    return numDirs;
  }

  /**
   * 获取指定类型存储目录的URI集合。
   * @param dirType 目录类型
   * @return 对应类型存储目录的URI集合
   * @throws IOException URI处理错误抛出异常
   */
  Collection<URI> getDirectories(NameNodeDirType dirType)
      throws IOException {
    ArrayList<URI> list = new ArrayList<>();
    Iterator<StorageDirectory> it = (dirType == null) ? dirIterator() :
                                    dirIterator(dirType);
    for ( ; it.hasNext();) {
      StorageDirectory sd = it.next();
      try {
        list.add(Util.fileAsURI(sd.getRoot()));
      } catch (IOException e) {
        throw new IOException("Exception while processing " +
            "StorageDirectory " + sd.getRoot(), e);
      }
    }
    return list;
  }
  
  /**
   * 从指定存储目录读取seen_txid文件，获取该目录记录的最大已处理事务ID。
   * @param sd 待读取的存储目录
   * @return 读取成功返回记录的txid，文件不存在返回0
   * @throws IOException 读取文件错误抛出异常
   */
  static long readTransactionIdFile(StorageDirectory sd) throws IOException {
    File txidFile = getStorageFile(sd, NameNodeFile.SEEN