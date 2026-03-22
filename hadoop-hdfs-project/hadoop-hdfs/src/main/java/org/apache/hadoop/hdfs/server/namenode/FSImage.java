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

import static org.apache.hadoop.util.Time.now;

import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.util.ShutdownHookManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.HAUtil;
import org.apache.hadoop.hdfs.protocol.LayoutVersion;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.NamenodeRole;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.RollingUpgradeStartupOption;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.StartupOption;
import org.apache.hadoop.hdfs.server.common.InconsistentFSStateException;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.common.Storage.FormatConfirmable;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import org.apache.hadoop.hdfs.server.common.Storage.StorageState;
import org.apache.hadoop.hdfs.server.common.Util;
import org.apache.hadoop.hdfs.server.namenode.FSImageStorageInspector.FSImageFile;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.NameNodeDirType;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.NameNodeFile;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.Phase;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.StartupProgress;
import org.apache.hadoop.hdfs.server.protocol.CheckpointCommand;
import org.apache.hadoop.hdfs.server.protocol.NamenodeCommand;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.NamenodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.util.Canceler;
import org.apache.hadoop.hdfs.util.MD5FileUtils;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.log.LogThrottlingHelper;
import org.apache.hadoop.log.LogThrottlingHelper.LogAction;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.util.Lists;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.util.concurrent.SubjectInheritingThread;

import org.apache.hadoop.thirdparty.com.google.common.base.Joiner;
import org.apache.hadoop.util.Preconditions;

/**
 * 文件系统镜像（FSImage）类，负责HDFS命名空间元数据的检查点持久化和编辑日志管理
 * 
 * FSImage存储了HDFS整个文件系统命名空间的快照，包括所有目录、文件的元数据信息，
 * 在NameNode启动时负责加载镜像和应用编辑日志，恢复命名空间到最新状态
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class FSImage implements Closeable {
  public static final Logger LOG =
      LoggerFactory.getLogger(FSImage.class.getName());

  /**
   * FSImage保存关闭钩子的优先级
   */
  public static final int SHUTDOWN_HOOK_PRIORITY = 10;

  protected FSEditLog editLog = null;
  private boolean isUpgradeFinalized = false;

  // If true, then image corruption was detected. The NameNode process will
  // exit immediately after saving the image.
  private AtomicBoolean exitAfterSave = new AtomicBoolean(false);

  protected NNStorage storage;
  
  /**
   * 最新已应用的事务ID，可能来自加载镜像或编辑日志
   */
  protected long lastAppliedTxId = 0;

  final private Configuration conf;

  protected NNStorageRetentionManager archivalManager;

  /**
   * 新增存储目录集合，用于HA场景下延迟创建VERSION文件。
   * HA模式下，新增目录先做部分格式化，等到下一次检查点时再补全VERSION文件；
   * 非HA模式下会在启动时生成新镜像同时创建VERSION文件，因此不使用该集合。
   * VERSION文件创建后该集合会被清空。
   */
  private Set<StorageDirectory> newDirs = null;

  /* Used to make sure there are no concurrent checkpoints for a given txid
   * The checkpoint here could be one of the following operations.
   * a. checkpoint when NN is in standby.
   * b. admin saveNameSpace operation.
   * c. download checkpoint file from any remote checkpointer.
  */
  private final Set<Long> currentlyCheckpointing =
      Collections.<Long>synchronizedSet(new HashSet<Long>());

  /** Limit logging about edit loading to every 5 seconds max. */
  private static final long LOAD_EDIT_LOG_INTERVAL_MS = 5000;
  private final LogThrottlingHelper loadEditLogHelper =
      new LogThrottlingHelper(LOAD_EDIT_LOG_INTERVAL_MS);

  /**
   * 构造FSImage对象，从配置中读取默认目录
   * @param conf Hadoop配置对象
   * @throws IOException 如果默认目录无效则抛出异常
   */
  public FSImage(Configuration conf) throws IOException {
    this(conf,
         FSNamesystem.getNamespaceDirs(conf),
         FSNamesystem.getNamespaceEditsDirs(conf));
  }

  /**
   * 构造FSImage对象，指定存储镜像和编辑日志的目录，初始化存储和编辑日志
   * @param conf Hadoop配置对象
   * @param imageDirs 存储FSImage的目录URI集合
   * @param editsDirs 存储编辑日志的目录URI列表
   * @throws IOException 如果目录无效则抛出异常
   */
  protected FSImage(Configuration conf,
                    Collection<URI> imageDirs,
                    List<URI> editsDirs)
      throws IOException {
    this.conf = conf;

    storage = new NNStorage(conf, imageDirs, editsDirs);
    if(conf.getBoolean(DFSConfigKeys.DFS_NAMENODE_NAME_DIR_RESTORE_KEY,
                       DFSConfigKeys.DFS_NAMENODE_NAME_DIR_RESTORE_DEFAULT)) {
      // 开启故障存储目录自动恢复功能
      storage.setRestoreFailedStorage(true);
    }

    this.editLog = FSEditLog.newInstance(conf, storage, editsDirs);
    archivalManager = new NNStorageRetentionManager(conf, storage, editLog);
    FSImageFormatProtobuf.initParallelLoad(conf);
  }
 
  /**
   * 格式化FSImage，创建全新的空命名空间
   * @param fsn 目标命名系统对象
   * @param clusterId 集群ID
   * @param force 是否强制格式化
   * @throws IOException 格式化过程中IO异常
   */
  void format(FSNamesystem fsn, String clusterId, boolean force)
      throws IOException {
    long fileCount = fsn.getFilesTotal();
    // Expect 1 file, which is the root inode
    Preconditions.checkState(fileCount == 1,
        "FSImage.format should be called with an uninitialized namesystem, has " +
        fileCount + " files");
    NamespaceInfo ns = NNStorage.newNamespaceInfo();
    LOG.info("Allocated new BlockPoolId: " + ns.getBlockPoolID());
    ns.clusterID = clusterId;
    
    storage.format(ns);
    editLog.formatNonFileJournals(ns, force);
    saveFSImageInAllDirs(fsn, 0);
  }
  
  /**
   * 确认是否可以执行格式化操作，交互模式下会提示用户确认已有目录
   * @param force 是否强制格式化，忽略目录已存在
   * @param interactive 是否交互模式，提示用户确认
   * @return true表示可以继续执行格式化
   * @throws IOExceptions 存储访问异常
   */
  boolean confirmFormat(boolean force, boolean interactive) throws IOException {
    List<FormatConfirmable> confirms = Lists.newArrayList();
    for (StorageDirectory sd : storage.dirIterable(null)) {
      confirms.add(sd);
    }
    
    confirms.addAll(editLog.getFormatConfirmables());
    return Storage.confirmFormat(confirms, force, interactive);
  }
  
  /**
   * 恢复并读取存储目录状态，根据启动选项执行状态转换，准备加载FSImage
   * @param startOpt 启动选项
   * @param target 目标命名系统
   * @param recovery 元数据恢复上下文
   * @return true表示需要重新保存镜像，false否则
   * @throws IOException 存储读取或状态转换异常
   */
  boolean recoverTransitionRead(StartupOption startOpt, FSNamesystem target,
      MetaRecoveryContext recovery)
      throws IOException {
    assert startOpt != StartupOption.FORMAT : 
      "NameNode formatting should be performed before reading the image";
    
    Collection<URI> imageDirs = storage.getImageDirectories();
    Collection<URI> editsDirs = editLog.getEditURIs();

    // none of the data dirs exist
    if((imageDirs.size() == 0 || editsDirs.size() == 0) 
                             && startOpt != StartupOption.IMPORT)  
      throw new IOException(
          "All specified directories are not accessible or do not exist.");
    
    // 1. 计算每个数据目录的状态，检查转换前一致性
    Map<StorageDirectory, StorageState> dataDirStates = 
             new HashMap<StorageDirectory, StorageState>();
    boolean isFormatted = recoverStorageDirs(startOpt, storage, dataDirStates);

    if (LOG.isTraceEnabled()) {
      LOG.trace("Data dir states:\n  " +
        Joiner.on("\n  ").withKeyValueSeparator(": ")
        .join(dataDirStates));
    }
    
    if (!isFormatted && startOpt != StartupOption.ROLLBACK 
                     && startOpt != StartupOption.IMPORT) {
      throw new IOException("NameNode is not formatted.");      
    }


    int layoutVersion = storage.getLayoutVersion();
    if (startOpt == StartupOption.METADATAVERSION) {
      // 仅输出版本信息后退出
      System.out.println("HDFS Image Version: " + layoutVersion);
      System.out.println("Software format version: " +
          storage.getServiceLayoutVersion());
      return false;
    }

    if (layoutVersion < Storage.LAST_PRE_UPGRADE_LAYOUT_VERSION) {
      NNStorage.checkVersionUpgradable(storage.getLayoutVersion());
    }
    if (startOpt != StartupOption.UPGRADE
        && startOpt != StartupOption.UPGRADEONLY
        && !RollingUpgradeStartupOption.STARTED.matches(startOpt)
        && layoutVersion < Storage.LAST_PRE_UPGRADE_LAYOUT_VERSION
        && layoutVersion != storage.getServiceLayoutVersion()) {
      throw new IOException(
          "\nFile system image contains an old layout version " 
          + storage.getLayoutVersion() + ".\nAn upgrade to version "
          + storage.getServiceLayoutVersion() + " is required.\n"
          + "Please restart NameNode with the \""
          + RollingUpgradeStartupOption.STARTED.getOptionString()
          + "\" option if a rolling upgrade is already started;"
          + " or restart NameNode with the \""
          + StartupOption.UPGRADE.getName() + "\" option to start"
          + " a new upgrade.");
    }
    
    storage.processStartupOptionsForUpgrade(startOpt, layoutVersion);

    // 2. 格式化未格式化的目录
    for (Iterator<StorageDirectory> it = storage.dirIterator(); it.hasNext();) {
      StorageDirectory sd = it.next();
      StorageState curState = dataDirStates.get(sd);
      switch(curState) {
      case NON_EXISTENT:
        throw new IOException(StorageState.NON_EXISTENT + 
                              " state cannot be here");
      case NOT_FORMATTED:
        // 只创建目录结构，暂不写VERSION文件。
        // HA模式下StandBy NameNode不会触发saveNamespace，因此保存目录延后创建VERSION
        LOG.info("Storage directory " + sd.getRoot() + " is not formatted.");
        LOG.info("Formatting ...");
        sd.clearDirectory(); // 创建空current目录
        if (!target.isHaEnabled()) {
          // 非HA模式，后续saveNamespace会处理剩余步骤
          continue;
        }
        // HA模式下保存目录，等到下次检查点再创建VERSION文件
        if (newDirs == null) {
          newDirs = new HashSet<StorageDirectory>();
        }
        newDirs.add(sd);
        break;
      default:
        break;
      }
    }

    // 3. 根据启动选项执行状态转换
    switch(startOpt) {
    case UPGRADE:
    case UPGRADEONLY:
      doUpgrade(target);
      return false; // 升级过程已保存镜像
    case IMPORT:
      doImportCheckpoint(target);
      return false; // 导入检查点过程已保存镜像
    case ROLLBACK:
      throw new AssertionError("Rollback is now a standalone command, " +
          "NameNode should not be started with this option.");
    case REGULAR:
    default:
      // 仅加载镜像
    }
    
    return loadFSImage(target, startOpt, recovery);
  }

  /**
   * 在新增存储目录中初始化VERSION文件
   */
  private void initNewDirs() {
    if (newDirs == null) {
      return;
    }
    for (StorageDirectory sd : newDirs) {
      try {
        storage.writeProperties(sd);
        LOG.info("Wrote VERSION in the new storage, " + sd.getCurrentDir());
      } catch (IOException e) {
        // 创建VERSION失败，上报目录错误
        storage.reportErrorOnFile(sd.getVersionFile());
      }
    }
    newDirs.clear();
    newDirs = null;
  }

  /**
   * 恢复每个存储目录的不完整转换（升级、回滚、检查点），获取目录状态
   * @param startOpt 启动选项
   * @param storage NN存储对象
   * @param dataDirStates 输出参数，存储目录到状态的映射
   * @return true如果至少有一个有效的已格式化存储目录
   * @throws IOExceptions 存储分析异常
   */
  public static boolean recoverStorageDirs(StartupOption startOpt,
      NNStorage storage, Map<StorageDirectory, StorageState> dataDirStates)
      throws IOException {
    boolean isFormatted = false;
    // 需要遍历所有存储目录包括共享目录，确保状态分析完整
    for (Iterator<StorageDirectory> it = 
                      storage.dirIterator(); it.hasNext();) {
      StorageDirectory sd = it.next();
      StorageState curState;
      if (startOpt == StartupOption.METADATAVERSION) {
        /* All we need is the layout version. */
        storage.readProperties(sd);
        return true;
      }

      try {
        curState = sd.analyzeStorage(startOpt, storage);
        // sd已加锁但未打开
        switch(curState) {
        case NON_EXISTENT:
          // 任何配置的存储目录不存在都会导致NameNode启动失败
          throw new InconsistentFSStateException(sd.getRoot(),
                      "storage directory does not exist or is not accessible.");
        case NOT_FORMATTED:
          break;
        case NORMAL:
          break;
        default:  // 需要恢复，执行恢复操作
          sd.doRecover(curState);
        }
        if (curState != StorageState.NOT_FORMATTED 
            && startOpt != StartupOption.ROLLBACK) {
          // 读取并验证和其他目录的一致性
          storage.readProperties(sd, startOpt);
          isFormatted = true;
        }
        if (startOpt == StartupOption.IMPORT && isFormatted)
          // 导入检查点只能导入到空目录，已有镜像则报错
          throw new IOException("Cannot import image from a checkpoint. " 
              + " NameNode already contains an image in " + sd.getRoot());
      } catch (IOException ioe) {
        sd.unlock();
        throw ioe;
      }
      dataDirStates.put(sd,curState);
    }
    return isFormatted;
  }

  /**
   * 检查升级过程是否符合要求，升级过程不能有旧的文件系统状态存在
   * @param storage NN存储对象
   * @throws IOException 如果存在旧状态抛出异常
   */
  public