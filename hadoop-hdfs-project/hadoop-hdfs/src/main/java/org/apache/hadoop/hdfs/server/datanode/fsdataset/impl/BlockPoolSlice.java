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
package org.apache.hadoop.hdfs.server.datanode.fsdataset.impl;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.RandomAccessFile;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.Scanner;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.RecursiveAction;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hdfs.server.datanode.FSCachingGetSpaceUsed;
import org.apache.hadoop.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CachingGetSpaceUsed;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.GetSpaceUsed;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockListAsLongs;
import org.apache.hadoop.hdfs.protocol.BlockListAsLongs.BlockReportReplica;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.ReplicaState;
import org.apache.hadoop.hdfs.server.datanode.FileIoProvider;
import org.apache.hadoop.hdfs.server.datanode.BlockMetadataHeader;
import org.apache.hadoop.hdfs.server.datanode.DataStorage;
import org.apache.hadoop.hdfs.server.datanode.DatanodeUtil;
import org.apache.hadoop.hdfs.server.datanode.ReplicaInfo;
import org.apache.hadoop.hdfs.server.datanode.ReplicaBuilder;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.RamDiskReplicaTracker.RamDiskReplica;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.ReplicaInputStreams;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.MultipleIOException;
import org.apache.hadoop.util.DataChecksum;
import org.apache.hadoop.util.DataChecksum.Type;
import org.apache.hadoop.util.DiskChecker;
import org.apache.hadoop.util.DiskChecker.DiskErrorException;
import org.apache.hadoop.util.ShutdownHookManager;
import org.apache.hadoop.util.Timer;

import org.apache.hadoop.classification.VisibleForTesting;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_DU_INTERVAL_KEY;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_GETSPACEUSED_JITTER_KEY;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_GETSPACEUSED_CLASSNAME;

/**
 * 文件所属模块：HDFS 数据节点存储层
 * 该类表示存储在单个卷上的一个块池切片，单个块池分散存储在集群所有DataNode的多个卷上，所有相同块池ID的BlockPoolSlice共同组成完整块池。
 * 本类负责管理本卷上该块池的目录结构、副本扫描加载、空间使用统计、副本去重等核心存储管理能力，由FsVolumeImpl进行外部同步控制。
 */
public class BlockPoolSlice {
  static final Logger LOG = LoggerFactory.getLogger(BlockPoolSlice.class);

  private final String bpid;
  private final FsVolumeImpl volume; // 该块池切片所属的存储卷
  private final File currentDir; // 对应路径：StorageDirectory/current/bpid/current
  // directory where finalized replicas are stored
  private final File finalizedDir;
  private final File lazypersistDir;
  private final File rbwDir; // 存储RBW（正在写入）副本的目录
  private final File tmpDir; // 存储临时副本的目录
  private final int ioFileBufferSize;
  @VisibleForTesting
  public static final String DU_CACHE_FILE = "dfsUsed";
  private final Runnable shutdownHook;
  private volatile boolean dfsUsedSaved = false;
  private static final int SHUTDOWN_HOOK_PRIORITY = 30;

  /**
   * Only tests are allowed to modify the value. For source code,
   * this should be treated as final only.
   */
  private boolean deleteDuplicateReplicas;
  private static final String REPLICA_CACHE_FILE = "replicas";
  private final long replicaCacheExpiry;
  private final File replicaCacheDir;
  private AtomicLong numOfBlocks = new AtomicLong();
  private final long cachedDfsUsedCheckTime;
  private final Timer timer;
  private final int maxDataLength;
  private final FileIoProvider fileIoProvider;
  private final Configuration config;
  private final File bpDir;

  private static ForkJoinPool addReplicaThreadPool = null;
  private static final int VOLUMES_REPLICA_ADD_THREADPOOL_SIZE = Runtime
      .getRuntime().availableProcessors();
  private static final Comparator<File> FILE_COMPARATOR =
      new Comparator<File>() {
    @Override
    public int compare(File f1, File f2) {
      return f1.getName().compareTo(f2.getName());
    }
  };

  // TODO:FEDERATION scalability issue - a thread per DU is needed
  private volatile GetSpaceUsed dfsUsage;

  /**
   * 构造块池切片，初始化各个存储目录，加载缓存的空间使用信息，注册关闭钩子
   * @param bpid 块池ID
   * @param volume 该块池切片所属的存储卷
   * @param bpDir 块池对应根目录
   * @param conf 配置对象
   * @param timer 获取时间的工具对象
   * @throws IOException 初始化目录失败时抛出IO异常
   */
  BlockPoolSlice(String bpid, FsVolumeImpl volume, File bpDir,
      Configuration conf, Timer timer) throws IOException {
    this.config = conf;
    this.bpDir = bpDir;
    this.bpid = bpid;
    this.volume = volume;
    this.fileIoProvider = volume.getFileIoProvider();
    this.currentDir = new File(bpDir, DataStorage.STORAGE_DIR_CURRENT);
    this.finalizedDir = new File(
        currentDir, DataStorage.STORAGE_DIR_FINALIZED);
    this.lazypersistDir = new File(currentDir, DataStorage.STORAGE_DIR_LAZY_PERSIST);
    if (!this.finalizedDir.exists()) {
      if (!this.finalizedDir.mkdirs()) {
        throw new IOException("Failed to mkdirs " + this.finalizedDir);
      }
    }

    this.ioFileBufferSize = DFSUtilClient.getIoFileBufferSize(conf);

    this.deleteDuplicateReplicas = conf.getBoolean(
        DFSConfigKeys.DFS_DATANODE_DUPLICATE_REPLICA_DELETION,
        DFSConfigKeys.DFS_DATANODE_DUPLICATE_REPLICA_DELETION_DEFAULT);

    this.cachedDfsUsedCheckTime =
        conf.getLong(
            DFSConfigKeys.DFS_DN_CACHED_DFSUSED_CHECK_INTERVAL_MS,
            DFSConfigKeys.DFS_DN_CACHED_DFSUSED_CHECK_INTERVAL_DEFAULT_MS);

    this.maxDataLength = conf.getInt(
        CommonConfigurationKeys.IPC_MAXIMUM_DATA_LENGTH,
        CommonConfigurationKeys.IPC_MAXIMUM_DATA_LENGTH_DEFAULT);

    this.timer = timer;

    // Files that were being written when the datanode was last shutdown
    // are now moved back to the data directory. It is possible that
    // in the future, we might want to do some sort of datanode-local
    // recovery for these blocks. For example, crc validation.
    //
    this.tmpDir = new File(bpDir, DataStorage.STORAGE_DIR_TMP);
    if (tmpDir.exists()) {
      fileIoProvider.fullyDelete(volume, tmpDir);
    }
    this.rbwDir = new File(currentDir, DataStorage.STORAGE_DIR_RBW);

    // create the rbw and tmp directories if they don't exist.
    fileIoProvider.mkdirs(volume, rbwDir);
    fileIoProvider.mkdirs(volume, tmpDir);

    String cacheDirRoot = conf.get(
        DFSConfigKeys.DFS_DATANODE_REPLICA_CACHE_ROOT_DIR_KEY);
    if (cacheDirRoot != null && !cacheDirRoot.isEmpty()) {
      this.replicaCacheDir = new File(cacheDirRoot,
          currentDir.getCanonicalPath());
      if (!this.replicaCacheDir.exists()) {
        if (!this.replicaCacheDir.mkdirs()) {
          throw new IOException("Failed to mkdirs " + this.replicaCacheDir);
        }
      }
    } else {
      this.replicaCacheDir = currentDir;
    }
    this.replicaCacheExpiry = conf.getTimeDuration(
        DFSConfigKeys.DFS_DATANODE_REPLICA_CACHE_EXPIRY_TIME_KEY,
        DFSConfigKeys.DFS_DATANODE_REPLICA_CACHE_EXPIRY_TIME_DEFAULT,
        TimeUnit.MILLISECONDS);

    // Use cached value initially if available. Or the following call will
    // block until the initial du command completes.
    this.dfsUsage = new FSCachingGetSpaceUsed.Builder().setBpid(bpid)
            .setVolume(volume)
            .setPath(bpDir)
            .setConf(conf)
            .setInitialUsed(loadDfsUsed())
            .build();


    if (addReplicaThreadPool == null) {
      // initialize add replica fork join pool
      initializeAddReplicaPool(conf, (FsDatasetImpl) volume.getDataset());
    }
    // Make the dfs usage to be saved during shutdown.
    shutdownHook = new Runnable() {
      @Override
      public void run() {
        if (!dfsUsedSaved) {
          saveDfsUsed();
          addReplicaThreadPool.shutdownNow();
        }
      }
    };
    ShutdownHookManager.get().addShutdownHook(shutdownHook,
        SHUTDOWN_HOOK_PRIORITY);
  }

  /**
   * 更新空间使用统计的配置，重新创建空间统计对象
   * @param interval 新的DU统计间隔，null则不修改
   * @param jitter 新的DU统计抖动，null则不修改
   * @param klass 新的GetSpaceUsed实现类，null则不修改
   * @throws IOException 创建新空间统计对象失败时抛出异常
   */
  public void updateDfsUsageConfig(Long interval, Long jitter, Class<? extends GetSpaceUsed> klass)
          throws IOException {
    // Close the old dfsUsage if it is CachingGetSpaceUsed.
    if (dfsUsage instanceof CachingGetSpaceUsed) {
      ((CachingGetSpaceUsed) dfsUsage).close();
    }
    if (interval != null) {
      Preconditions.checkArgument(interval > 0,
          FS_DU_INTERVAL_KEY + " should be larger than 0");
      config.setLong(FS_DU_INTERVAL_KEY, interval);
    }
    if (jitter != null) {
      Preconditions.checkArgument(jitter >= 0,
          FS_GETSPACEUSED_JITTER_KEY + " should be larger than or equal to 0");
      config.setLong(FS_GETSPACEUSED_JITTER_KEY, jitter);
    }

    if (klass != null) {
      config.setClass(FS_GETSPACEUSED_CLASSNAME, klass, CachingGetSpaceUsed.class);
    }
    // Start new dfsUsage.
    this.dfsUsage = new FSCachingGetSpaceUsed.Builder().setBpid(bpid)
        .setVolume(volume)
        .setPath(bpDir)
        .setConf(config)
        .setInitialUsed(loadDfsUsed())
        .build();
  }

  @VisibleForTesting
  public GetSpaceUsed getDfsUsage() {
    return dfsUsage;
  }

  /**
   * 静态初始化添加副本的ForkJoin线程池，供所有块池切片并发扫描副本使用
   * @param conf 配置对象
   * @param dataset FsDatasetImpl数据集合对象
   */
  private synchronized static void initializeAddReplicaPool(Configuration conf,
      FsDatasetImpl dataset) {
    if (addReplicaThreadPool == null) {
      int numberOfBlockPoolSlice = dataset.getVolumeCount()
          * dataset.getBPServiceCount();
      int poolsize = Math.max(numberOfBlockPoolSlice,
          VOLUMES_REPLICA_ADD_THREADPOOL_SIZE);
      // Default pool sizes is max of (volume * number of bp_service) and
      // number of processor.
      addReplicaThreadPool = new ForkJoinPool(conf.getInt(
          DFSConfigKeys.DFS_DATANODE_VOLUMES_REPLICA_ADD_THREADPOOL_SIZE_KEY,
          poolsize));
    }
  }

  File getDirectory() {
    return currentDir.getParentFile();
  }

  File getFinalizedDir() {
    return finalizedDir;
  }

  File getLazypersistDir() {
    return lazypersistDir;
  }

  File getRbwDir() {
    return rbwDir;
  }

  File getTmpDir() {
    return tmpDir;
  }

  /**
   * 减少已使用空间统计值，必须由调用者保证同步
   * @param value 要减去的空间大小（字节）
   */
  void decDfsUsed(long value) {
    if (dfsUsage instanceof CachingGetSpaceUsed) {
      ((CachingGetSpaceUsed)dfsUsage).incDfsUsed(-value);
    }
  }

  /**
   * 获取当前块池切片已使用空间大小
   * @return 已使用字节数
   * @throws IOException 获取空间信息失败时抛出IO异常
   */
  long getDfsUsed() throws IOException {
    return dfsUsage.getUsed();
  }

  /**
   * 增加已使用空间统计值
   * @param value 要增加的空间大小（字节）
   */
  void incDfsUsed(long value) {
    if (dfsUsage instanceof CachingGetSpaceUsed) {
      ((CachingGetSpaceUsed)dfsUsage).incDfsUsed(value);
    }
  }

  /**
   * 从缓存文件加载已使用空间值，如果缓存存在且未过期则返回缓存值，否则返回-1触发重新统计
   * 缓存值一定程度的不影响不影响核心逻辑，跳过DU统计可以大幅缩短DataNode启动时间
   * @return 缓存的已使用字节数，缓存无效时返回-1
   */
  long loadDfsUsed() {
    long cachedDfsUsed;
    long mtime;
    Scanner sc;

    File duCacheFile = new File(currentDir, DU_CACHE_FILE);
    try {
      sc = new Scanner(duCacheFile, "UTF-8");
    } catch (FileNotFoundException fnfe) {
      FsDatasetImpl.LOG.warn("{} file missing in {}, will proceed with Du " +
              "for space computation calculation, ",
              DU_CACHE_FILE, currentDir);
      return -1;
    }

    try {
      // Get the recorded dfsUsed from the file.
      if (sc.hasNextLong()) {
        cachedDfsUsed = sc.nextLong();
      } else {
        FsDatasetImpl.LOG.warn("cachedDfsUsed not found in file:{}, will " +
                "proceed with Du for space computation calculation, ",
                duCacheFile);
        return -1;
      }
      // Get the recorded mtime from the file.
      if (sc.hasNextLong()) {
        mtime = sc.nextLong();
      } else {
        FsDatasetImpl.LOG.warn("mtime not found in file:{}, will proceed" +
                " with Du for space computation calculation, ", duCacheFile);
        return -1;
      }

      long elapsedTime = timer.now() - mtime;
      // Return the cached value if mtime is okay.
      if (mtime > 0 && (elapsedTime < cachedDfsUsedCheckTime)) {
        FsDatasetImpl.LOG.info("Cached dfsUsed found for " + currentDir + ": " +
            cachedDfsUsed);
        return cachedDfsUsed;
      }
      FsDatasetImpl.LOG.warn("elapsed time:{} is greater than threshold:{}," +
                      " mtime:{} in file:{}, will proceed with Du for space" +
                      " computation calculation",
              elapsedTime, cachedDfsUsedCheckTime, mtime