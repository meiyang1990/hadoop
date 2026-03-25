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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.RandomAccessFile;
import java.net.URI;
import java.nio.channels.ClosedChannelException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.DF;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockListAsLongs;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.ReplicaState;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import org.apache.hadoop.hdfs.server.datanode.BlockMetadataHeader;
import org.apache.hadoop.hdfs.server.datanode.DataStorage;
import org.apache.hadoop.hdfs.server.datanode.DatanodeUtil;
import org.apache.hadoop.hdfs.server.datanode.DirectoryScanner.ReportCompiler;
import org.apache.hadoop.hdfs.server.datanode.FileIoProvider;
import org.apache.hadoop.hdfs.server.datanode.FinalizedReplica;
import org.apache.hadoop.hdfs.server.datanode.LocalReplica;
import org.apache.hadoop.hdfs.server.datanode.LocalReplicaInPipeline;
import org.apache.hadoop.hdfs.server.datanode.ReplicaBeingWritten;
import org.apache.hadoop.hdfs.server.datanode.ReplicaBuilder;
import org.apache.hadoop.hdfs.server.datanode.ReplicaInPipeline;
import org.apache.hadoop.hdfs.server.datanode.ReplicaInfo;
import org.apache.hadoop.hdfs.server.datanode.StorageLocation;
import org.apache.hadoop.hdfs.server.datanode.checker.VolumeCheckResult;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.DataNodeVolumeMetrics;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeReference;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.RamDiskReplicaTracker.RamDiskReplica;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.apache.hadoop.util.CloseableReferenceCount;
import org.apache.hadoop.util.DataChecksum;
import org.apache.hadoop.util.DiskChecker.DiskErrorException;
import org.apache.hadoop.util.DiskChecker.DiskOutOfSpaceException;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.util.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.base.Joiner;
import org.apache.hadoop.util.Preconditions;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * 文件级注释：HDFS DataNode 上单个存储卷的实现类，负责管理该存储卷上所有块副本的存储与生命周期管理
 * 该类使用 {@link FsDatasetImpl} 对象进行同步控制。
 */
@InterfaceAudience.Private
@VisibleForTesting
public class FsVolumeImpl implements FsVolumeSpi {
  public static final Logger LOG =
      LoggerFactory.getLogger(FsVolumeImpl.class);
  private static final ObjectWriter WRITER =
      new ObjectMapper().writerWithDefaultPrettyPrinter();
  private static final ObjectReader READER =
      new ObjectMapper().readerFor(BlockIteratorState.class);

  private final FsDatasetImpl dataset;
  private final String storageID;
  private final StorageType storageType;
  private final Map<String, BlockPoolSlice> bpSlices
      = new ConcurrentHashMap<String, BlockPoolSlice>();

  // 指向构造该存储卷的原始存储位置，不包含后续拼接的current目录路径
  private final StorageLocation storageLocation;

  private final File currentDir;    // <StorageDirectory>/current 目录，存储所有块池数据
  private final DF usage;
  private final ReservedSpaceCalculator reserved;
  private long cachedCapacity;
  private CloseableReferenceCount reference = new CloseableReferenceCount();

  // 为正在写入或正在重复制的副本预保留的磁盘空间
  private AtomicLong reservedForReplicas;
  private long recentReserved = 0;
  private final Configuration conf;
  // 配置的容量上限，用于测试限制容量。如果为负数则直接从文件系统获取实际容量
  protected volatile long configuredCapacity;
  private final FileIoProvider fileIoProvider;
  private final DataNodeVolumeMetrics metrics;
  private URI baseURI;
  private boolean enableSameDiskTiering;
  private final String mount;
  private double reservedForArchive;

  /**
   * 每个存储卷独立的线程池，处理缓存块的异步加载任务
   * 每个卷的最大线程数通过配置项 dfs.datanode.fsdatasetcache.max.threads.per.volume 限制，避免资源争用
   */
  protected ThreadPoolExecutor cacheExecutor;

  /**
   * 构造FsVolumeImpl，对外暴露的构造方法
   */
  FsVolumeImpl(FsDatasetImpl dataset, String storageID, StorageDirectory sd,
      FileIoProvider fileIoProvider, Configuration conf) throws IOException {
    // 非测试场景下，usage会在ReservedSpaceCalculator.Builder中创建
    this(dataset, storageID, sd, fileIoProvider, conf, null);
  }

  /**
   * 内部构造方法，支持注入DF对象用于测试
   */
  FsVolumeImpl(FsDatasetImpl dataset, String storageID, StorageDirectory sd,
      FileIoProvider fileIoProvider, Configuration conf, DF usage)
      throws IOException {

    if (sd.getStorageLocation() == null) {
      throw new IOException("StorageLocation specified for storage directory " +
          sd + " is null");
    }
    this.dataset = dataset;
    this.storageID = storageID;
    this.reservedForReplicas = new AtomicLong(0L);
    this.storageLocation = sd.getStorageLocation();
    this.currentDir = sd.getCurrentDir();
    this.storageType = storageLocation.getStorageType();
    this.configuredCapacity = -1;
    this.usage = usage;
    if (this.usage != null) {
      // 构建预留空间计算器
      reserved = new ReservedSpaceCalculator.Builder(conf)
          .setUsage(this.usage).setStorageType(storageType)
          .setDir(currentDir != null ? currentDir.getParent() : "NULL").build();
      boolean fixedSizeVolume = conf.getBoolean(
          DFSConfigKeys.DFS_DATANODE_FIXED_VOLUME_SIZE_KEY,
          DFSConfigKeys.DFS_DATANODE_FIXED_VOLUME_SIZE_DEFAULT);
      if (fixedSizeVolume) {
        // 固定容量卷，提前缓存容量值
        cachedCapacity = this.usage.getCapacity();
      }
    } else {
      reserved = null;
      LOG.warn("Setting reserved to null as usage is null");
      cachedCapacity = -1;
    }
    if (currentDir != null) {
      File parent = currentDir.getParentFile();
      // 初始化块缓存线程池
      cacheExecutor = initializeCacheExecutor(parent);
      // 创建该卷的指标收集器
      this.metrics = DataNodeVolumeMetrics.create(conf, parent.getPath());
      // 初始化基础URI
      this.baseURI = new File(currentDir.getParent()).toURI();
    } else {
      cacheExecutor = null;
      this.metrics = null;
    }
    this.conf = conf;
    this.fileIoProvider = fileIoProvider;
    // 加载同磁盘分层存储配置
    this.enableSameDiskTiering =
        conf.getBoolean(DFSConfigKeys.DFS_DATANODE_ALLOW_SAME_DISK_TIERING,
            DFSConfigKeys.DFS_DATANODE_ALLOW_SAME_DISK_TIERING_DEFAULT);
    if (enableSameDiskTiering && usage != null) {
      // 开启同磁盘分层时，记录该卷所在挂载点
      this.mount = usage.getMount();
    } else {
      mount = "";
    }
  }

  /**
   * 获取该存储卷所在挂载点路径，用于同磁盘分层存储匹配
   */
  String getMount() {
    return mount;
  }

  /**
   * 初始化块缓存处理线程池，仅对非RAM存储且非测试场景创建线程池
   */
  protected ThreadPoolExecutor initializeCacheExecutor(File parent) {
    // RAM存储不需要缓存线程池
    if (storageType.isRAM()) {
      return null;
    }
    // 测试场景下datanode为null，不创建线程池
    if (dataset.datanode == null) {
      // FsVolumeImpl is used in test.
      return null;
    }

    // 从配置获取每个卷允许的最大线程数
    final int maxNumThreads = dataset.datanode.getConf().getInt(
        DFSConfigKeys.DFS_DATANODE_FSDATASETCACHE_MAX_THREADS_PER_VOLUME_KEY,
        DFSConfigKeys.DFS_DATANODE_FSDATASETCACHE_MAX_THREADS_PER_VOLUME_DEFAULT);

    // 转义路径中的百分号，避免格式化问题
    String escapedPath = parent.toString().replaceAll("%", "%%");
    // 创建线程工厂，设置线程名格式
    ThreadFactory workerFactory = new ThreadFactoryBuilder()
        .setDaemon(true)
        .setNameFormat("FsVolumeImplWorker-" + escapedPath + "-%d")
        .build();
    // 创建线程池，核心线程1个，最大线程数按配置，空闲线程60秒超时
    ThreadPoolExecutor executor = new ThreadPoolExecutor(
        1, maxNumThreads,
        60, TimeUnit.SECONDS,
        new LinkedBlockingQueue<Runnable>(),
        workerFactory);
    // 允许核心线程超时回收
    executor.allowCoreThreadTimeOut(true);
    return executor;
  }

  /**
   * 打印引用计数变更的追踪日志，跳过常见的查询操作避免日志冗余
   */
  private void printReferenceTraceInfo(String op) {
    StackTraceElement[] stack = Thread.currentThread().getStackTrace();
    for (StackTraceElement ste : stack) {
      switch (ste.getMethodName()) {
      case "getDfsUsed":
      case "getBlockPoolUsed":
      case "getAvailable":
      case "getVolumeMap":
        // 常见查询操作跳过日志
        return;
      default:
        break;
      }
    }
    FsDatasetImpl.LOG.trace("Reference count: " + op + " " + this + ": " +
        this.reference.getReferenceCount());
    FsDatasetImpl.LOG.trace(
        Joiner.on("\n").join(Thread.currentThread().getStackTrace()));
  }

  /**
   * 增加存储卷引用计数，IO操作前必须调用该方法
   * @throws ClosedChannelException 如果存储卷已经关闭则抛出异常
   */
  private void reference() throws ClosedChannelException {
    this.reference.reference();
    if (FsDatasetImpl.LOG.isTraceEnabled()) {
      printReferenceTraceInfo("incr");
    }
  }

  /**
   * 减少存储卷引用计数，IO完成后调用
   */
  private void unreference() {
    if (FsDatasetImpl.LOG.isTraceEnabled()) {
      printReferenceTraceInfo("desc");
    }
    if (FsDatasetImpl.LOG.isDebugEnabled()) {
      if (reference.getReferenceCount() <= 0) {
        FsDatasetImpl.LOG.debug("Decrease reference count <= 0 on " + this +
          Joiner.on("\n").join(Thread.currentThread().getStackTrace()));
      }
    }
    checkReference();
    this.reference.unreference();
  }

  /**
   * FsVolumeReference实现类，封装存储卷引用，实现AutoCloseable接口自动管理引用计数
   */
  private static class FsVolumeReferenceImpl implements FsVolumeReference {
    private FsVolumeImpl volume;

    /**
     * 构造引用并增加存储卷引用计数
     */
    FsVolumeReferenceImpl(FsVolumeImpl volume) throws ClosedChannelException {
      this.volume = volume;
      volume.reference();
    }

    /**
     * 关闭引用，减少存储卷引用计数
     * @throws IOException 从不抛出异常
     */
    @Override
    public void close() throws IOException {
      if (volume != null) {
        volume.unreference();
        volume = null;
      }
    }

    @Override
    public FsVolumeSpi getVolume() {
      return this.volume;
    }
  }

  @Override
  public FsVolumeReference obtainReference() throws ClosedChannelException {
    return new FsVolumeReferenceImpl(this);
  }

  /**
   * 检查引用计数合法性，确保当前引用计数大于0
   */
  private void checkReference() {
    Preconditions.checkState(reference.getReferenceCount() > 0);
  }

  @VisibleForTesting
  public int getReferenceCount() {
    return this.reference.getReferenceCount();
  }

  /**
   * 将存储卷标记为关闭，停止该卷上所有数据传输线程
   * @throws IOException 如果存储卷已经关闭则抛出异常
   */
  void setClosed() throws IOException {
    try {
      this.reference.setClosed();
      // 通知数据集停止该卷上所有数据传输线程
      dataset.stopAllDataxceiverThreads(this);
    } catch (ClosedChannelException e) {
      throw new IOException("The volume has already closed.", e);
    }
  }

  /**
   * 检查存储卷是否已经完全关闭（所有IO都已完成，引用计数为0）
   */
  boolean checkClosed() {
    if (this.reference.getReferenceCount() > 0) {
      FsDatasetImpl.LOG.debug("The reference count for {} is {}, wait to be 0.",
          this, reference.getReferenceCount());
      return false;
    }
    return true;
  }

  @VisibleForTesting
  public File getCurrentDir() {
    return currentDir;
  }

  /**
   * 获取指定块池的RBW目录路径
   */
  protected File getRbwDir(String bpid) throws IOException {
    return getBlockPoolSlice(bpid).getRbwDir();
  }

  /**
   * 获取指定块池的延迟持久化目录路径
   */
  protected File getLazyPersistDir(String bpid) throws IOException {
    return getBlockPoolSlice(bpid).getLazypersistDir();
  }

  /**
   * 获取指定块池的临时目录路径
   */
  protected File getTmpDir(String bpid) throws IOException {
    return getBlockPoolSlice(bpid).getTmpDir();
  }

  /**
   * 块文件删除后的空间更新处理
   */
  void onBlockFileDeletion(String bpid, long value) {
    // 减少DFS已用空间和块计数
    decDfsUsedAndNumBlocks(bpid, value, true);
    if (isTransientStorage()) {
      // 临时存储释放锁定的内存
      dataset.releaseLockedMemory(value, true);
    }
  }

  /**
   * 元数据文件删除后的空间更新处理
   */
  void onMetaFileDeletion(String bpid, long value) {
    decDfsUsedAndNumBlocks(bpid, value, false);
  }

  /**
   * 减少块池已用空间和块计数，块池切片本身线程安全，操作不需要加锁
   */
  private void decDfsUsedAndNumBlocks(String bpid, long value,
                                      boolean blockFileDeleted) {
    // BlockPoolSlice map is thread safe, and update the space used or
    // number of blocks are atomic operations, so it doesn't require to
    // hold the dataset lock.