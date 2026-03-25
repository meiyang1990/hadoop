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

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;

import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;
import javax.management.StandardMBean;

import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.HardLink;
import org.apache.hadoop.classification.VisibleForTesting;

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.hdfs.ExtendedBlockId;
import org.apache.hadoop.hdfs.server.common.AutoCloseDataSetLock;
import org.apache.hadoop.hdfs.server.common.DataNodeLockManager;
import org.apache.hadoop.hdfs.server.common.DataNodeLockManager.LockLevel;
import org.apache.hadoop.hdfs.server.datanode.DataNodeFaultInjector;
import org.apache.hadoop.hdfs.server.datanode.DataNodeLayoutSubLockStrategy;
import org.apache.hadoop.hdfs.server.datanode.DataSetLockManager;
import org.apache.hadoop.hdfs.server.datanode.DataSetSubLockStrategy;
import org.apache.hadoop.hdfs.server.datanode.FileIoProvider;
import org.apache.hadoop.hdfs.server.datanode.FinalizedReplica;
import org.apache.hadoop.hdfs.server.datanode.LocalReplica;
import org.apache.hadoop.hdfs.server.datanode.metrics.DataNodeMetrics;
import org.apache.hadoop.util.AutoCloseableLock;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockListAsLongs;
import org.apache.hadoop.hdfs.protocol.BlockLocalPathInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.RecoveryInProgressException;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.ReplicaState;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.datanode.BlockMetadataHeader;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.metrics.DataNodeMetricHelper;
import org.apache.hadoop.hdfs.server.datanode.DataStorage;
import org.apache.hadoop.hdfs.server.datanode.DatanodeUtil;
import org.apache.hadoop.hdfs.server.datanode.Replica;
import org.apache.hadoop.hdfs.server.datanode.ReplicaAlreadyExistsException;
import org.apache.hadoop.hdfs.server.datanode.ReplicaBuilder;
import org.apache.hadoop.hdfs.server.datanode.ReplicaHandler;
import org.apache.hadoop.hdfs.server.datanode.ReplicaInPipeline;
import org.apache.hadoop.hdfs.server.datanode.ReplicaInfo;
import org.apache.hadoop.hdfs.server.datanode.ReplicaNotFoundException;
import org.apache.hadoop.hdfs.server.datanode.StorageLocation;
import org.apache.hadoop.hdfs.server.datanode.UnexpectedReplicaStateException;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeReference;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi.ScanInfo;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.LengthInputStream;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.ReplicaInputStreams;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.ReplicaOutputStreams;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.RoundRobinVolumeChoosingPolicy;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.VolumeChoosingPolicy;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.RamDiskReplicaTracker.RamDiskReplica;
import org.apache.hadoop.hdfs.server.datanode.metrics.FSDatasetMBean;
import org.apache.hadoop.hdfs.server.protocol.BlockRecoveryCommand.RecoveringBlock;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.server.protocol.ReplicaRecoveryInfo;
import org.apache.hadoop.hdfs.server.protocol.StorageReport;
import org.apache.hadoop.hdfs.server.protocol.VolumeFailureSummary;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.MultipleIOException;
import org.apache.hadoop.io.nativeio.NativeIO;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.util.MBeans;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.DataChecksum;
import org.apache.hadoop.util.DiskChecker.DiskErrorException;
import org.apache.hadoop.util.DiskChecker.DiskOutOfSpaceException;
import org.apache.hadoop.util.Lists;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.util.Timer;

import org.apache.hadoop.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * HDFS DataNode 文件数据集实现类，负责管理本节点所有数据块的存储与访问。
 * 核心职责包括：块副本的增删改查、存储卷管理、块恢复、懒持久化、缓存管理等，
 * 是DataNode处理块读写请求的核心底层模块。
 */
@InterfaceAudience.Private
class FsDatasetImpl implements FsDatasetSpi<FsVolumeImpl> {
  static final Logger LOG = LoggerFactory.getLogger(FsDatasetImpl.class);
  private final static boolean isNativeIOAvailable;
  private Timer timer;
  static {
    // 检查NativeIO是否可用，Windows平台无NativeIO会打印警告
    isNativeIOAvailable = NativeIO.isAvailable();
    if (Path.WINDOWS && !isNativeIOAvailable) {
      LOG.warn("Data node cannot fully support concurrent reading"
          + " and writing without native code extensions on Windows.");
    }
  }

  @Override // FsDatasetSpi
  /**
   * 获取所有存储卷的引用列表
   * @return 所有有效存储卷的引用集合
   */
  public FsVolumeReferences getFsVolumeReferences() {
    return new FsVolumeReferences(volumes.getVolumes());
  }

  @Override
  /**
   * 根据存储UUID获取DatanodeStorage对象
   * @param storageUuid 存储唯一标识
   * @return 对应存储信息对象
   */
  public DatanodeStorage getStorage(final String storageUuid) {
    return storageMap.get(storageUuid);
  }

  @Override // FsDatasetSpi
  /**
   * 生成指定块池的存储报告，上报给NameNode
   * @param bpid 块池ID
   * @return 每个存储卷的容量、使用量等信息数组
   * @throws IOException IO异常
   */
  public StorageReport[] getStorageReports(String bpid)
      throws IOException {
    List<StorageReport> reports;
    // Volumes are the references from a copy-on-write snapshot, so the
    // access on the volume metrics doesn't require an additional lock.
    List<FsVolumeImpl> curVolumes = volumes.getVolumes();
    reports = new ArrayList<>(curVolumes.size());
    for (FsVolumeImpl volume : curVolumes) {
      try (FsVolumeReference ref = volume.obtainReference()) {
        StorageReport sr = new StorageReport(volume.toDatanodeStorage(),
            false,
            volume.getCapacity(),
            volume.getDfsUsed(),
            volume.getAvailable(),
            volume.getBlockPoolUsed(bpid),
            volume.getNonDfsUsed(),
            volume.getMount()
        );
        reports.add(sr);
      } catch (ClosedChannelException e) {
        continue;
      }
    }

    return reports.toArray(new StorageReport[reports.size()]);
  }

  @Override
  /**
   * 获取指定块所在的存储卷
   * @param b 扩展块对象
   * @return 存储卷对象，找不到返回null
   */
  public FsVolumeImpl getVolume(final ExtendedBlock b) {
    try (AutoCloseableLock lock = lockManager.readLock(LockLevel.BLOCK_POOl,
        b.getBlockPoolId())) {
      final ReplicaInfo r =
          volumeMap.get(b.getBlockPoolId(), b.getLocalBlock());
      return r != null ? (FsVolumeImpl) r.getVolume() : null;
    }
  }

  @Override // FsDatasetSpi
  /**
   * 根据块池ID和块ID获取存储的块信息（包含生成时间戳）
   * @param bpid 块池ID
   * @param blkid 块ID
   * @return 块对象，找不到返回null
   * @throws IOException IO异常
   */
  public Block getStoredBlock(String bpid, long blkid)
      throws IOException {
    try (AutoCloseableLock lock = lockManager.readLock(LockLevel.DIR,
        bpid, getReplicaInfo(bpid, blkid).getStorageUuid(),
        datasetSubLockStrategy.blockIdToSubLock(blkid))) {
      ReplicaInfo r = volumeMap.get(bpid, blkid);
      if (r == null) {
        return null;
      }
      return new Block(blkid, r.getBytesOnDisk(), r.getGenerationStamp());
    }
  }

  @Override
  /**
   * 深度复制指定块池的所有副本对象
   * @param bpid 块池ID
   * @return 所有副本的深拷贝集合
   * @throws IOException IO异常
   */
  public Set<? extends Replica> deepCopyReplica(String bpid)
      throws IOException {
    Set<ReplicaInfo> replicas = new HashSet<>();
    volumeMap.replicas(bpid, (iterator) -> {
      while (iterator.hasNext()) {
        ReplicaInfo b = iterator.next();
        replicas.add(b);
      }
    });
    return replicas;
  }

  /**
   * 测试用方法：获取内存中副本信息的克隆
   * @param bpid 块池ID
   * @param blockId 块ID
   * @return 克隆后的副本对象
   */
  ReplicaInfo fetchReplicaInfo(String bpid, long blockId) {
    ReplicaInfo r = volumeMap.get(bpid, blockId);
    if (r == null) {
      return null;
    }
    switch(r.getState()) {
    case FINALIZED:
    case RBW:
    case RWR:
    case RUR:
    case TEMPORARY:
      return new ReplicaBuilder(r.getState()).from(r).build();
    }
    return null;
  }
  
  @Override // FsDatasetSpi
  /**
   * 获取块元数据文件的输入流
   * @param b 目标块
   * @return 元数据输入流，不存在返回null
   * @throws IOException IO异常
   */
  public LengthInputStream getMetaDataInputStream(ExtendedBlock b)
      throws IOException {
    ReplicaInfo info = getBlockReplica(b);
    if (info == null || !info.metadataExists()) {
      return null;
    }
    DataNodeFaultInjector.get().delayGetMetaDataInputStream();
    return info.getMetadataInputStream(0);
  }

  final DataNode datanode;
  private final DataNodeMetrics dataNodeMetrics;
  final DataStorage dataStorage;
  private final FsVolumeList volumes;
  final Map<String, DatanodeStorage> storageMap;
  final FsDatasetAsyncDiskService asyncDiskService;
  final Daemon lazyWriter;
  final FsDatasetCache cacheManager;
  private final Configuration conf;
  private final int volFailuresTolerated;
  private final int volsConfigured;
  private volatile boolean fsRunning;

  final ReplicaMap volumeMap;
  final Map<String, Set<Long>> deletingBlock;
  final RamDiskReplicaTracker ramDiskReplicaTracker;
  final RamDiskAsyncLazyPersistService asyncLazyPersistService;

  private static final int MAX_BLOCK_EVICTIONS_PER_ITERATION = 3;

  private final int smallBufferSize;

  final LocalFileSystem localFS;

  private boolean blockPinningEnabled;
  private final int maxDataLength;

  private final DataSetLockManager lockManager;
  private static String blockPoolId = "";

  // Make limited notify times from DirectoryScanner to NameNode.
  private long maxDirScannerNotifyCount;
  private long curDirScannerNotifyCount;
  private long lastDirScannerNotifyTime;
  private volatile long lastDirScannerFinishTime;

  private final DataSetSubLockStrategy datasetSubLockStrategy;

  /**
   * 构造FsDatasetImpl，初始化所有存储卷和核心组件
   * @param datanode DataNode实例
   * @param storage DataNode存储对象
   * @param conf 配置对象
   * @throws IOException 初始化失败抛出IO异常
   */
  FsDatasetImpl(DataNode datanode, DataStorage storage, Configuration conf
      ) throws IOException {
    this.fsRunning = true;
    this.datanode = datanode;
    this.dataNodeMetrics = datanode.getMetrics();
    this.dataStorage = storage;
    this.conf = conf;
    this.smallBufferSize = DFSUtilClient.getSmallBufferSize(conf);
    this.lockManager = datanode.getDataSetLockManager();

    // The number of volumes required for operation is the total number
    // of volumes minus the number of failed volumes we can tolerate.
    volFailuresTolerated = datanode.getDnConf().getVolFailuresTolerated();

    Collection<StorageLocation> dataLocations = DataNode.getStorageLocations(conf);
    List<VolumeFailureInfo> volumeFailureInfos = getInitialVolumeFailureInfos(
        dataLocations, storage);

    volsConfigured = datanode.getDnConf().getVolsConfigured();
    int volsFailed = volumeFailureInfos.size();

    if (volFailuresTolerated < DataNode.MAX_VOLUME_FAILURE_TOLERATED_LIMIT
        || volFailuresTolerated >= volsConfigured) {
      throw new HadoopIllegalArgumentException("Invalid value configured for "
          + "dfs.datanode.failed.volumes.tolerated - " + volFailuresTolerated
          + ". Value configured is either less than maxVolumeFailureLimit or greater than "
          + "to the number of configured volumes (" + volsConfigured + ").");
    }
    if (volFailuresTolerated == DataNode.MAX_VOLUME_FAILURE_TOLERATED_LIMIT) {
      if (volsConfigured == volsFailed) {
        throw new DiskErrorException(
            "Too many failed volumes - " + "current valid volumes: "
                + storage.getNumStorageDirs() + ", volumes configured: "
                + volsConfigured + ", volumes failed: " + volsFailed
                + ", volume failures tolerated: " + volFailuresTolerated);
      }
    } else {
      if (volsFailed > volFailuresTolerated) {
        throw new DiskErrorException(
            "Too many failed volumes - " + "current valid volumes: "
                + storage.getNumStorageDirs() + ", volumes configured: "
                + volsConfigured + ", volumes failed: " + volsFailed
                + ", volume failures tolerated: " + volFailuresTolerated);
      }
    }

    storageMap = new ConcurrentHashMap<String, DatanodeStorage>();
    volumeMap = new ReplicaMap(lockManager);
    ramDiskReplicaTracker = RamDiskReplicaTracker.getInstance(conf, this);

    @SuppressWarnings("unchecked")
    // 通过反射创建存储卷选择策略实例
    final VolumeChoosingPolicy<FsVolumeImpl> blockChooserImpl =
        ReflectionUtils.newInstance(conf.getClass(
            DFSConfigKeys.DFS_DATANODE_FSDATASET_VOLUME_CHOOSING_POLICY_KEY,
            RoundRobinVolumeChoosingPolicy.class,
            VolumeChoosingPolicy.class), conf);
    volumes = new F