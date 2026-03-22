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
import java.io.RandomAccessFile;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.HardLink;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.LayoutVersion;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.NodeType;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.StartupOption;
import org.apache.hadoop.hdfs.server.common.InconsistentFSStateException;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.Lists;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.collect.ComparisonChain;
import org.apache.hadoop.thirdparty.com.google.common.collect.Maps;

/** 
 * DataNode存储信息管理类，负责管理DataNode的所有存储目录和块池存储
 * <p>
 * 负责存储目录的格式化、版本升级、回滚、热插拔等存储生命周期管理，维护块池与存储的映射关系
 * @see Storage
 */
@InterfaceAudience.Private
public class DataStorage extends Storage {

  public final static String BLOCK_SUBDIR_PREFIX = "subdir";
  final static String STORAGE_DIR_DETACHED = "detach";
  public final static String STORAGE_DIR_RBW = "rbw";
  public final static String STORAGE_DIR_FINALIZED = "finalized";
  public final static String STORAGE_DIR_LAZY_PERSIST = "lazypersist";
  public final static String STORAGE_DIR_TMP = "tmp";

  /**
   * 当前启用回收站的块池ID集合。当回收站启用时，删除的块文件会先移动到回收站而不是立即删除，
   * 用于滚动升级等场景，方便出现回滚时恢复块文件。如果目录升级中存在previous目录，则即使启用回收站也不生效。
   * 底层基于ConcurrentHashMap实现线程安全的并发访问。
   */
  private Set<String> trashEnabledBpids;

  /**
   * 当前存储所属的DataNode UUID，与旧版本的StorageID兼容，对于从UUID版本之前升级的DataNode，该值等于原StorageID。
   * 由于兼容性原因，保留为字符串类型而非UUID类型。
   */
  private volatile String datanodeUuid = null;
  
  // 块池ID到块切片存储的映射表，线程安全
  private final Map<String, BlockPoolSliceStorage> bpStorageMap
      = Collections.synchronizedMap(new HashMap<String, BlockPoolSliceStorage>());


  /**
   * 默认构造函数，初始化DataNode存储类型和回收站集合
   */
  DataStorage() {
    super(NodeType.DATA_NODE);
    trashEnabledBpids = Collections.newSetFromMap(
        new ConcurrentHashMap<String, Boolean>());
  }
  
  /**
   * 根据块池ID获取对应的块池切片存储对象
   * @param bpid 块池ID
   * @return 对应的块池切片存储对象，不存在则返回null
   */
  public BlockPoolSliceStorage getBPStorage(String bpid) {
    return bpStorageMap.get(bpid);
  }
  
  /**
   * 根据已有存储信息构造DataStorage对象
   * @param storageInfo 存储信息对象
   */
  public DataStorage(StorageInfo storageInfo) {
    super(storageInfo);
  }

  /**
   * 获取当前存储所属的DataNode UUID
   * @return DataNode UUID字符串
   */
  public String getDatanodeUuid() {
    return datanodeUuid;
  }

  /**
   * 设置当前存储所属的DataNode UUID
   * @param newDatanodeUuid 新的DataNode UUID字符串
   */
  public void setDatanodeUuid(String newDatanodeUuid) {
    this.datanodeUuid = newDatanodeUuid;
  }

  private static boolean createStorageID(StorageDirectory sd, int lv,
      Configuration conf) {
    // 修复从早于ADD_DATANODE_AND_STORAGE_UUIDS版本升级集群时未正确生成新存储ID的问题
    final boolean haveValidStorageId = DataNodeLayoutVersion.supports(
        LayoutVersion.Feature.ADD_DATANODE_AND_STORAGE_UUIDS, lv)
        && DatanodeStorage.isValidStorageId(sd.getStorageUuid());
    return createStorageID(sd, !haveValidStorageId, conf);
  }

  /**
   * 为当前存储目录生成存储ID，必要时重新生成
   * @param sd 存储目录对象
   * @param regenerateStorageIds 是否强制重新生成存储ID
   * @param conf 配置对象
   * @return 如果生成了新的存储ID返回true，否则返回false
   */
  public static boolean createStorageID(
      StorageDirectory sd, boolean regenerateStorageIds, Configuration conf) {
    final String oldStorageID = sd.getStorageUuid();
    if (sd.getStorageLocation() != null &&
        sd.getStorageLocation().getStorageType() == StorageType.PROVIDED) {
      // PROVIDED类型存储仅支持一个存储ID，从配置读取
      // TODO 支持多个provided存储ID
      sd.setStorageUuid(conf.get(DFSConfigKeys.DFS_PROVIDER_STORAGEUUID,
          DFSConfigKeys.DFS_PROVIDER_STORAGEUUID_DEFAULT));
      return false;
    }
    if (oldStorageID == null || regenerateStorageIds) {
      sd.setStorageUuid(DatanodeStorage.generateUuid());
      LOG.info("Generated new storageID {} for directory {} {}", sd
              .getStorageUuid(), sd.getRoot(),
          (oldStorageID == null ? "" : (" to replace " + oldStorageID)));
      return true;
    }
    return false;
  }

  /**
   * 为指定块池启用回收站，块升级过程中存在previous目录时会覆盖该设置不启用回收站
   * @param bpid 目标块池ID
   */
  public void enableTrash(String bpid) {
    if (trashEnabledBpids.add(bpid)) {
      getBPStorage(bpid).stopTrashCleaner();
      LOG.info("Enabled trash for bpid {}",  bpid);
    }
  }

  /**
   * 清空指定块池的回收站并禁用回收站
   * @param bpid 目标块池ID
   */
  public void clearTrash(String bpid) {
    if (trashEnabledBpids.contains(bpid)) {
      getBPStorage(bpid).clearTrash();
      trashEnabledBpids.remove(bpid);
      LOG.info("Cleared trash for bpid {}", bpid);
    }
  }

  /**
   * 检查指定块池是否启用了回收站
   * @param bpid 目标块池ID
   * @return 如果启用返回true，否则返回false
   */
  public boolean trashEnabled(String bpid) {
    return trashEnabledBpids.contains(bpid);
  }

  /**
   * 为指定块池设置滚动升级标记
   * @param bpid 目标块池ID
   * @throws IOException 写入标记时IO异常
   */
  public void setRollingUpgradeMarker(String bpid) throws IOException {
    getBPStorage(bpid).setRollingUpgradeMarkers(getStorageDirs());
  }

  /**
   * 清除指定块池的滚动升级标记
   * @param bpid 目标块池ID
   * @throws IOException 清除标记时IO异常
   */
  public void clearRollingUpgradeMarker(String bpid) throws IOException {
    getBPStorage(bpid).clearRollingUpgradeMarkers(getStorageDirs());
  }

  /**
   * 获取副本对应的回收站目录，如果滚动升级进行中，删除的副本需要移动到回收站而不是直接删除。
   * 如果未启用回收站则返回null，后续可直接删除。
   * @param bpid 块池ID
   * @param info 副本信息
   * @return 目标回收站目录路径，不需要回收站则返回null
   */
  public String getTrashDirectoryForReplica(String bpid, ReplicaInfo info) {
    if (trashEnabledBpids.contains(bpid)) {
      return getBPStorage(bpid).getTrashDirectory(info);
    }
    return null;
  }

  /**
   * Volume构建器，用于热添加存储卷时暂存预加载的存储元数据，
   * 调用build()方法才会将预加载的元数据正式添加到DataStorage中，使存储卷生效
   */
  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  static public class VolumeBuilder {
    private DataStorage storage;
    /** 卷级存储目录 */
    private StorageDirectory sd;
    /** 块池ID到该块池对应存储目录数组的映射 */
    private Map<String, List<StorageDirectory>> bpStorageDirMap =
        Maps.newHashMap();

    @VisibleForTesting
    public VolumeBuilder(DataStorage storage, StorageDirectory sd) {
      this.storage = storage;
      this.sd = sd;
    }

    /**
     * 获取当前构建器对应的卷级存储目录
     * @return 存储目录对象
     */
    public final StorageDirectory getStorageDirectory() {
      return this.sd;
    }

    private void addBpStorageDirectories(String bpid,
        List<StorageDirectory> dirs) {
      bpStorageDirMap.put(bpid, dirs);
    }

    /**
     * 将预加载的卷元数据正式添加到DataStorage中，激活该存储卷
     */
    public void build() {
      assert this.sd != null;
      synchronized (storage) {
        for (Map.Entry<String, List<StorageDirectory>> e :
            bpStorageDirMap.entrySet()) {
          final String bpid = e.getKey();
          BlockPoolSliceStorage bpStorage = this.storage.bpStorageMap.get(bpid);
          assert bpStorage != null;
          for (StorageDirectory bpSd : e.getValue()) {
            bpStorage.addStorageDir(bpSd);
          }
        }
        storage.addStorageDir(sd);
      }
    }
  }

  /**
   * 加载单个存储目录，分析存储状态、处理格式升级，返回加载完成的存储目录
   * @param datanode DataNode对象引用
   * @param nsInfo 命名空间信息
   * @param location 存储位置对象
   * @param startOpt 启动选项
   * @param callables 升级任务回调列表，用于异步升级
   * @return 加载完成的存储目录对象
   * @throws IOException 加载过程IO异常或状态错误
   */
  private StorageDirectory loadStorageDirectory(DataNode datanode,
      NamespaceInfo nsInfo, StorageLocation location, StartupOption startOpt,
      List<Callable<StorageDirectory>> callables) throws IOException {
    StorageDirectory sd = new StorageDirectory(null, false, location);
    try {
      StorageState curState = sd.analyzeStorage(startOpt, this, true);
      // sd已加锁但未打开
      switch (curState) {
      case NORMAL:
        break;
      case NON_EXISTENT:
        LOG.info("Storage directory with location {} does not exist", location);
        throw new IOException("Storage directory with location " + location
            + " does not exist");
      case NOT_FORMATTED: // 需要格式化
        LOG.info("Storage directory with location {} is not formatted for "
            + "namespace {}. Formatting...", location, nsInfo.getNamespaceID());
        format(sd, nsInfo, datanode.getDatanodeUuid(), datanode.getConf());
        break;
      default:  // 恢复处理通用逻辑
        sd.doRecover(curState);
      }

      // 2. 执行存储状态转换
      // 每个存储目录独立处理，启动时部分目录可升级回滚，部分保持最新状态正常启动
      if (!doTransition(sd, nsInfo, startOpt, callables, datanode.getConf())) {

        // 3. 更新加载成功的存储信息
        setServiceLayoutVersion(getServiceLayoutVersion());
        writeProperties(sd);
      }

      return sd;
    } catch (IOException ioe) {
      sd.unlock();
      throw ioe;
    }
  }

  /**
   * 预准备一个存储目录，返回VolumeBuilder用于后续正式添加存储到DataStorage，
   * 如果添加失败可以直接丢弃构建器，不修改DataStorage状态，支持存储热插拔
   * @param datanode DataNode对象
   * @param location 存储目录对应的位置对象
   * @param nsInfos 所有命名空间信息列表
   * @return 包含预加载元数据的VolumeBuilder，后续调用build()即可激活存储
   * @throws IOException 准备过程IO异常
   * 注意：如果抛出IOException，DataStorage状态不会被修改
   */
  public VolumeBuilder prepareVolume(DataNode datanode,
      StorageLocation location, List<NamespaceInfo> nsInfos)
          throws IOException {
    if (containsStorageDir(location)) {
      final String errorMessage = "Storage directory is in use.";
      LOG.warn(errorMessage);
      throw new IOException(errorMessage);
    }

    StorageDirectory sd = loadStorageDirectory(
        datanode, nsInfos.get(0), location, StartupOption.HOTSWAP, null);
    VolumeBuilder builder =
        new VolumeBuilder(this, sd);
    for (NamespaceInfo nsInfo : nsInfos) {
      location.makeBlockPoolDir(nsInfo.getBlockPoolID(), datanode.getConf());

      final BlockPoolSliceStorage bpStorage = getBlockPoolSliceStorage(nsInfo);
      final List<StorageDirectory> dirs = bpStorage.loadBpStorageDirectories(
          nsInfo, location, StartupOption.HOTSWAP, null, datanode.getConf());
      builder.addBpStorageDirectories(nsInfo.getBlockPoolID(), dirs);
    }
    return builder;
  }

  /**
   * 获取并行加载存储目录的线程数
   * @param dataDirs 数据目录总数
   * @param conf 配置对象
   * @return 并行加载线程数，不小于1
   */
  static int getParallelVolumeLoadThreadsNum(int dataDirs, Configuration conf) {
    final String key
        = DFSConfigKeys.DFS_DATANODE_PARALLEL_VOLUME_LOAD_THREADS_NUM_KEY;
    final int n = conf.getInt(key, dataDirs);
    if (n < 1) {
      throw new HadoopIllegalArgumentException(key + " = " + n + " < 1");
    }
    final int min = Math.min(n, dataDirs);
    LOG.info("Using {} threads to upgrade data directories ({}={}, "
        + "dataDirs={})", min, key, n, dataDirs);
    return min;
  }

  /**
   * 升级任务包装类，存储数据目录和异步升级Future对象
   */
  static class UpgradeTask {
    private final StorageLocation dataDir;
    private final Future<StorageDirectory> future;

    UpgradeTask(StorageLocation dataDir, Future<StorageDirectory> future) {
      this.dataDir = dataDir;
      this.future = future;
    }
  }

  /**
   * 添加一组存储卷到DataStorage管理，空卷会自动格式化，非空卷会根据需要恢复状态
   * @param datanode DataNode对象引用
   * @param nsInfo 命名空间信息
   * @param dataDirs 数据存储位置集合
   * @param startOpt 启动选项
   * @return 加载成功的存储目录列表
   * @throws IOException 加载过程IO异常
   */
  @VisibleForTesting
  synchronized List<StorageDirectory> addStorageLocations(Data