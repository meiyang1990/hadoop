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

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_CRM_CHECKLOCKTIME_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_CRM_CHECKLOCKTIME_ENABLE;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_CRM_MAXLOCKTIME_MS;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_CRM_MAXLOCKTIME_MS_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_CRM_SLEEP_TIME_MS;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_CRM_SLEEP_TIME_MS_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_PATH_BASED_CACHE_BLOCK_MAP_ALLOCATION_PERCENT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_PATH_BASED_CACHE_BLOCK_MAP_ALLOCATION_PERCENT_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_LIST_CACHE_DIRECTIVES_NUM_RESPONSES;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_LIST_CACHE_DIRECTIVES_NUM_RESPONSES_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_LIST_CACHE_POOLS_NUM_RESPONSES;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_LIST_CACHE_POOLS_NUM_RESPONSES_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_PATH_BASED_CACHE_REFRESH_INTERVAL_MS;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_PATH_BASED_CACHE_REFRESH_INTERVAL_MS_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_CACHING_ENABLED_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_CACHING_ENABLED_DEFAULT;

import java.io.DataInput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.locks.ReentrantLock;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BatchedRemoteIterator.BatchedListEntries;
import org.apache.hadoop.fs.CacheFlag;
import org.apache.hadoop.fs.InvalidRequestException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.CacheDirective;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveEntry;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveInfo;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveInfo.Expiration;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveStats;
import org.apache.hadoop.hdfs.protocol.CachePoolEntry;
import org.apache.hadoop.hdfs.protocol.CachePoolInfo;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.CacheDirectiveInfoProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.CachePoolInfoProto;
import org.apache.hadoop.hdfs.protocolPB.PBHelperClient;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.blockmanagement.CacheReplicationMonitor;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor.CachedBlocksList;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor.CachedBlocksList.Type;
import org.apache.hadoop.hdfs.server.namenode.FSDirectory.DirOp;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.CacheManagerSection;
import org.apache.hadoop.hdfs.server.namenode.metrics.NameNodeMetrics;
import org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.Phase;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.StartupProgress;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.StartupProgress.Counter;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.Step;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.StepType;
import org.apache.hadoop.hdfs.util.ReadOnlyList;
import org.apache.hadoop.hdfs.util.RwLockMode;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.util.GSet;
import org.apache.hadoop.util.LightWeightGSet;
import org.apache.hadoop.util.Lists;
import org.apache.hadoop.util.Time;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.collect.HashMultimap;
import org.apache.hadoop.thirdparty.com.google.common.collect.Multimap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 文件路径: hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/CacheManager.java
 * 
 * HDFS DataNode缓存管理器，负责管理基于路径的缓存指令和缓存池，调度DataNode执行缓存/取消缓存操作
 * 维护缓存块与DataNode的映射关系，处理DataNode上报的缓存报告，协调缓存副本复制监控
 * 该类由FSNamesystem实例化，是NameNode端管理HDFS缓存的核心组件
 */
@InterfaceAudience.LimitedPrivate({"HDFS"})
public class CacheManager {
  public static final Logger LOG = LoggerFactory.getLogger(CacheManager.class);

  private static final float MIN_CACHED_BLOCKS_PERCENT = 0.001f;

  // TODO: add pending / underCached / schedule cached blocks stats.

  /**
   * 所属的FSNamesystem对象
   */
  private final FSNamesystem namesystem;

  /**
   * 关联的块管理器
   */
  private final BlockManager blockManager;

  /**
   * 按ID排序存储所有缓存指令，listCacheDirectives依赖该排序实现分页遍历
   */
  private final TreeMap<Long, CacheDirective> directivesById = new TreeMap<>();

  /**
   * 下一个新缓存指令的ID，ID单调递增永不复用
   */
  private long nextDirectiveId;

  /**
   * 按路径分组存储缓存指令，支持同一路径下多条缓存指令
   */
  private final Multimap<String, CacheDirective> directivesByPath =
      HashMultimap.create();

  /**
   * 按名称排序存储所有缓存池
   */
  private final TreeMap<String, CachePool> cachePools =
      new TreeMap<String, CachePool>();

  /**
   * 单次列表操作最多返回的缓存池数量
   */
  private final int maxListCachePoolsResponses;

  /**
   * 单次列表操作最多返回的缓存指令数量
   */
  private final int maxListCacheDirectivesNumResponses;

  /**
   * 缓存重新扫描间隔（毫秒）
   */
  private final long scanIntervalMs;

  /**
   * 存储所有已被缓存的块
   */
  private final GSet<CachedBlock, CachedBlock> cachedBlocks;

  /**
   * 保护CacheReplicationMonitor的锁
   */
  private final ReentrantLock crmLock = new ReentrantLock();

  private final SerializerCompat serializerCompat = new SerializerCompat();

  /**
   * 是否启用HDFS缓存功能
   * 禁用时不处理缓存报告、不存储缓存信息、不启动缓存复制监控线程，仅保留缓存指令元数据存储不丢失配置
   */
  private final boolean enabled;

  /**
   * 缓存复制监控线程，负责调度缓存和取消缓存任务
   */
  private CacheReplicationMonitor monitor;
  private boolean isCheckLockTimeEnable;
  private long maxLockTimeMs;
  private long sleepTimeMs;

  /**
   * 持久化状态容器，用于保存缓存管理器状态到FSImage
   */
  public static final class PersistState {
    public final CacheManagerSection section;
    public final List<CachePoolInfoProto> pools;
    public final List<CacheDirectiveInfoProto> directives;

    public PersistState(CacheManagerSection section,
        List<CachePoolInfoProto> pools, List<CacheDirectiveInfoProto> directives) {
      this.section = section;
      this.pools = pools;
      this.directives = directives;
    }
  }

  /**
   * 构造CacheManager，从配置初始化各项参数和存储结构
   * @param namesystem 所属的FSNamesystem
   * @param conf 配置对象
   * @param blockManager 关联的块管理器
   */
  CacheManager(FSNamesystem namesystem, Configuration conf,
      BlockManager blockManager) {
    this.namesystem = namesystem;
    this.blockManager = blockManager;
    this.nextDirectiveId = 1;
    this.enabled = conf.getBoolean(DFS_NAMENODE_CACHING_ENABLED_KEY,
        DFS_NAMENODE_CACHING_ENABLED_DEFAULT);
    this.maxListCachePoolsResponses = conf.getInt(
        DFS_NAMENODE_LIST_CACHE_POOLS_NUM_RESPONSES,
        DFS_NAMENODE_LIST_CACHE_POOLS_NUM_RESPONSES_DEFAULT);
    this.maxListCacheDirectivesNumResponses = conf.getInt(
        DFS_NAMENODE_LIST_CACHE_DIRECTIVES_NUM_RESPONSES,
        DFS_NAMENODE_LIST_CACHE_DIRECTIVES_NUM_RESPONSES_DEFAULT);
    scanIntervalMs = conf.getLong(
        DFS_NAMENODE_PATH_BASED_CACHE_REFRESH_INTERVAL_MS,
        DFS_NAMENODE_PATH_BASED_CACHE_REFRESH_INTERVAL_MS_DEFAULT);
    float cachedBlocksPercent = conf.getFloat(
          DFS_NAMENODE_PATH_BASED_CACHE_BLOCK_MAP_ALLOCATION_PERCENT,
          DFS_NAMENODE_PATH_BASED_CACHE_BLOCK_MAP_ALLOCATION_PERCENT_DEFAULT);
    // 检查并限制缓存块分配比例不低于最小值
    if (cachedBlocksPercent < MIN_CACHED_BLOCKS_PERCENT) {
      LOG.info("Using minimum value {} for {}", MIN_CACHED_BLOCKS_PERCENT,
        DFS_NAMENODE_PATH_BASED_CACHE_BLOCK_MAP_ALLOCATION_PERCENT);
      cachedBlocksPercent = MIN_CACHED_BLOCKS_PERCENT;
    }
    // 根据是否启用缓存初始化缓存块存储大小
    this.cachedBlocks = enabled ? new LightWeightGSet<CachedBlock, CachedBlock>(
          LightWeightGSet.computeCapacity(cachedBlocksPercent,
              "cachedBlocks")) : new LightWeightGSet<>(0);
    this.isCheckLockTimeEnable = conf.getBoolean(
        DFS_NAMENODE_CRM_CHECKLOCKTIME_ENABLE,
        DFS_NAMENODE_CRM_CHECKLOCKTIME_DEFAULT);
    this.maxLockTimeMs = conf.getLong(DFS_NAMENODE_CRM_MAXLOCKTIME_MS,
        DFS_NAMENODE_CRM_MAXLOCKTIME_MS_DEFAULT);
    this.sleepTimeMs = conf.getLong(DFS_NAMENODE_CRM_SLEEP_TIME_MS,
        DFS_NAMENODE_CRM_SLEEP_TIME_MS_DEFAULT);
  }

  /**
   * 获取缓存功能是否启用
   * @return true表示启用，false表示禁用
   */
  public boolean isEnabled() {
    return enabled;
  }

  /**
   * 获取是否启用缓存复制监控锁超时检查
   * @return true表示启用，false表示禁用
   */
  public boolean isCheckLockTimeEnable() {
    return isCheckLockTimeEnable;
  }

  /**
   * 获取最大锁持有时间（毫秒）
   * @return 最大锁持有时间
   */
  public long getMaxLockTimeMs() {
    return this.maxLockTimeMs;
  }

  /**
   * 获取锁超时检查间隔睡眠时间（毫秒）
   * @return 睡眠时间
   */
  public long getSleepTimeMs() {
    return this.sleepTimeMs;
  }

  /**
   * 清空所有缓存指令和缓存池，重置状态，在二级NameNode检查点重置FSNamesystem时调用
   */
  void clear() {
    directivesById.clear();
    directivesByPath.clear();
    cachePools.clear();
    nextDirectiveId = 1;
  }

  /**
   * 启动缓存复制监控线程，仅在缓存功能启用时启动
   */
  public void startMonitorThread() {
    if (!isEnabled()) {
      LOG.info("Not starting CacheReplicationMonitor as name-node caching" +
              " is disabled.");
      return;
    }

    crmLock.lock();
    try {
      // 避免重复启动监控线程
      if (this.monitor == null) {
        this.monitor = new CacheReplicationMonitor(namesystem, this,
            scanIntervalMs, crmLock);
        this.monitor.start();
      }
    } finally {
      crmLock.unlock();
    }
  }

  /**
   * 停止缓存复制监控线程，释放资源
   */
  public void stopMonitorThread() {
    if (!isEnabled()) {
      return;
    }

    crmLock.lock();
    try {
      if (this.monitor != null) {
        CacheReplicationMonitor prevMonitor = this.monitor;
        this.monitor = null;
        IOUtils.closeStream(prevMonitor);
      }
    } finally {
      crmLock.unlock();
    }
  }

  /**
   * 重置所有缓存指令的统计信息
   */
  public void clearDirectiveStats() {
    assert namesystem.hasWriteLock(RwLockMode.FS);
    for (CacheDirective directive : directivesById.values()) {
      directive.resetStatistics();
    }
  }

  /**
   * 获取所有缓存池的不可修改视图
   * @return 缓存池集合
   */
  public Collection<CachePool> getCachePools() {
    assert namesystem.hasReadLock(RwLockMode.FS);
    return Collections.unmodifiableCollection(cachePools.values());
  }

  /**
   * 获取所有缓存指令的不可修改视图
   * @return 缓存指令集合
   */
  public Collection<CacheDirective> getCacheDirectives() {
    assert namesystem.hasReadLock(RwLockMode.FS);
    return Collections.unmodifiableCollection(directivesById.values());
  }
  
  @VisibleForTesting
  public GSet<CachedBlock, CachedBlock> getCachedBlocks() {
    assert namesystem.hasReadLock(RwLockMode.BM);
    return cachedBlocks;
  }

  /**
   * 获取下一个可用的缓存指令ID
   * @return 下一个ID
   * @throws IOException 当ID耗尽时抛出异常
   */
  private long getNextDirectiveId() throws IOException {
    assert namesystem.hasWriteLock(RwLockMode.FS);
    if (nextDirectiveId >= Long.MAX_VALUE - 1) {
      throw new IOException("No more available IDs.");
    }
    return nextDirectiveId++;
  }

  // Helper getter / validation methods

  /**
   * 检查权限检查器对缓存池是否有写权限
   * @param pc 权限检查器
   * @param pool 目标缓存池
   * @throws AccessControlException 无权限时抛出异常
   */
  private static void checkWritePermission(FSPermissionChecker pc,
      CachePool pool) throws AccessControlException {
    if ((pc != null)) {
      pc.checkPermission(pool, FsAction.WRITE);
    }
  }

  /**
   * 验证缓存指令的缓存池名称是否合法
   * @param directive 缓存指令
   * @return 验证通过的池名称
   * @throws InvalidRequestException 名称不合法时抛出异常
   */
  private static String validatePoolName(CacheDirective directive)
      throws InvalidRequestException {
    String pool = directive.getPool();
    if (pool == null) {
      throw new InvalidRequestException("No pool specified.");
    }
    if (pool.isEmpty()) {
      throw new InvalidRequestException("Invalid empty pool name.");
    }
    return pool;
  }

  /**
   * 验证缓存指令的路径是否合法
   *