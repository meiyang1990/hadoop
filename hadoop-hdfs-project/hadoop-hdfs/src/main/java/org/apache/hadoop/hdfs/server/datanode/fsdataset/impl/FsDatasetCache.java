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

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_CACHE_REVOCATION_TIMEOUT_MS;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_CACHE_REVOCATION_TIMEOUT_MS_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_CACHE_REVOCATION_POLLING_MS;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_CACHE_REVOCATION_POLLING_MS_DEFAULT;

import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Preconditions;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

import org.apache.commons.lang3.time.DurationFormatUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.ChecksumException;
import org.apache.hadoop.hdfs.ExtendedBlockId;
import org.apache.hadoop.hdfs.protocol.BlockListAsLongs;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.server.datanode.DNConf;
import org.apache.hadoop.hdfs.server.datanode.DatanodeUtil;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * HDFS数据节点FsDatasetImpl的块缓存管理器，通过mmap(2)和mlock(2)系统调用将数据块锁定到内存中实现缓存。
 * 进入缓存的块会进行校验和验证，支持DRAM和持久化内存两种缓存介质，处理缓存添加、回收、异步撤销等操作。
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class FsDatasetCache {
  /**
   * 存储可映射块及其对应缓存状态的内部值类
   */
  private static final class Value {
    final State state;
    final MappableBlock mappableBlock;

    Value(MappableBlock mappableBlock, State state) {
      this.mappableBlock = mappableBlock;
      this.state = state;
    }
  }

  /**
   * 块缓存状态枚举
   */
  private enum State {
    /**
     * 块正在缓存中，还未完成
     */
    CACHING,

    /**
     * 块缓存过程被取消，只有后台任务可以移除该状态的块
     */
    CACHING_CANCELLED,

    /**
     * 块已完成缓存，可以使用
     */
    CACHED,

    /**
     * 块正在从缓存中移除
     */
    UNCACHING;

    /**
     * 判断当前状态是否需要向NameNode和客户端报告为已缓存
     * @return true表示需要报告为已缓存
     */
    public boolean shouldAdvertise() {
      return (this == CACHED);
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(FsDatasetCache
      .class);

  /**
   * 存储所有块的缓存信息，键为扩展块ID，值为块对象和状态
   */
  private final HashMap<ExtendedBlockId, Value> mappableBlockMap =
      new HashMap<ExtendedBlockId, Value>();

  /**
   * 当前已缓存块的总数量
   */
  private final LongAdder numBlocksCached = new LongAdder();

  /**
   * 所属的数据块数据集实现对象
   */
  private final FsDatasetImpl dataset;

  /**
   * 执行立即取消缓存操作的线程池
   */
  private final ThreadPoolExecutor uncachingExecutor;

  /**
   * 执行延迟取消缓存操作的定时线程池
   */
  private final ScheduledThreadPoolExecutor deferredUncachingExecutor;

  /**
   * 缓存回收超时时间（毫秒），超过该时间强制回收缓存
   */
  private final long revocationMs;

  /**
   * 延迟回收轮询间隔（毫秒），每隔该时间检查是否可以回收缓存
   */
  private final long revocationPollingMs;

  /**
   * 可映射块加载器，负责加载块到缓存，支持DRAM和持久化内存两种实现
   */
  private final MappableBlockLoader cacheLoader;

  /**
   * 内存缓存统计信息，记录缓存使用量等指标
   */
  private final CacheStats memCacheStats;

  /**
   * 缓存命令执行失败的块数量
   */
  final LongAdder numBlocksFailedToCache = new LongAdder();
  /**
   * 取消缓存命令执行失败的块数量
   */
  final LongAdder numBlocksFailedToUncache = new LongAdder();

  /**
   * 构造FsDatasetCache缓存管理器，初始化线程池和配置参数，创建块加载器
   * @param dataset 所属的数据块数据集实现
   * @throws IOException 初始化失败时抛出异常
   */
  public FsDatasetCache(FsDatasetImpl dataset) throws IOException {
    this.dataset = dataset;
    ThreadFactory workerFactory = new ThreadFactoryBuilder()
        .setDaemon(true)
        .setNameFormat("FsDatasetCache-%d-" + dataset.toString())
        .build();
    this.uncachingExecutor = new ThreadPoolExecutor(
            0, 1,
            60, TimeUnit.SECONDS,
            new LinkedBlockingQueue<Runnable>(),
            workerFactory);
    this.uncachingExecutor.allowCoreThreadTimeOut(true);
    this.deferredUncachingExecutor = new ScheduledThreadPoolExecutor(
            1, workerFactory);
    this.revocationMs = dataset.datanode.getConf().getLong(
        DFS_DATANODE_CACHE_REVOCATION_TIMEOUT_MS,
        DFS_DATANODE_CACHE_REVOCATION_TIMEOUT_MS_DEFAULT);
    long confRevocationPollingMs = dataset.datanode.getConf().getLong(
        DFS_DATANODE_CACHE_REVOCATION_POLLING_MS,
        DFS_DATANODE_CACHE_REVOCATION_POLLING_MS_DEFAULT);
    long minRevocationPollingMs = revocationMs / 2;
    if (minRevocationPollingMs < confRevocationPollingMs) {
      throw new RuntimeException("configured value " +
              confRevocationPollingMs + "for " +
              DFS_DATANODE_CACHE_REVOCATION_POLLING_MS +
              " is too high.  It must not be more than half of the " +
              "value of " +  DFS_DATANODE_CACHE_REVOCATION_TIMEOUT_MS +
              ".  Reconfigure this to " + minRevocationPollingMs);
    }
    this.revocationPollingMs = confRevocationPollingMs;

    this.cacheLoader = MappableBlockLoaderFactory.createCacheLoader(
        this.getDnConf());
    // Both lazy writer and read cache are sharing this statistics.
    this.memCacheStats = cacheLoader.initialize(this.getDnConf());
  }

  /**
   * 初始化指定块池的缓存，针对持久化内存缓存创建目录并恢复已有缓存状态
   * @param bpid 块池ID
   * @throws IOException 初始化失败时抛出IO异常
   */
  public void initCache(String bpid) throws IOException {
    if (cacheLoader.isTransientCache()) {
      return;
    }
    PmemVolumeManager.getInstance().createBlockPoolDir(bpid);
    if (getDnConf().getPmemCacheRecoveryEnabled()) {
      final Map<ExtendedBlockId, MappableBlock> keyToMappableBlock =
          PmemVolumeManager.getInstance().recoverCache(bpid, cacheLoader);
      Set<Map.Entry<ExtendedBlockId, MappableBlock>> entrySet
          = keyToMappableBlock.entrySet();
      for (Map.Entry<ExtendedBlockId, MappableBlock> entry : entrySet) {
        mappableBlockMap.put(entry.getKey(),
            new Value(keyToMappableBlock.get(entry.getKey()), State.CACHED));
        numBlocksCached.increment();
        dataset.datanode.getMetrics().incrBlocksCached(1);
      }
    }
  }

  DNConf getDnConf() {
    return this.dataset.datanode.getDnConf();
  }

  /**
   * 获取缓存在持久化内存中的副本的缓存路径
   * @param bpid 块池ID
   * @param blockId 块ID
   * @return 缓存路径，如果是DRAM缓存或块未缓存返回null
   * @throws IOException 获取路径失败时抛出IO异常
   */
  String getReplicaCachePath(String bpid, long blockId) throws IOException {
    if (cacheLoader.isTransientCache() ||
        !isCached(bpid, blockId)) {
      return null;
    }
    ExtendedBlockId key = new ExtendedBlockId(blockId, bpid);
    return PmemVolumeManager.getInstance().getCachePath(key);
  }

  /**
   * 获取持久化内存中缓存块的起始内存地址，用于直接内存访问
   * @param bpid 块池ID
   * @param blockId 块ID
   * @return 内存地址，如果是DRAM缓存、块未缓存或不是本地加载器返回-1
   */
  long getCacheAddress(String bpid, long blockId) {
    if (cacheLoader.isTransientCache() ||
        !isCached(bpid, blockId)) {
      return -1;
    }
    if (!(cacheLoader.isNativeLoader())) {
      return -1;
    }
    ExtendedBlockId key = new ExtendedBlockId(blockId, bpid);
    MappableBlock mappableBlock = mappableBlockMap.get(key).mappableBlock;
    return mappableBlock.getAddress();
  }

  /**
   * 获取指定块池下所有已缓存块的ID列表，用于生成缓存报告发送给NameNode
   * @param bpid 块池ID
   * @return 已缓存块ID列表
   */
  synchronized List<Long> getCachedBlocks(String bpid) {
    List<Long> blocks = new ArrayList<Long>();
    for (Iterator<Entry<ExtendedBlockId, Value>> iter =
        mappableBlockMap.entrySet().iterator(); iter.hasNext(); ) {
      Entry<ExtendedBlockId, Value> entry = iter.next();
      if (entry.getKey().getBlockPoolId().equals(bpid)) {
        if (entry.getValue().state.shouldAdvertise()) {
          blocks.add(entry.getKey().getBlockId());
        }
      }
    }
    return blocks;
  }

  /**
   * 发起一个块缓存请求，将任务提交给后台线程异步执行
   * @param blockId 块ID
   * @param bpid 块池ID
   * @param blockFileName 块数据文件路径
   * @param length 块长度
   * @param genstamp 块生成时间戳
   * @param volumeExecutor 卷执行器，用于执行缓存任务
   */
  synchronized void cacheBlock(long blockId, String bpid,
      String blockFileName, long length, long genstamp,
      Executor volumeExecutor) {
    ExtendedBlockId key = new ExtendedBlockId(blockId, bpid);
    Value prevValue = mappableBlockMap.get(key);
    if (prevValue != null) {
      LOG.debug("Block with id {}, pool {} already exists in the "
              + "FsDatasetCache with state {}", blockId, bpid, prevValue.state
      );
      numBlocksFailedToCache.increment();
      return;
    }
    mappableBlockMap.put(key, new Value(null, State.CACHING));
    volumeExecutor.execute(
        new CachingTask(key, blockFileName, length, genstamp));
    LOG.debug("Initiating caching for Block with id {}, pool {}", blockId,
        bpid);
  }

  /**
   * 发起一个块取消缓存请求，根据块当前状态处理不同的取消逻辑
   * @param bpid 块池ID
   * @param blockId 块ID
   */
  synchronized void uncacheBlock(String bpid, long blockId) {
    ExtendedBlockId key = new ExtendedBlockId(blockId, bpid);
    Value prevValue = mappableBlockMap.get(key);
    boolean deferred = false;

    if (cacheLoader.isTransientCache() && !dataset.datanode.
        getShortCircuitRegistry().processBlockMunlockRequest(key)) {
      deferred = true;
    }
    if (prevValue == null) {
      LOG.debug("Block with id {}, pool {} does not need to be uncached, "
          + "because it is not currently in the mappableBlockMap.", blockId,
          bpid);
      numBlocksFailedToUncache.increment();
      return;
    }
    switch (prevValue.state) {
    case CACHING:
      LOG.debug("Cancelling caching for block with id {}, pool {}.", blockId,
          bpid);
      mappableBlockMap.put(key,
          new Value(prevValue.mappableBlock, State.CACHING_CANCELLED));
      break;
    case CACHED:
      mappableBlockMap.put(key,
          new Value(prevValue.mappableBlock, State.UNCACHING));
      if (deferred) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("{} is anchored, and can't be uncached now.  Scheduling it " +
                  "for uncaching in {} ",
              key, DurationFormatUtils.formatDurationHMS(revocationPollingMs));
        }
        deferredUncachingExecutor.schedule(
            new UncachingTask(key, revocationMs),
            revocationPollingMs, TimeUnit.MILLISECONDS);
      } else {
        LOG.debug("{} has been scheduled for immediate uncaching.", key);
        uncachingExecutor.execute(new UncachingTask(key, 0));
      }
      break;
    default:
      LOG.debug("Block with id {}, pool {} does not need to be uncached, "
          + "because it is in state {}.", blockId, bpid, prevValue.state);
      numBlocksFailedToUncache.increment();
      break;
    }
  }

  /**
   * 尝试预留指定大小的缓存空间，会自动向上对齐到页大小
   * @param count 需要预留的字节数
   * @return 预留成功返回新的已使用字节数，失败返回-1
   */
  long reserve(long count) {
    return memCacheStats.reserve(count);
  }

  /**
   * 释放指定大小的缓存空间，会自动向上对齐到页大小
   * @param count 需要释放的字节数
   * @return 释放后新的已使用字节数
   */
  long release(long count) {
    return memCacheStats.release(count);
  }

  /**
   * 释放指定大小的缓存空间，自动向下对齐到页大小
   * @param count 需要释放的字节数
   * @return 释放后新的已使用字节数
   */
  long releaseRoundDown(long count) {
    return memCacheStats.releaseRoundDown(count);
  }

  /**
   * 获取操作系统页大小
   * @return 操作系统页大小（字节）
   */
  long getOsPageSize() {
    return memCacheStats.getPageSize();
  }

  /**
   * 将指定字节数向上对齐到操作系统页大小
   * @param count 需要对齐的字节数
   * @return 对齐后的字节数
   */
  long roundUpPageSize(long count) {
    return memCacheStats.roundUpPageSize(count);
  }

  /**
   * 后台缓存任务，负责执行mmap、mlock、校验和验证，将块加载到缓存中
   */
  private class CachingTask implements Runnable {
    private final ExtendedBlockId key; 
    private final String blockFileName;
    private final long length;
    private final long genstamp;

    CachingTask(ExtendedBlockId key, String blockFileName, long length, long genstamp) {
      this.key = key;
      this.blockFileName = blockFileName;
      this.length = length;
      this.genstamp = genstamp;
    }

    @Override
    public void run() {
      boolean success = false;
      FileInputStream blockIn = null, metaIn = null;
      MappableBlock