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

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_PROVIDED_ALIASMAP_LOAD_RETRIES;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathHandle;
import org.apache.hadoop.fs.RawPathHandle;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockListAsLongs;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.server.common.FileRegion;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.ReplicaState;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import org.apache.hadoop.hdfs.server.common.blockaliasmap.BlockAliasMap;
import org.apache.hadoop.hdfs.server.common.blockaliasmap.impl.TextFileRegionAliasMap;
import org.apache.hadoop.hdfs.server.datanode.DirectoryScanner.ReportCompiler;
import org.apache.hadoop.hdfs.server.datanode.FileIoProvider;
import org.apache.hadoop.hdfs.server.datanode.ReplicaBuilder;
import org.apache.hadoop.hdfs.server.datanode.ReplicaInPipeline;
import org.apache.hadoop.hdfs.server.datanode.ReplicaInfo;
import org.apache.hadoop.hdfs.server.datanode.StorageLocation;
import org.apache.hadoop.hdfs.server.datanode.checker.VolumeCheckResult;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;
import org.apache.hadoop.util.DiskChecker.DiskErrorException;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.util.Timer;

import org.apache.hadoop.classification.VisibleForTesting;

/**
 * 文件说明：HDFS DataNode 外置提供存储卷实现类，管理存储在外部存储系统中的数据块副本
 * 核心职责：实现PROVIDED类型存储卷，支持从外部存储系统读取数据块，不支持写入操作
 */
@InterfaceAudience.Private
class ProvidedVolumeImpl extends FsVolumeImpl {

  /**
   * 获取全路径除去前缀后的后缀部分，用于构造块路径相对路径
   * @param prefix 路径前缀
   * @param fullPath 完整路径
   * @return 去除前缀并处理开头斜杠后的后缀
   */
  @VisibleForTesting
  protected static String getSuffix(final Path prefix, final Path fullPath) {
    String prefixStr = prefix.toString();
    String pathStr = fullPath.toString();
    if (!pathStr.startsWith(prefixStr)) {
      LOG.debug("Path {} is not a prefix of the path {}", prefix, fullPath);
      return pathStr;
    }
    String suffix = pathStr.replaceFirst("^" + prefixStr, "");
    if (suffix.startsWith("/")) {
      suffix = suffix.substring(1);
    }
    return suffix;
  }

  /**
   * 存储容量使用统计类，维护外置提供卷的已用空间统计
   */
  public static class ProvidedVolumeDF {

    private AtomicLong used = new AtomicLong();

    public long getSpaceUsed() {
      return used.get();
    }

    public void decDfsUsed(long value) {
      used.addAndGet(-value);
    }

    public void incDfsUsed(long value) {
      used.addAndGet(value);
    }

    public long getCapacity() {
      return getSpaceUsed();
    }
  }

  /**
   * 外置存储块池切片类，管理单个块池内的外置块元数据和别名映射
   */
  static class ProvidedBlockPoolSlice {
    private ProvidedVolumeImpl providedVolume;

    private BlockAliasMap<FileRegion> aliasMap;
    private Configuration conf;
    private String bpid;
    private ReplicaMap bpVolumeMap;
    private ProvidedVolumeDF df;
    private AtomicLong numOfBlocks = new AtomicLong();
    private int numRetries;

    /**
     * 构造块池切片，初始化块别名映射
     * @param bpid 块池ID
     * @param volume 所属外置存储卷
     * @param conf Hadoop配置
     */
    ProvidedBlockPoolSlice(String bpid, ProvidedVolumeImpl volume,
        Configuration conf) {
      this.providedVolume = volume;
      bpVolumeMap = new ReplicaMap();
      Class<? extends BlockAliasMap> fmt =
          conf.getClass(DFSConfigKeys.DFS_PROVIDED_ALIASMAP_CLASS,
              TextFileRegionAliasMap.class, BlockAliasMap.class);
      aliasMap = ReflectionUtils.newInstance(fmt, conf);
      this.conf = conf;
      this.bpid = bpid;
      this.df = new ProvidedVolumeDF();
      bpVolumeMap.initBlockPool(bpid);
      this.numRetries = conf.getInt(DFS_PROVIDED_ALIASMAP_LOAD_RETRIES, 0);
      LOG.info("Created alias map using class: " + aliasMap.getClass());
    }

    BlockAliasMap<FileRegion> getBlockAliasMap() {
      return aliasMap;
    }

    @VisibleForTesting
    void setFileRegionProvider(BlockAliasMap<FileRegion> blockAliasMap) {
      this.aliasMap = blockAliasMap;
    }

    /**
     * 从块别名映射拉取块信息，加载到卷副本映射中
     * @param volumeMap 全局卷副本映射
     * @param ramDiskReplicaTracker RamDisk副本追踪器
     * @param remoteFS 远程文件系统
     * @throws IOException 加载失败时抛出异常
     */
    void fetchVolumeMap(ReplicaMap volumeMap,
        RamDiskReplicaTracker ramDiskReplicaMap, FileSystem remoteFS)
        throws IOException {
      BlockAliasMap.Reader<FileRegion> reader = null;
      int tries = 1;
      do {
        try {
          reader = aliasMap.getReader(null, bpid);
          break;
        } catch (IOException e) {
          tries++;
          reader = null;
        }
      } while (tries <= numRetries);

      if (reader == null) {
        LOG.error("Got null reader from BlockAliasMap " + aliasMap
            + "; no blocks will be populated");
        return;
      }
      Path blockPrefixPath = new Path(providedVolume.getBaseURI());
      // 遍历所有块区域记录
      for (FileRegion region : reader) {
        // 检查块是否属于当前卷
        if (containsBlock(providedVolume.baseURI,
            region.getProvidedStorageLocation().getPath().toUri())) {
          // 获取块相对卷根路径的后缀
          String blockSuffix = getSuffix(blockPrefixPath,
              new Path(region.getProvidedStorageLocation().getPath().toUri()));
          PathHandle pathHandle = null;
          // 如果有nonce则构造路径句柄
          if (region.getProvidedStorageLocation().getNonce().length > 0) {
            pathHandle = new RawPathHandle(ByteBuffer
                .wrap(region.getProvidedStorageLocation().getNonce()));
          }
          // 构造已完成副本对象
          ReplicaInfo newReplica = new ReplicaBuilder(ReplicaState.FINALIZED)
              .setBlockId(region.getBlock().getBlockId())
              .setPathPrefix(blockPrefixPath)
              .setPathSuffix(blockSuffix)
              .setOffset(region.getProvidedStorageLocation().getOffset())
              .setLength(region.getBlock().getNumBytes())
              .setGenerationStamp(region.getBlock().getGenerationStamp())
              .setPathHandle(pathHandle)
              .setFsVolume(providedVolume)
              .setConf(conf)
              .setRemoteFS(remoteFS)
              .build();
          ReplicaInfo oldReplica =
              volumeMap.get(bpid, newReplica.getBlockId());
          if (oldReplica == null) {
            // 添加到全局和块池本地副本映射
            volumeMap.add(bpid, newReplica);
            bpVolumeMap.add(bpid, newReplica);
            incrNumBlocks();
            incDfsUsed(region.getBlock().getNumBytes());
          } else {
            LOG.warn("A block with id " + newReplica.getBlockId()
                + " exists locally. Skipping PROVIDED replica");
          }
        }
      }
    }

    private void incrNumBlocks() {
      numOfBlocks.incrementAndGet();
    }

    public boolean isEmpty() {
      return bpVolumeMap.size(bpid) == 0;
    }

    public void shutdown(BlockListAsLongs blocksListsAsLongs) {
      // nothing to do!
    }

    /**
     * 编译块扫描报告，刷新别名映射后添加所有块到扫描报告
     * @param report 扫描报告集合
     * @param reportCompiler 报告编译器
     * @throws IOException IO异常
     * @throws InterruptedException 中断异常
     */
    public void compileReport(Collection<ScanInfo> report,
        ReportCompiler reportCompiler)
        throws IOException, InterruptedException {
      /* refresh the aliasMap and return the list of blocks found.
       * the assumption here is that the block ids in the external
       * block map, after the refresh, are consistent with those
       * from before the refresh, i.e., for blocks which did not change,
       * the ids remain the same.
       */
      aliasMap.refresh();
      BlockAliasMap.Reader<FileRegion> reader = aliasMap.getReader(null, bpid);
      for (FileRegion region : reader) {
        reportCompiler.throttle();
        report.add(new ScanInfo(region.getBlock().getBlockId(), providedVolume,
            region, region.getProvidedStorageLocation().getLength()));
      }
    }

    public long getNumOfBlocks() {
      return numOfBlocks.get();
    }

    long getDfsUsed() throws IOException {
      return df.getSpaceUsed();
    }

    void incDfsUsed(long value) {
      df.incDfsUsed(value);
    }
  }

  private URI baseURI;
  private final Map<String, ProvidedBlockPoolSlice> bpSlices =
      new ConcurrentHashMap<String, ProvidedBlockPoolSlice>();

  private ProvidedVolumeDF df;
  // 该外置卷指向的远程文件系统
  private FileSystem remoteFS;

  /**
   * 构造外置提供存储卷，初始化远程文件系统连接
   * @param dataset 所属文件数据集
   * @param storageID 存储ID
   * @param sd 存储目录
   * @param fileIoProvider 文件IO提供者
   * @param conf Hadoop配置
   * @throws IOException 初始化失败抛出异常
   */
  ProvidedVolumeImpl(FsDatasetImpl dataset, String storageID,
      StorageDirectory sd, FileIoProvider fileIoProvider,
      Configuration conf) throws IOException {
    super(dataset, storageID, sd, fileIoProvider, conf, null);
    assert getStorageLocation().getStorageType() == StorageType.PROVIDED:
      "Only provided storages must use ProvidedVolume";

    baseURI = getStorageLocation().getUri();
    df = new ProvidedVolumeDF();
    remoteFS = FileSystem.get(baseURI, conf);
  }

  @Override
  public String[] getBlockPoolList() {
    return bpSlices.keySet().toArray(new String[bpSlices.keySet().size()]);
  }

  @Override
  public long getCapacity() {
    try {
      // 外置卷容量默认等于已使用空间
      return getDfsUsed();
    } catch (IOException e) {
      LOG.warn("Exception when trying to get capacity of ProvidedVolume: {}",
          e);
    }
    return 0L;
  }

  @Override
  public long getDfsUsed() throws IOException {
    long dfsUsed = 0;
    synchronized(getDataset()) {
      for(ProvidedBlockPoolSlice s : bpSlices.values()) {
        dfsUsed += s.getDfsUsed();
      }
    }
    return dfsUsed;
  }

  @Override
  long getBlockPoolUsed(String bpid) throws IOException {
    return getProvidedBlockPoolSlice(bpid).getDfsUsed();
  }

  @Override
  public long getAvailable() throws IOException {
    long remaining = getCapacity() - getDfsUsed();
    // 外置存储不允许剩余空间为负，避免NameNode标记容量溢出
    if (remaining < 0L) {
      LOG.warn("Volume {} has less than 0 available space", this);
      return 0L;
    }
    return remaining;
  }

  @Override
  long getActualNonDfsUsed() throws IOException {
    return 0L;
  }

  @Override
  public long getNonDfsUsed() throws IOException {
    return 0L;
  }

  @Override
  long getNumBlocks() {
    long numBlocks = 0L;
    for (ProvidedBlockPoolSlice s : bpSlices.values()) {
      numBlocks += s.getNumBlocks();
    }
    return numBlocks;
  }

  @Override
  void incDfsUsedAndNumBlocks(String bpid, long value) {
    throw new UnsupportedOperationException(
        "ProvidedVolume does not yet support writes");
  }

  @Override
  public URI getBaseURI() {
    return baseURI;
  }

  @Override
  public File getFinalizedDir(String bpid) throws IOException {
    return null;
  }

  @Override
  public void reserveSpaceForReplica(long bytesToReserve) {
    throw new UnsupportedOperationException(
        "ProvidedVolume does not yet support writes");
  }

  @Override
  public void releaseReservedSpace(long bytesToRelease) {
    throw new UnsupportedOperationException(
        "ProvidedVolume does not yet support writes");
  }

  private static final ObjectWriter WRITER =
      new ObjectMapper().writerWithDefaultPrettyPrinter();

  /**
   * 块迭代器状态类，用于持久化保存迭代进度
   */
  private static class ProvidedBlockIteratorState {
    ProvidedBlockIteratorState() {
      iterStartMs = Time.now();
      lastSavedMs = iterStartMs;
      lastBlockId = -1L;
    }

    // 上次保存状态的时间戳
    @JsonProperty
    private long lastSavedMs;

    // 迭代器创建时间戳
    @JsonProperty
    private long iterStartMs;

    // 上次读取的最后一个块ID，假设块按块ID排序返回
    @JsonProperty
    private long lastBlockId;
  }

  /**
   * 外置存储块迭代器实现类，支持从别名映射迭代块
   */
  private class ProviderBlockIteratorImpl
      implements FsVolumeSpi.BlockIterator {

    private String bpid;
    private String name;
    private BlockAliasMap<FileRegion> blockAliasMap;
    private Iterator<FileRegion> blockIterator;
    private ProvidedBlockIteratorState state;

    /**
     * 构造迭代器，初始化并重绕迭代
     * @param bpid 块池ID
     * @param name 迭代器名称
     * @param blockAliasMap 块别名映射
     */
    ProviderBlockIteratorImpl(String bpid, String name,
        BlockAliasMap<FileRegion> blockAliasMap) {
      this.bpid = bpid;
      this.name = name;
      this.blockAliasMap = blockAliasMap;
      rewind();
    }

    @Override
    public void close() throws IOException {
      blockAliasMap.close();
    }

    @Override
    public ExtendedBlock nextBlock() throws IOException {
      if (null == blockIterator || !blockIterator.hasNext()) {
        return null;
      }
      FileRegion nextRegion = null;
      // 跳过小于上次保存块ID的块，支持断点续迭代
      while (null == nextRegion && blockIterator.hasNext()) {
        FileRegion temp = blockIterator.next();
        if (temp.getBlock().getBlockId() < state.lastBlockId) {
          continue;
        }
        nextRegion = temp;
      }
      if (null == nextRegion) {
        return null;
      }
      state.lastBlockId = nextRegion.getBlock().getBlockId();
      return new ExtendedBlock(bpid, nextRegion.getBlock());
    }

    @Override
    public boolean atEnd() {
      return blockIterator != null ? !blockIterator.hasNext(): true