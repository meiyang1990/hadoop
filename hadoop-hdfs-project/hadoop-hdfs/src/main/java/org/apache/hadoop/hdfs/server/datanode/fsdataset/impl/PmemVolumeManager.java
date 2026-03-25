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

import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.TrueFileFilter;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.ExtendedBlockId;
import org.apache.hadoop.io.nativeio.NativeIO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 文件级注释：持久化内存（PMEM）卷管理器，管理DataNode节点上的持久化内存缓存卷，负责缓存空间分配、块定位、缓存恢复等功能
 * 是HDFS持久化内存缓存功能的核心管理组件
 */
/**
 * Manage the persistent memory volumes.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public final class PmemVolumeManager {

  /**
   * 持久化内存已使用字节计数器，线程安全地记录每个PMEM卷的空间使用情况
   */
  /**
   * Counts used bytes for persistent memory.
   */
  private static class UsedBytesCount {
    private long maxBytes;
    private final AtomicLong usedBytes = new AtomicLong(0);

    UsedBytesCount(long maxBytes) {
      this.maxBytes = maxBytes;
    }

    /**
     * 尝试预留指定字节数的空间
     * @param bytesCount 需要预留的字节数
     * @return 预留成功返回新的已使用字节数，失败返回-1
     */
    /**
     * Try to reserve more bytes.
     *
     * @param bytesCount    The number of bytes to add.
     *
     * @return              The new number of usedBytes if we succeeded;
     *                      -1 if we failed.
     */
    long reserve(long bytesCount) {
      while (true) {
        long cur = usedBytes.get();
        long next = cur + bytesCount;
        if (next > maxBytes) {
          return -1;
        }
        if (usedBytes.compareAndSet(cur, next)) {
          return next;
        }
      }
    }

    /**
     * 释放指定字节数的空间
     * @param bytesCount 需要释放的字节数
     * @return 释放后新的已使用字节数
     */
    /**
     * Release some bytes that we're using.
     *
     * @param bytesCount    The number of bytes to release.
     *
     * @return              The new number of usedBytes.
     */
    long release(long bytesCount) {
      return usedBytes.addAndGet(-bytesCount);
    }

    long getUsedBytes() {
      return usedBytes.get();
    }

    long getMaxBytes() {
      return maxBytes;
    }

    long getAvailableBytes() {
      return maxBytes - usedBytes.get();
    }

    void setMaxBytes(long maxBytes) {
      this.maxBytes = maxBytes;
    }
  }

  private static final Logger LOG =
      LoggerFactory.getLogger(PmemVolumeManager.class);
  public static final String CACHE_DIR = "hdfs_pmem_cache";
  private static PmemVolumeManager pmemVolumeManager = null;
  private final ArrayList<String> pmemVolumes = new ArrayList<>();
  // 维护块ID与对应缓存卷索引的映射关系
  // Maintain which pmem volume a block is cached to.
  private final Map<ExtendedBlockId, Byte> blockKeyToVolume =
      new ConcurrentHashMap<>();
  private final List<UsedBytesCount> usedBytesCounts = new ArrayList<>();
  private boolean cacheRecoveryEnabled;

  /**
   * 持久化内存总缓存容量（字节）
   */
  /**
   * The total cache capacity in bytes of persistent memory.
   */
  private long cacheCapacity;
  private static long maxBytesPerPmem = -1;
  private int count = 0;
  private byte nextIndex = 0;

  /**
   * 私有构造函数，初始化PMEM卷管理器，加载并验证配置的PMEM卷
   * @param pmemVolumesConfig 配置的PMEM卷路径数组
   * @param cacheRecoveryEnabled 是否开启缓存恢复
   * @throws IOException 初始化失败抛出异常
   */
  private PmemVolumeManager(String[] pmemVolumesConfig,
                            boolean cacheRecoveryEnabled) throws IOException {
    if (pmemVolumesConfig == null || pmemVolumesConfig.length == 0) {
      throw new IOException("The persistent memory volume, " +
          DFSConfigKeys.DFS_DATANODE_PMEM_CACHE_DIRS_KEY +
          " is not configured!");
    }
    this.cacheRecoveryEnabled = cacheRecoveryEnabled;
    this.loadVolumes(pmemVolumesConfig);
    cacheCapacity = 0L;
    for (UsedBytesCount counter : usedBytesCounts) {
      cacheCapacity += counter.getMaxBytes();
    }
  }

  /**
   * 单例初始化方法，线程安全地初始化PMEM卷管理器实例
   * @param pmemVolumesConfig 配置的PMEM卷路径数组
   * @param cacheRecoveryEnabled 是否开启缓存恢复
   * @throws IOException 初始化失败抛出异常
   */
  public synchronized static void init(
      String[] pmemVolumesConfig, boolean cacheRecoveryEnabled)
      throws IOException {
    if (pmemVolumeManager == null) {
      pmemVolumeManager = new PmemVolumeManager(pmemVolumesConfig,
          cacheRecoveryEnabled);
    }
  }

  /**
   * 获取PMEM卷管理器单例实例
   * @return 单例实例
   */
  public static PmemVolumeManager getInstance() {
    if (pmemVolumeManager == null) {
      throw new RuntimeException(
          "The pmemVolumeManager should be instantiated!");
    }
    return pmemVolumeManager;
  }

  @VisibleForTesting
  public static void reset() {
    pmemVolumeManager = null;
  }

  @VisibleForTesting
  public static void setMaxBytes(long maxBytes) {
    maxBytesPerPmem = maxBytes;
  }

  /**
   * 获取当前持久化内存总已使用缓存字节数
   * @return 总已使用字节数
   */
  public long getCacheUsed() {
    long usedBytes = 0L;
    for (UsedBytesCount counter : usedBytesCounts) {
      usedBytes += counter.getUsedBytes();
    }
    return usedBytes;
  }

  /**
   * 获取当前持久化内存总缓存容量
   * @return 总缓存容量（字节）
   */
  public long getCacheCapacity() {
    return cacheCapacity;
  }

  /**
   * 为指定块预留持久化内存空间
   * @param key 块的ExtendedBlockId标识
   * @param bytesCount 需要预留的字节数
   * @return 预留成功返回新的已使用字节数，失败返回-1
   */
  /**
   * Try to reserve more bytes on persistent memory.
   *
   * @param key           The ExtendedBlockId for a block.
   *
   * @param bytesCount    The number of bytes to add.
   *
   * @return              The new number of usedBytes if we succeeded;
   *                      -1 if we failed.
   */
  synchronized long reserve(ExtendedBlockId key, long bytesCount) {
    try {
      // 选择满足空间要求的PMEM卷
      byte index = chooseVolume(bytesCount);
      long usedBytes = usedBytesCounts.get(index).reserve(bytesCount);
      // 预留成功，记录块与卷索引的映射关系
      // Put the entry into blockKeyToVolume if reserving bytes succeeded.
      if (usedBytes > 0) {
        blockKeyToVolume.put(key, index);
      }
      return usedBytes;
    } catch (IOException e) {
      LOG.warn(e.getMessage());
      return -1L;
    }
  }

  /**
   * 释放指定块占用的持久化内存空间
   * @param key 块的ExtendedBlockId标识
   * @param bytesCount 需要释放的字节数
   * @return 释放后新的已使用字节数
   */
  /**
   * Release some bytes that we're using on persistent memory.
   *
   * @param key           The ExtendedBlockId for a block.
   *
   * @param bytesCount    The number of bytes to release.
   *
   * @return              The new number of usedBytes.
   */
  long release(ExtendedBlockId key, long bytesCount) {
    Byte index = blockKeyToVolume.remove(key);
    return usedBytesCounts.get(index).release(bytesCount);
  }

  /**
   * 加载并验证配置的所有PMEM卷，初始化每个卷的空间计数器
   * @param volumes 配置的PMEM卷路径数组
   * @throws IOException 没有可用有效PMEM卷抛出异常
   */
  /**
   * Load and verify the configured pmem volumes.
   *
   * @throws IOException   If there is no available pmem volume.
   */
  private void loadVolumes(String[] volumes)
      throws IOException {
    // 遍历检查每个配置的PMEM卷
    // Check whether the volume exists
    for (byte n = 0; n < volumes.length; n++) {
      try {
        File pmemDir = new File(volumes[n]);
        File realPmemDir = verifyIfValidPmemVolume(pmemDir);
        // 未开启缓存恢复，清理该卷上原有缓存数据
        if (!cacheRecoveryEnabled) {
          // Clean up the cache left before, if any.
          cleanup(realPmemDir);
        }
        this.pmemVolumes.add(realPmemDir.getPath());
        long maxBytes;
        // 未指定单卷最大容量，使用卷可用空间作为总容量
        if (maxBytesPerPmem == -1) {
          maxBytes = realPmemDir.getUsableSpace();
        } else {
          maxBytes = maxBytesPerPmem;
        }
        UsedBytesCount usedBytesCount = new UsedBytesCount(maxBytes);
        this.usedBytesCounts.add(usedBytesCount);
        LOG.info("Added persistent memory - {} with size={}",
            volumes[n], maxBytes);
      } catch (IllegalArgumentException e) {
        LOG.error("Failed to parse persistent memory volume " + volumes[n], e);
        continue;
      } catch (IOException e) {
        LOG.error("Bad persistent memory volume: " + volumes[n], e);
        continue;
      }
    }
    count = pmemVolumes.size();
    if (count == 0) {
      throw new IOException(
          "At least one valid persistent memory volume is required!");
    }
  }

  void cleanup(File realPmemDir) {
    try {
      FileUtils.cleanDirectory(realPmemDir);
    } catch (IOException e) {
      LOG.error("Failed to clean up " + realPmemDir.getPath(), e);
    }
  }

  void cleanup() {
    // 清理所有PMEM卷下的所有文件
    // Remove all files under the volume.
    for (String pmemVolume : pmemVolumes) {
      cleanup(new File(pmemVolume));
    }
  }

  /**
   * 从PMEM卷中恢复已有缓存，加载所有缓存块信息
   * @param bpid 块池ID
   * @param cacheLoader 可映射块加载器
   * @return 恢复得到的块ID到可映射块的映射表
   * @throws IOException 恢复过程IO异常
   */
  /**
   * Recover cache from the cached files in the configured pmem volumes.
   */
  public Map<ExtendedBlockId, MappableBlock> recoverCache(
      String bpid, MappableBlockLoader cacheLoader) throws IOException {
    final Map<ExtendedBlockId, MappableBlock> keyToMappableBlock
        = new ConcurrentHashMap<>();
    // 遍历所有PMEM卷恢复缓存
    for (byte volumeIndex = 0; volumeIndex < pmemVolumes.size();
         volumeIndex++) {
      long maxBytes = usedBytesCounts.get(volumeIndex).getMaxBytes();
      long usedBytes = 0;
      // 获取当前卷对应块池的缓存目录
      File cacheDir = new File(pmemVolumes.get(volumeIndex), bpid);
      Collection<File> cachedFileList = FileUtils.listFiles(cacheDir,
          TrueFileFilter.INSTANCE, TrueFileFilter.INSTANCE);
      // 扫描该目录下所有缓存文件进行恢复
      // Scan the cached files in pmem volumes for cache recovery.
      for (File cachedFile : cachedFileList) {
        MappableBlock mappableBlock = cacheLoader.
            getRecoveredMappableBlock(cachedFile, bpid, volumeIndex);
        ExtendedBlockId key = mappableBlock.getKey();
        keyToMappableBlock.put(key, mappableBlock);
        usedBytes += cachedFile.length();
      }
      // 根据已恢复缓存占用空间更新容量和使用量
      // Update maxBytes and cache capacity according to cache space
      // used by recovered cached files.
      usedBytesCounts.get(volumeIndex).setMaxBytes(maxBytes + usedBytes);
      cacheCapacity += usedBytes;
      usedBytesCounts.get(volumeIndex).reserve(usedBytes);
    }
    return keyToMappableBlock;
  }

  /**
   * 恢复块与PMEM卷索引的映射关系
   * @param key 块的ExtendedBlockId标识
   * @param volumeIndex PMEM卷索引
   */
  public void recoverBlockKeyToVolume(ExtendedBlockId key, byte volumeIndex) {
    blockKeyToVolume.put(key, volumeIndex);
  }

  @VisibleForTesting
  /**
   * 验证指定路径是否为有效的PMEM卷，通过内存映射写测试验证可用性
   * @param pmemDir PMEM目录路径
   * @return 验证通过返回实际缓存目录
   * @throws IOException 验证失败抛出异常
   */
  static File verifyIfValidPmemVolume(File pmemDir)
      throws IOException {
    if (!pmemDir.exists()) {
      final String message = pmemDir + " does not exist";
      throw new IOException(message);
    }
    if (!pmemDir.isDirectory()) {
      final String message = pmemDir + " is not a directory";
      throw new IllegalArgumentException(message);
    }

    // 获取实际缓存目录路径
    File realPmemDir = new File(getRealPmemDir(pmemDir.getPath()));
    if (!realPmemDir.exists() && !realPmemDir.mkdir()) {
      throw new IOException("Failed to create " + realPmemDir.getPath());
    }

    // 生成随机测试文件，测试内存映射写功能
    String uuidStr = UUID.randomUUID().toString();
    String testFilePath = realPmemDir.getPath() + "/.verify.pmem." + uuidStr;
    byte[] contents = uuidStr.getBytes(StandardCharsets.UTF_8);
    RandomAccessFile testFile = null;
    MappedByteBuffer out = null;
    try {
      testFile = new RandomAccessFile(testFilePath, "rw");
      out = testFile.getChannel().map(FileChannel.MapMode.READ_WRITE, 0,
          contents.length);
      if (out == null) {
        throw new IOException(
            "Failed to map the test file under " + realPmemDir);
      }
      out.put(contents);
      // 强制将数据写入存储设备
      // Forces to write data to storage device containing the mapped file
      out.force();
      return realPmemDir;
    } catch (IOException e) {
      throw new IOException(
          "Exception while writing data to persistent storage dir: " +
              realPmemDir, e);
    } finally {
      // 清理测试文件资源
      if (out != null) {
        out.clear();
      }
      if (testFile != null) {
        IOUtils.closeStream(testFile);
        NativeIO.POSIX.munmap(out);
        try {
          FsDatasetUtil.deleteMappedFile(testFilePath);
        } catch (IOException e) {
          LOG.warn("Failed to delete test file " + testFilePath +
              " from