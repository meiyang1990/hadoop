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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.ExtendedBlockId;
import org.apache.hadoop.hdfs.server.datanode.DNConf;
import org.apache.hadoop.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;

/**
 * 文件整体说明：将HDFS数据块映射到持久化内存(PMem)的加载器，实现基于内存映射的持久化内存块缓存功能
 * 是DataNode持久化内存缓存功能的核心实现类，替代传统DRAM缓存提供块缓存能力
 * 
 * Maps block to persistent memory by using mapped byte buffer.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class PmemMappableBlockLoader extends MappableBlockLoader {
  private static final Logger LOG =
      LoggerFactory.getLogger(PmemMappableBlockLoader.class);
  // PMem卷管理器实例，负责PMem空间的分配与管理
  private PmemVolumeManager pmemVolumeManager;
  // 是否开启缓存恢复功能，重启DataNode时恢复已有缓存
  private boolean cacheRecoveryEnabled;

  /**
   * 初始化PMem可映射块加载器，完成PMem卷管理器初始化并返回缓存统计信息
   * @param dnConf DataNode配置对象
   * @return 缓存统计信息，当前不支持惰性写入故返回最大锁定内存为0
   * @throws IOException 初始化失败时抛出IO异常
   */
  @Override
  CacheStats initialize(DNConf dnConf) throws IOException {
    LOG.info("Initializing cache loader: " + this.getClass().getName());
    PmemVolumeManager.init(dnConf.getPmemVolumes(),
        dnConf.getPmemCacheRecoveryEnabled());
    pmemVolumeManager = PmemVolumeManager.getInstance();
    cacheRecoveryEnabled = dnConf.getPmemCacheRecoveryEnabled();
    // The configuration for max locked memory is shaded.
    LOG.info("Persistent memory is used for caching data instead of " +
        "DRAM. Max locked memory is set to zero to disable DRAM cache");
    // TODO: PMem is not supporting Lazy Writer now, will refine this stats
    // while implementing it.
    return new CacheStats(0L);
  }

  /**
   * Load the block.
   *
   * Map the block and verify its checksum.
   *
   * The block will be mapped to PmemDir/BlockPoolId/subdir#/subdir#/BlockId,
   * in which PmemDir is a persistent memory volume chosen by PmemVolumeManager.
   *
   * @param length         The current length of the block.
   * @param blockIn        The block input stream. Should be positioned at the
   *                       start. The caller must close this.
   * @param metaIn         The meta file input stream. Should be positioned at
   *                       the start. The caller must close this.
   * @param blockFileName  The block file name, for logging purposes.
   * @param key            The extended block ID.
   *
   * @throws IOException   If mapping block fails or checksum fails.
   *
   * @return               The Mappable block.
   */
  @Override
  MappableBlock load(long length, FileInputStream blockIn,
      FileInputStream metaIn, String blockFileName, ExtendedBlockId key)
      throws IOException {
    PmemMappedBlock mappableBlock = null;
    String cachePath = null;

    FileChannel blockChannel = null;
    RandomAccessFile cacheFile = null;
    try {
      // 获取原块文件的通道
      blockChannel = blockIn.getChannel();
      if (blockChannel == null) {
        throw new IOException("Block InputStream has no FileChannel.");
      }
      // 根据块ID从PMem卷管理器获取缓存路径
      cachePath = pmemVolumeManager.getCachePath(key);
      // 创建PMem缓存文件的随机访问文件对象
      cacheFile = new RandomAccessFile(cachePath, "rw");
      // 将原块数据零拷贝传输到PMem缓存文件
      blockChannel.transferTo(0, length, cacheFile.getChannel());

      // Verify checksum for the cached data instead of block file.
      // The file channel should be repositioned.
      // 重置缓存文件通道位置到开头，准备校验和验证
      cacheFile.getChannel().position(0);
      // 验证缓存数据的校验和
      verifyChecksum(length, metaIn, cacheFile.getChannel(), blockFileName);

      // 创建PMem映射块对象
      mappableBlock = new PmemMappedBlock(length, key);
      LOG.info("Successfully cached one replica:{} into persistent memory"
          + ", [cached path={}, length={}]", key, cachePath, length);
    } finally {
      // 关闭原块文件通道
      IOUtils.closeStream(blockChannel);
      // 关闭缓存文件
      IOUtils.closeStream(cacheFile);
      // 加载失败时清理不完整的缓存文件
      if (mappableBlock == null) {
        LOG.debug("Delete {} due to unsuccessful mapping.", cachePath);
        FsDatasetUtil.deleteMappedFile(cachePath);
      }
    }
    return mappableBlock;
  }

  /**
   * 获取PMem缓存已使用字节数
   * @return PMem缓存已使用字节数
   */
  @Override
  public long getCacheUsed() {
    return pmemVolumeManager.getCacheUsed();
  }

  /**
   * 获取PMem缓存总容量字节数
   * @return PMem缓存总容量字节数
   */
  @Override
  public long getCacheCapacity() {
    return pmemVolumeManager.getCacheCapacity();
  }

  /**
   * 为指定块预留PMem空间
   * @param key 扩展块ID
   * @param bytesCount 需要预留的字节数
   * @return 预留后剩余可用字节数
   */
  @Override
  long reserve(ExtendedBlockId key, long bytesCount) {
    return pmemVolumeManager.reserve(key, bytesCount);
  }

  /**
   * 释放指定块占用的PMem空间
   * @param key 扩展块ID
   * @param bytesCount 需要释放的字节数
   * @return 释放后剩余可用字节数
   */
  @Override
  long release(ExtendedBlockId key, long bytesCount) {
    return pmemVolumeManager.release(key, bytesCount);
  }

  /**
   * 判断当前缓存是否为临时缓存
   * @return 持久化内存缓存不是临时缓存，返回false
   */
  @Override
  public boolean isTransientCache() {
    return false;
  }

  /**
   * 判断当前加载器是否为原生加载器
   * @return 当前实现为Java层映射，不是原生加载器，返回false
   */
  @Override
  public boolean isNativeLoader() {
    return false;
  }

  /**
   * DataNode重启时，从已有缓存文件恢复块缓存
   * @param cacheFile PMem中的缓存文件
   * @param bpid 块池ID
   * @param volumeIndex PMem卷索引
   * @return 恢复后的可映射块对象
   * @throws IOException 恢复失败时抛出IO异常
   */
  @Override
  public MappableBlock getRecoveredMappableBlock(
      File cacheFile, String bpid, byte volumeIndex) throws IOException {
    // 从缓存文件名解析出块ID，构造扩展块ID
    ExtendedBlockId key = new ExtendedBlockId(getBlockId(cacheFile), bpid);
    // 创建PMem映射块对象
    MappableBlock mappableBlock = new PmemMappedBlock(cacheFile.length(), key);
    // 将恢复的块关联到对应PMem卷
    PmemVolumeManager.getInstance().recoverBlockKeyToVolume(key, volumeIndex);

    // 获取块缓存路径并打印恢复日志
    String path = PmemVolumeManager.getInstance().getCachePath(key);
    long length = mappableBlock.getLength();
    LOG.info("Recovering persistent memory cache for block {}, " +
        "path = {}, length = {}", key, path, length);
    return mappableBlock;
  }

  /**
   * Parse the file name and get the BlockId.
   * 从缓存文件名解析出块ID，缓存文件以块ID作为文件名
   * @param file PMem缓存文件对象
   * @return 块ID
   */
  public long getBlockId(File file) {
    return Long.parseLong(file.getName());
  }

  /**
   * 关闭PMem块加载器，根据配置决定是否清理全部缓存
   */
  @Override
  void shutdown() {
    if (!cacheRecoveryEnabled) {
      LOG.info("Clean up cache on persistent memory during shutdown.");
      PmemVolumeManager.getInstance().cleanup();
    }
  }
}