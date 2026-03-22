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
import org.apache.hadoop.io.nativeio.NativeIO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

/**
 * 文件级内存映射块加载器，将HDFS数据块通过mmap映射到进程地址空间，
 * 用于DN块缓存功能，提升热点数据访问性能。
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class MemoryMappableBlockLoader extends MappableBlockLoader {
  private static final Logger LOG =
      LoggerFactory.getLogger(MemoryMappableBlockLoader.class);
  /** 内存缓存使用统计 */
  private CacheStats memCacheStats;

  /**
   * 初始化内存映射块加载器，创建缓存统计对象
   * @param dnConf 数据节点配置
   * @return 初始化后的缓存统计对象
   * @throws IOException 初始化失败时抛出
   */
  @Override
  CacheStats initialize(DNConf dnConf) throws IOException {
    LOG.info("Initializing cache loader: MemoryMappableBlockLoader.");
    this.memCacheStats = new CacheStats(dnConf.getMaxLockedMemory());
    return memCacheStats;
  }

  /**
   * 将指定数据块加载到内存，通过mmap映射并mlock锁定内存，完成校验和验证
   *
   * @param length         数据块当前长度
   * @param blockIn        数据块输入流，已定位到起始位置，调用者负责关闭
   * @param metaIn         元数据文件输入流，已定位到起始位置，调用者负责关闭
   * @param blockFileName  数据块文件名，用于日志输出
   * @param key            扩展块ID，标识唯一数据块
   *
   * @throws IOException   内存映射失败或校验和验证失败时抛出
   * @return               映射完成的可访问块对象
   */
  @Override
  MappableBlock load(long length, FileInputStream blockIn,
      FileInputStream metaIn, String blockFileName, ExtendedBlockId key)
      throws IOException {
    MemoryMappedBlock mappableBlock = null;
    MappedByteBuffer mmap = null;
    // 自动关闭文件通道
    try (FileChannel blockChannel = blockIn.getChannel()) {
      if (blockChannel == null) {
        throw new IOException("Block InputStream has no FileChannel.");
      }
      // 创建只读内存映射
      mmap = blockChannel.map(FileChannel.MapMode.READ_ONLY, 0, length);
      // 调用原生mlock锁定内存到物理内存，防止被交换出去
      NativeIO.POSIX.getCacheManipulator().mlock(blockFileName, mmap, length);
      // 验证块校验和，确保数据完整性
      verifyChecksum(length, metaIn, blockChannel, blockFileName);
      // 封装为MemoryMappedBlock返回
      mappableBlock = new MemoryMappedBlock(mmap, length);
    } finally {
      // 加载失败时释放已分配的内存映射
      if (mappableBlock == null) {
        if (mmap != null) {
          NativeIO.POSIX.munmap(mmap); // 取消映射同时会自动解锁内存
        }
      }
    }
    return mappableBlock;
  }

  /**
   * 获取当前已使用的缓存内存大小
   * @return 已使用缓存字节数
   */
  @Override
  public long getCacheUsed() {
    return memCacheStats.getCacheUsed();
  }

  /**
   * 获取缓存总容量
   * @return 缓存总容量字节数
   */
  @Override
  public long getCacheCapacity() {
    return memCacheStats.getCacheCapacity();
  }

  /**
   * 预分配指定大小的缓存空间
   * @param key 目标数据块ID
   * @param bytesCount 需要预分配的字节数
   * @return 分配后剩余可用缓存大小
   */
  @Override
  long reserve(ExtendedBlockId key, long bytesCount) {
    return memCacheStats.reserve(bytesCount);
  }

  /**
   * 释放指定大小的缓存空间
   * @param key 目标数据块ID
   * @param bytesCount 需要释放的字节数
   * @return 释放后剩余可用缓存大小
   */
  @Override
  long release(ExtendedBlockId key, long bytesCount) {
    return memCacheStats.release(bytesCount);
  }

  /**
   * 判断是否为瞬时缓存，内存映射重启后不会持久化
   * @return 固定返回true，表示该缓存是瞬时的
   */
  @Override
  public boolean isTransientCache() {
    return true;
  }

  /**
   * 恢复持久化缓存中的块，本实现不支持持久化缓存，直接返回null
   * @param cacheFile 缓存文件
   * @param bpid 块池ID
   * @param volumeIndex 卷索引
   * @return 固定返回null
   * @throws IOException 永远不会抛出
   */
  @Override
  public MappableBlock getRecoveredMappableBlock(
      File cacheFile, String bpid, byte volumeIndex) throws IOException {
    return null;
  }

  /**
   * 判断是否为原生加载器，本实现是Java层面的内存映射，不是原生加载器
   * @return 固定返回false
   */
  @Override
  public boolean isNativeLoader() {
    return false;
  }
}