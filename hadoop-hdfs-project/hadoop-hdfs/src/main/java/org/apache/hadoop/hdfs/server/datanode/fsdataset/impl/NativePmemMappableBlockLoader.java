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

import org.apache.hadoop.util.Preconditions;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.ExtendedBlockId;
import org.apache.hadoop.hdfs.server.datanode.BlockMetadataHeader;
import org.apache.hadoop.hdfs.server.datanode.DNConf;
import org.apache.hadoop.io.nativeio.NativeIO;
import org.apache.hadoop.io.nativeio.NativeIO.POSIX;
import org.apache.hadoop.util.DataChecksum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * 文件说明：使用原生PMDK库将HDFS数据块映射到持久化内存(PMEM)的加载器实现
 * 功能描述：负责将DataNode上的数据块加载并映射到持久化内存区域，提供低延迟的数据块访问能力
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class NativePmemMappableBlockLoader extends PmemMappableBlockLoader {
  private static final Logger LOG =
      LoggerFactory.getLogger(NativePmemMappableBlockLoader.class);

  /**
   * 初始化持久化内存块加载器，继承父类初始化逻辑
   * @param dnConf DataNode配置对象
   * @return 缓存统计信息对象
   * @throws IOException 初始化失败时抛出异常
   */
  @Override
  CacheStats initialize(DNConf dnConf) throws IOException {
    return super.initialize(dnConf);
  }

  /**
   * 加载数据块并映射到持久化内存
   * 功能流程：映射块到持久化内存 -> 校验校验和 -> 将数据复制到持久化内存
   * 映射路径规则：PmemDir/BlockPoolId/子目录#/子目录#/BlockId，其中PmemDir由PmemVolumeManager分配
   * @param length         数据块当前长度
   * @param blockIn        数据块输入流，起始位置已定位到开头，由调用方负责关闭
   * @param metaIn         元数据文件输入流，起始位置已定位到开头，由调用方负责关闭
   * @param blockFileName  数据块文件名，用于日志输出
   * @param key            扩展块ID，唯一标识该数据块
   *
   * @throws IOException   映射失败或校验和校验失败时抛出异常
   *
   * @return               映射完成的可访问块对象
   */
  @Override
  public MappableBlock load(long length, FileInputStream blockIn,
      FileInputStream metaIn, String blockFileName,
      ExtendedBlockId key)
      throws IOException {
    NativePmemMappedBlock mappableBlock = null;
    POSIX.PmemMappedRegion region = null;
    String filePath = null;

    // 获取数据块文件通道并自动关闭
    try (FileChannel blockChannel = blockIn.getChannel()) {
      if (blockChannel == null) {
        throw new IOException("Block InputStream has no FileChannel.");
      }

      assert NativeIO.isAvailable();
      // 根据块ID计算持久化内存中的缓存路径
      filePath = PmemVolumeManager.getInstance().getCachePath(key);
      // 通过原生PMDK接口映射块到持久化内存
      region = POSIX.Pmem.mapBlock(filePath, length, false);
      if (region == null) {
        throw new IOException("Failed to map the block " + blockFileName +
            " to persistent storage.");
      }
      // 校验和校验并将数据复制到持久化内存
      verifyChecksumAndMapBlock(region, length, metaIn, blockChannel,
          blockFileName);
      // 构建映射块对象
      mappableBlock = new NativePmemMappedBlock(region.getAddress(),
          region.getLength(), key);
      LOG.info("Successfully cached one replica:{} into persistent memory"
              + ", [cached path={}, address={}, length={}]", key, filePath,
          region.getAddress(), length);
    } finally {
      // 加载失败时清理已分配的资源
      if (mappableBlock == null) {
        if (region != null) {
          // 取消持久化内存映射
          POSIX.Pmem.unmapBlock(region.getAddress(),
              region.getLength());
          // 删除已创建的映射文件
          FsDatasetUtil.deleteMappedFile(filePath);
        }
      }
    }
    return mappableBlock;
  }

  /**
   * 校验数据块校验和并将数据复制到持久化内存，该操作是I/O密集型操作
   * @param region         持久化内存映射区域对象
   * @param length         数据块长度
   * @param metaIn         元数据输入流
   * @param blockChannel   数据块文件通道
   * @param blockFileName  数据块文件名，用于日志和错误信息
   * @throws IOException   读取失败、校验失败或写入持久化内存失败时抛出异常
   */
  private void verifyChecksumAndMapBlock(POSIX.PmemMappedRegion region,
      long length, FileInputStream metaIn, FileChannel blockChannel,
      String blockFileName) throws IOException {
    // 从元数据文件头读取校验和信息
    BlockMetadataHeader header =
        BlockMetadataHeader.readHeader(new DataInputStream(
            new BufferedInputStream(metaIn, BlockMetadataHeader
                .getHeaderSize())));
    // 获取元数据文件通道并自动关闭
    try (FileChannel metaChannel = metaIn.getChannel()) {
      if (metaChannel == null) {
        throw new IOException("Cannot get FileChannel" +
            " from Block InputStream meta file.");
      }
      // 获取校验和对象与块参数
      DataChecksum checksum = header.getChecksum();
      final int bytesPerChecksum = checksum.getBytesPerChecksum();
      final int checksumSize = checksum.getChecksumSize();
      // 计算每次处理的块数，单次最大处理8MB数据
      final int numChunks = (8 * 1024 * 1024) / bytesPerChecksum;
      // 分配数据块缓冲区和校验和缓冲区
      ByteBuffer blockBuf = ByteBuffer.allocate(numChunks * bytesPerChecksum);
      ByteBuffer checksumBuf = ByteBuffer.allocate(numChunks * checksumSize);
      // 已校验字节数统计
      int bytesVerified = 0;
      long mappedAddress = -1L;
      if (region != null) {
        mappedAddress = region.getAddress();
      }
      // 循环分块校验数据
      while (bytesVerified < length) {
        Preconditions.checkState(bytesVerified % bytesPerChecksum == 0,
            "Unexpected partial chunk before EOF.");
        assert bytesVerified % bytesPerChecksum == 0;
        // 从数据块通道读取数据到缓冲区
        int bytesRead = fillBuffer(blockChannel, blockBuf);
        if (bytesRead == -1) {
          throw new IOException(
              "Checksum verification failed for the block " + blockFileName +
                  ": premature EOF");
        }
        // 翻转缓冲区准备读取
        blockBuf.flip();
        // 计算本次读取的块数（包含末尾可能的不完整块）
        int chunks = (bytesRead + bytesPerChecksum - 1) / bytesPerChecksum;
        checksumBuf.limit(chunks * checksumSize);
        // 从元数据通道读取校验和数据
        fillBuffer(metaChannel, checksumBuf);
        // 翻转缓冲区准备读取
        checksumBuf.flip();
        // 分块校验数据校验和
        checksum.verifyChunkedSums(blockBuf, checksumBuf, blockFileName,
            bytesVerified);
        // 更新已校验字节计数
        bytesVerified += bytesRead;
        // 将数据复制到持久化内存映射区域
        POSIX.Pmem.memCopy(blockBuf.array(), mappedAddress,
            region.isPmem(), bytesRead);
        // 更新下一次复制的目标地址
        mappedAddress += bytesRead;
        // 清空缓冲区准备下一轮读取
        blockBuf.clear();
        checksumBuf.clear();
      }
      // 持久化内存数据同步刷盘
      if (region != null) {
        POSIX.Pmem.memSync(region);
      }
    }
  }

  /**
   * 判断当前加载器是否为原生加载器
   * @return 始终返回true，表示这是原生实现的加载器
   */
  @Override
  public boolean isNativeLoader() {
    return true;
  }

  /**
   * 从已有持久化内存缓存文件恢复映射块
   * @param cacheFile  持久化内存中的缓存文件
   * @param bpid       块池ID
   * @param volumeIndex 卷索引
   * @return 恢复完成的可映射块对象
   * @throws IOException 恢复失败或映射失败时抛出异常
   */
  @Override
  public MappableBlock getRecoveredMappableBlock(
      File cacheFile, String bpid, byte volumeIndex) throws IOException {
    // 映射已有缓存文件到进程地址空间
    NativeIO.POSIX.PmemMappedRegion region =
        NativeIO.POSIX.Pmem.mapBlock(cacheFile.getAbsolutePath(),
            cacheFile.length(), true);
    if (region == null) {
      throw new IOException("Failed to recover the block "
          + cacheFile.getName() + " in persistent storage.");
    }
    // 构建扩展块ID
    ExtendedBlockId key =
        new ExtendedBlockId(super.getBlockId(cacheFile), bpid);
    // 构建映射块对象
    MappableBlock mappableBlock = new NativePmemMappedBlock(
        region.getAddress(), region.getLength(), key);
    // 注册块到卷的映射关系，供后续管理使用
    PmemVolumeManager.getInstance().recoverBlockKeyToVolume(key, volumeIndex);

    // 输出恢复日志
    String path = PmemVolumeManager.getInstance().getCachePath(key);
    long addr = mappableBlock.getAddress();
    long length = mappableBlock.getLength();
    LOG.info("Recovering persistent memory cache for block {}, " +
        "path = {}, address = {}, length = {}", key, path, addr, length);
    return mappableBlock;
  }
}