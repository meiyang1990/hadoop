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
import org.apache.hadoop.util.DataChecksum;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * 文件块加载到DataNode缓存区域的抽象基类，定义了块缓存加载的通用接口和公共校验逻辑，
 * 不同类型的缓存实现（如堆外内存、持久化内存）需要继承此类实现对应加载逻辑。
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public abstract class MappableBlockLoader {

  /**
   * 初始化MappableBlockLoader加载器，加载DataNode配置并完成缓存空间初始化。
   * @param dnConf DataNode配置对象
   * @return 缓存统计信息对象
   * @throws IOException 初始化失败时抛出IO异常
   */
  abstract CacheStats initialize(DNConf dnConf) throws IOException;

  /**
   * 将指定数据块加载映射到缓存区域，并完成数据校验和校验和验证。
   * @param length 数据块当前长度
   * @param blockIn 数据块输入流，调用方负责关闭流
   * @param metaIn 元数据文件输入流，调用方负责关闭流
   * @param blockFileName 数据块文件名，用于日志记录
   * @param key 数据块的扩展ID
   * @return 映射完成的可映射块对象
   * @throws IOException 映射失败或校验和验证失败时抛出IO异常
   */
  abstract MappableBlock load(long length, FileInputStream blockIn,
      FileInputStream metaIn, String blockFileName, ExtendedBlockId key)
      throws IOException;

  /**
   * 为指定数据块预分配缓存空间。
   * @param key 数据块扩展ID
   * @param bytesCount 需要预分配的字节数
   * @return 分配成功后返回当前已使用缓存总字节数；分配失败返回-1
   */
  abstract long reserve(ExtendedBlockId key, long bytesCount);

  /**
   * 释放指定数据块占用的缓存空间。
   * @param key 数据块扩展ID
   * @param bytesCount 需要释放的字节数
   * @return 释放完成后当前已使用缓存总字节数
   */
  abstract long release(ExtendedBlockId key, long bytesCount);

  /**
   * 获取当前已使用缓存空间的近似值。
   * @return 已使用缓存字节数
   */
  abstract long getCacheUsed();

  /**
   * 获取缓存总容量。
   * @return 缓存总容量字节数
   */
  abstract long getCacheCapacity();

  /**
   * 检查当前缓存是否为易失性缓存（重启后数据丢失）。
   * @return 非持久化缓存返回true，持久化缓存返回false
   */
  abstract boolean isTransientCache();

  /**
   * 检查当前加载器是否为原生PMEM（持久化内存）加载器。
   * @return 是原生PMEM加载器返回true，否则返回false
   */
  abstract boolean isNativeLoader();

  /**
   * 从持久化内存中恢复已存在的可映射块。
   * @param cacheFile 缓存文件
   * @param bpid 块池ID
   * @param volumeIndex 卷索引
   * @return 恢复后的可映射块对象
   * @throws IOException 恢复失败时抛出IO异常
   */
  abstract MappableBlock getRecoveredMappableBlock(
      File cacheFile, String bpid, byte volumeIndex) throws IOException;

  /**
   * 关闭清理加载器资源，在DataNode关闭时调用。
   */
  void shutdown() {
    // Do nothing.
  }

  /**
   * 验证数据块的校验和，这是一个IO密集型操作。
   * @param length 数据块长度
   * @param metaIn 元数据输入流
   * @param blockChannel 数据块文件通道
   * @param blockFileName 数据块文件名，用于日志
   * @throws IOException 校验失败或IO错误时抛出异常
   */
  protected void verifyChecksum(long length, FileInputStream metaIn,
      FileChannel blockChannel, String blockFileName) throws IOException {
    // 从元数据文件读取校验信息，读取数据头获取校验和对象
    BlockMetadataHeader header =
        BlockMetadataHeader.readHeader(new DataInputStream(
            new BufferedInputStream(metaIn, BlockMetadataHeader
                .getHeaderSize())));
    // 获取元数据文件通道，自动关闭处理
    try (FileChannel metaChannel = metaIn.getChannel()) {
      if (metaChannel == null) {
        throw new IOException(
            "Block InputStream meta file has no FileChannel.");
      }
      // 获取校验和配置
      DataChecksum checksum = header.getChecksum();
      final int bytesPerChecksum = checksum.getBytesPerChecksum();
      final int checksumSize = checksum.getChecksumSize();
      // 一次批量处理8MB对应的数据块分块
      final int numChunks = (8 * 1024 * 1024) / bytesPerChecksum;
      // 分配数据块缓冲区和校验和缓冲区
      ByteBuffer blockBuf = ByteBuffer.allocate(numChunks * bytesPerChecksum);
      ByteBuffer checksumBuf = ByteBuffer.allocate(numChunks * checksumSize);
      int bytesVerified = 0;
      // 分块循环校验所有数据
      while (bytesVerified < length) {
        Preconditions.checkState(bytesVerified % bytesPerChecksum == 0,
            "Unexpected partial chunk before EOF");
        assert bytesVerified % bytesPerChecksum == 0;
        // 读取数据块到缓冲区
        int bytesRead = fillBuffer(blockChannel, blockBuf);
        if (bytesRead == -1) {
          throw new IOException("checksum verification failed: premature EOF");
        }
        // 翻转缓冲区准备读取
        blockBuf.flip();
        // 计算读取到的分块数量，处理最后一个可能的不完整块
        int chunks = (bytesRead + bytesPerChecksum - 1) / bytesPerChecksum;
        // 限制校验和读取长度匹配实际分块数量
        checksumBuf.limit(chunks * checksumSize);
        // 读取对应数量的校验和
        fillBuffer(metaChannel, checksumBuf);
        checksumBuf.flip();
        // 逐块验证校验和
        checksum.verifyChunkedSums(blockBuf, checksumBuf, blockFileName,
            bytesVerified);
        // 更新已验证字节数统计
        bytesVerified += bytesRead;
        // 清空缓冲区准备下一轮读取
        blockBuf.clear();
        checksumBuf.clear();
      }
    }
  }

  /**
   * 从文件通道读取数据填满缓冲区，直到缓冲区满或者到达EOF。
   * @param channel 源文件通道
   * @param buf 目标字节缓冲区
   * @return 实际读取的字节数；到达EOF返回-1
   * @throws IOException 读取IO错误时抛出异常
   */
  protected int fillBuffer(FileChannel channel, ByteBuffer buf)
      throws IOException {
    int bytesRead = channel.read(buf);
    if (bytesRead < 0) {
      //EOF
      return bytesRead;
    }
    // 持续读取直到缓冲区满或者到达EOF
    while (buf.remaining() > 0) {
      int n = channel.read(buf);
      if (n < 0) {
        //EOF
        return bytesRead;
      }
      bytesRead += n;
    }
    return bytesRead;
  }
}