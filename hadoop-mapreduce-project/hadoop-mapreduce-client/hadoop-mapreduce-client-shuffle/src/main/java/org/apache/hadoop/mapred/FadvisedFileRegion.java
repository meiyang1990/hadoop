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

package org.apache.hadoop.mapred;

import java.io.FileDescriptor;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;

import io.netty.channel.DefaultFileRegion;
import org.apache.hadoop.io.ReadaheadPool;
import org.apache.hadoop.io.ReadaheadPool.ReadaheadRequest;
import org.apache.hadoop.io.nativeio.NativeIO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.io.nativeio.NativeIO.POSIX.POSIX_FADV_DONTNEED;

import org.apache.hadoop.classification.VisibleForTesting;

/**
 * MapReduce Shuffle阶段支持POSIX缓存管理和预读优化的Netty文件区域实现。
 * 扩展了Netty DefaultFileRegion，增加了操作系统缓存管理、预读池优化和自定义缓冲区传输功能，
 * 优化Shuffle阶段数据输出性能，解决不同平台下transferTo性能问题。
 */
public class FadvisedFileRegion extends DefaultFileRegion {

  private static final Logger LOG =
      LoggerFactory.getLogger(FadvisedFileRegion.class);

  // 关闭操作同步锁
  private final Object closeLock = new Object();
  // 是否管理操作系统页缓存
  private final boolean manageOsCache;
  // 预读长度
  private final int readaheadLength;
  // 预读池实例
  private final ReadaheadPool readaheadPool;
  // 文件描述符
  private final FileDescriptor fd;
  // 文件标识，用于日志和调试
  private final String identifier;
  // 传输总字节数
  private final long count;
  // 起始偏移量
  private final long position;
  // 自定义传输缓冲区大小
  private final int shuffleBufferSize;
  // 是否允许使用原生transferTo传输
  private final boolean shuffleTransferToAllowed;
  // 待传输文件通道
  private final FileChannel fileChannel;

  // 当前活跃的预读请求
  private volatile ReadaheadRequest readaheadRequest;

  /**
   * 构造支持缓存管理和预读优化的FadvisedFileRegion实例。
   * @param file 待传输随机访问文件
   * @param position 起始偏移量
   * @param count 传输总字节数
   * @param manageOsCache 是否管理操作系统页缓存
   * @param readaheadLength 预读长度
   * @param readaheadPool 预读池实例
   * @param identifier 文件标识
   * @param shuffleBufferSize 自定义传输缓冲区大小
   * @param shuffleTransferToAllowed 是否允许原生transferTo传输
   * @throws IOException 文件获取通道或描述符失败时抛出
   */
  public FadvisedFileRegion(RandomAccessFile file, long position, long count,
      boolean manageOsCache, int readaheadLength, ReadaheadPool readaheadPool,
      String identifier, int shuffleBufferSize,
      boolean shuffleTransferToAllowed) throws IOException {
    super(file.getChannel(), position, count);
    this.manageOsCache = manageOsCache;
    this.readaheadLength = readaheadLength;
    this.readaheadPool = readaheadPool;
    this.fd = file.getFD();
    this.identifier = identifier;
    this.fileChannel = file.getChannel();
    this.count = count;
    this.position = position;
    this.shuffleBufferSize = shuffleBufferSize;
    this.shuffleTransferToAllowed = shuffleTransferToAllowed;
  }

  /**
   * 将文件区域数据传输到目标通道，支持预读优化和自定义传输策略。
   * @param target 目标可写字节通道
   * @param position 当前传输偏移量（相对于本区域起始位置）
   * @return 实际传输字节数
   * @throws IOException 传输过程中IO错误时抛出
   */
  @Override
  public long transferTo(WritableByteChannel target, long position)
          throws IOException {
    synchronized (closeLock) {
      if (fd.valid()) {
        // 提交预读请求到预读池
        if (readaheadPool != null && readaheadLength > 0) {
          readaheadRequest = readaheadPool.readaheadStream(identifier, fd,
                  position() + position, readaheadLength,
                  position() + count(), readaheadRequest);
        }

        if(this.shuffleTransferToAllowed) {
          // 使用Netty原生transferTo传输
          return super.transferTo(target, position);
        } else {
          // 使用自定义缓冲区传输，适配Windows平台性能问题
          return customShuffleTransfer(target, position);
        }
      } else {
        // 文件描述符已失效，返回0字节
        return 0L;
      }
    }

  }

  /**
   * This method transfers data using local buffer. It transfers data from
   * a disk to a local buffer in memory, and then it transfers data from the
   * buffer to the target. This is used only if transferTo is disallowed in
   * the configuration file. super.TransferTo does not perform well on Windows
   * due to a small IO request generated. customShuffleTransfer can control
   * the size of the IO requests by changing the size of the intermediate
   * buffer.
   */
  @VisibleForTesting
  long customShuffleTransfer(WritableByteChannel target, long position)
          throws IOException {
    long actualCount = this.count - position;
    // 参数范围校验
    if (actualCount < 0 || position < 0) {
      throw new IllegalArgumentException(
              "position out of range: " + position +
                      " (expected: 0 - " + (this.count - 1) + ')');
    }
    if (actualCount == 0) {
      return 0L;
    }

    long remaining = actualCount;
    int readSize;
    // 分配中间缓冲区，大小不超过剩余传输量和配置的缓冲区大小
    ByteBuffer byteBuffer = ByteBuffer.allocate(
            Math.min(
                    this.shuffleBufferSize,
                    remaining > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) remaining));

    // 循环读取直到传输完成或读完
    while(remaining > 0L &&
            (readSize = fileChannel.read(byteBuffer, this.position+position)) > 0) {
      // 根据读取量调整计数器和缓冲区
      if(readSize < remaining) {
        remaining -= readSize;
        position += readSize;
        byteBuffer.flip();
      } else {
        // 剩余量不足一个缓冲区，手动调整缓冲区限制
        byteBuffer.limit((int)remaining);
        byteBuffer.position(0);
        position += remaining;
        remaining = 0;
      }

      // 将缓冲区数据写入目标通道
      while(byteBuffer.hasRemaining()) {
        target.write(byteBuffer);
      }

      // 清空缓冲区准备下一次读取
      byteBuffer.clear();
    }

    // 返回实际传输字节数
    return actualCount - remaining;
  }


  /**
   * 释放资源，取消预读请求。
   */
  @Override
  protected void deallocate() {
    synchronized (closeLock) {
      if (readaheadRequest != null) {
        // 取消未完成的预读请求
        readaheadRequest.cancel();
        readaheadRequest = null;
      }
      super.deallocate();
    }
  }

  /**
   * Call when the transfer completes successfully so we can advise the OS that
   * we don't need the region to be cached anymore.
   * 传输成功完成后通知操作系统本区域数据不需要继续缓存，释放页缓存。
   */
  public void transferSuccessful() {
    synchronized (closeLock) {
      if (fd.valid() && manageOsCache && count() > 0) {
        try {
          // 通过posix_fadvise告知操作系统不需要缓存此区域数据，避免内存占用
          NativeIO.POSIX.getCacheManipulator().posixFadviseIfPossible(identifier,
                  fd, position(), count(), POSIX_FADV_DONTNEED);
        } catch (Throwable t) {
          LOG.warn("Failed to manage OS cache for " + identifier +
                  " fd " + fd, t);
        }
      }
    }
  }
}