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

import org.apache.hadoop.classification.VisibleForTesting;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.handler.stream.ChunkedFile;
import org.apache.hadoop.io.ReadaheadPool;
import org.apache.hadoop.io.ReadaheadPool.ReadaheadRequest;
import org.apache.hadoop.io.nativeio.NativeIO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.io.nativeio.NativeIO.POSIX.POSIX_FADV_DONTNEED;

/**
 * 支持操作系统缓存管理和预读优化的分块文件读取类，扩展Netty的ChunkedFile，用于MapReduce Shuffle阶段数据传输
 * 通过posix fadvise机制管理OS缓存，减少MapReduce shuffle过程中不必要的内存占用
 */
public class FadvisedChunkedFile extends ChunkedFile {

  private static final Logger LOG =
      LoggerFactory.getLogger(FadvisedChunkedFile.class);

  // 关闭操作锁，保证多线程安全
  private final Object closeLock = new Object();
  // 是否由本类管理操作系统页缓存
  private final boolean manageOsCache;
  // 预读长度
  private final int readaheadLength;
  // 预读请求池，用于异步预读提升读取性能
  private final ReadaheadPool readaheadPool;
  // 目标文件的文件描述符
  private final FileDescriptor fd;
  // 文件标识，用于日志和调试
  private final String identifier;

  // 当前正在处理的预读请求，volatile保证多线程可见性
  private volatile ReadaheadRequest readaheadRequest;

  /**
   * 构造FadvisedChunkedFile对象，初始化分块读取配置和缓存管理参数
   * @param file 要读取的随机访问文件
   * @param position 读取起始偏移量
   * @param count 要读取的总字节数
   * @param chunkSize 每个分块大小
   * @param manageOsCache 是否开启操作系统缓存管理
   * @param readaheadLength 预读字节长度
   * @param readaheadPool 预读请求池实例
   * @param identifier 文件标识，用于日志
   * @throws IOException 文件打开异常
   */
  public FadvisedChunkedFile(RandomAccessFile file, long position, long count,
      int chunkSize, boolean manageOsCache, int readaheadLength,
      ReadaheadPool readaheadPool, String identifier) throws IOException {
    super(file, position, count, chunkSize);
    this.manageOsCache = manageOsCache;
    this.readaheadLength = readaheadLength;
    this.readaheadPool = readaheadPool;
    this.fd = file.getFD();
    this.identifier = identifier;
  }

  @VisibleForTesting
  FileDescriptor getFd() {
    return fd;
  }

  /**
   * 读取下一个分块数据，在读取前提交预读请求提升性能
   * @param allocator Netty ByteBuf分配器
   * @return 读取到的ByteBuf，文件读取完毕或文件已关闭返回null
   * @throws Exception 读取过程异常
   */
  @Override
  public ByteBuf readChunk(ByteBufAllocator allocator) throws Exception {
    synchronized (closeLock) {
      // 检查文件描述符是否有效
      if (fd.valid()) {
        // 如果开启缓存管理且预读池存在，提交预读请求
        if (manageOsCache && readaheadPool != null) {
          readaheadRequest = readaheadPool
              .readaheadStream(
                  identifier, fd, currentOffset(), readaheadLength,
                  endOffset(), readaheadRequest);
        }
        // 调用父类方法读取分块
        return super.readChunk(allocator);
      } else {
        return null;
      }
    }
  }

  /**
   * 关闭分块文件，清理预读请求并告知操作系统不再需要该文件缓存，释放OS内存
   * @throws Exception 关闭过程异常
   */
  @Override
  public void close() throws Exception {
    synchronized (closeLock) {
      // 取消未完成的预读请求
      if (readaheadRequest != null) {
        readaheadRequest.cancel();
        readaheadRequest = null;
      }
      // 如果开启缓存管理，通知OS这片文件数据不再需要，可释放页缓存
      if (fd.valid() &&
          manageOsCache && endOffset() - startOffset() > 0) {
        try {
          NativeIO.POSIX.getCacheManipulator().posixFadviseIfPossible(
              identifier,
              fd,
              startOffset(), endOffset() - startOffset(),
              POSIX_FADV_DONTNEED);
        } catch (Throwable t) {
          LOG.warn("Failed to manage OS cache for " + identifier +
              " fd " + fd.toString(), t);
        }
      }
      // fd becomes invalid upon closing
      super.close();
    }
  }
}