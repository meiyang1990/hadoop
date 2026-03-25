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
package org.apache.hadoop.hdfs.server.datanode.fsdataset;

import java.io.Closeable;
import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.io.IOException;

import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.FileIoProvider;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.nativeio.NativeIOException;
import org.apache.hadoop.util.DataChecksum;
import org.slf4j.Logger;

/**
 * 保存数据块副本的数据输出流和校验和输出流，提供统一的流管理和IO操作接口。
 * 用于DataNode写入数据块副本时，同时管理数据文件和校验文件的输出。
 */
public class ReplicaOutputStreams implements Closeable {
  public static final Logger LOG = DataNode.LOG;

  private FileDescriptor outFd = null;
  /** Stream to block. */
  private OutputStream dataOut;
  /** Stream to checksum. */
  private final OutputStream checksumOut;
  private final DataChecksum checksum;
  private final FsVolumeSpi volume;
  private final FileIoProvider fileIoProvider;

  /**
   * 构造副本输出流对象，封装数据输出流、校验和输出流和校验和对象。
   * @param dataOut 数据块文件输出流
   * @param checksumOut 校验和文件输出流
   * @param checksum 校验和计算对象
   * @param volume 副本所在的FsVolume
   * @param fileIoProvider IO操作提供者，包装底层文件操作
   */
  public ReplicaOutputStreams(
      OutputStream dataOut, OutputStream checksumOut, DataChecksum checksum,
      FsVolumeSpi volume, FileIoProvider fileIoProvider) {

    this.dataOut = dataOut;
    this.checksum = checksum;
    this.checksumOut = checksumOut;
    this.volume = volume;
    this.fileIoProvider = fileIoProvider;

    try {
      // 如果数据输出流是文件输出流，提取其文件描述符供后续原生IO操作使用
      if (this.dataOut instanceof FileOutputStream) {
        this.outFd = ((FileOutputStream)this.dataOut).getFD();
      } else {
        LOG.debug("Could not get file descriptor for outputstream of class " +
            this.dataOut.getClass());
      }
    } catch (IOException e) {
      LOG.warn("Could not get file descriptor for outputstream of class " +
          this.dataOut.getClass());
    }
  }

  /**
   * 获取数据输出流对应的文件描述符。
   * @return 文件描述符，非文件输出流时为null
   */
  public FileDescriptor getOutFd() {
    return outFd;
  }

  /** @return the data output stream. */
  public OutputStream getDataOut() {
    return dataOut;
  }

  /** @return the checksum output stream. */
  public OutputStream getChecksumOut() {
    return checksumOut;
  }

  /** @return the checksum. */
  public DataChecksum getChecksum() {
    return checksum;
  }

  /** @return is writing to a transient storage? */
  public boolean isTransientStorage() {
    return volume.isTransientStorage();
  }

  @Override
  /**
   * 关闭数据输出流和校验和输出流，释放资源。
   */
  public void close() {
    IOUtils.closeStream(dataOut);
    IOUtils.closeStream(checksumOut);
  }

  /**
   * 单独关闭数据输出流，用于写入完成后提前释放数据文件句柄。
   * @throws IOException 关闭失败时抛出IO异常
   */
  public void closeDataStream() throws IOException {
    dataOut.close();
    dataOut = null;
  }

  /**
   * 将数据输出流的缓冲区数据同步到磁盘（刷盘持久化）。
   * @throws IOException 同步失败时抛出IO异常
   */
  public void syncDataOut() throws IOException {
    if (dataOut instanceof FileOutputStream) {
      fileIoProvider.sync(volume, (FileOutputStream) dataOut);
    }
  }
  
  /**
   * 将校验和输出流的缓冲区数据同步到磁盘（刷盘持久化）。
   * @throws IOException 同步失败时抛出IO异常
   */
  public void syncChecksumOut() throws IOException {
    if (checksumOut instanceof FileOutputStream) {
      fileIoProvider.sync(volume, (FileOutputStream) checksumOut);
    }
  }

  /**
   * 刷新数据输出流缓冲区。
   * @throws IOException 刷新失败时抛出IO异常
   */
  public void flushDataOut() throws IOException {
    if (dataOut != null) {
      fileIoProvider.flush(volume, dataOut);
    }
  }

  /**
   * 刷新校验和输出流缓冲区。
   * @throws IOException 刷新失败时抛出IO异常
   */
  public void flushChecksumOut() throws IOException {
    if (checksumOut != null) {
      fileIoProvider.flush(volume, checksumOut);
    }
  }

  /**
   * 将数据写入数据输出流到磁盘。
   * @param b 待写入字节数组
   * @param off 数组起始偏移量
   * @param len 待写入长度
   * @throws IOException 写入失败时抛出IO异常
   */
  public void writeDataToDisk(byte[] b, int off, int len)
      throws IOException {
    dataOut.write(b, off, len);
  }

  /**
   * 对指定文件范围执行同步持久化操作（基于原生系统调用）。
   * @param offset 起始偏移量
   * @param nbytes 同步字节数
   * @param flags 同步标志位
   * @throws NativeIOException 原生IO操作失败时抛出异常
   */
  public void syncFileRangeIfPossible(long offset, long nbytes,
      int flags) throws NativeIOException {
    fileIoProvider.syncFileRange(
        volume, outFd, offset, nbytes, flags);
  }

  /**
   * 调用posix_fadvise释放已写入区域的页缓存，避免不必要的内存占用。
   * @param identifier 文件标识符，用于日志
   * @param offset 起始偏移量
   * @param len 释放长度
   * @param flags posix_fadvise标志位，通常是POSIX_FADV_DONTNEED
   * @throws NativeIOException 原生IO操作失败时抛出异常
   */
  public void dropCacheBehindWrites(String identifier,
      long offset, long len, int flags) throws NativeIOException {
    fileIoProvider.posixFadvise(
        volume, identifier, outFd, offset, len, flags);
  }
}