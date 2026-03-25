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
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.IOException;

import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.FileIoProvider;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.nativeio.NativeIOException;
import org.slf4j.Logger;

/**
 * 存放数据节点副本的数据和校验和输入流，管理流的生命周期和所属卷资源
 */
public class ReplicaInputStreams implements Closeable {
  public static final Logger LOG = DataNode.LOG;

  private InputStream dataIn;
  private InputStream checksumIn;
  private FsVolumeReference volumeRef;
  private final FileIoProvider fileIoProvider;
  private FileDescriptor dataInFd = null;

  /**
   * 构造副本输入流对象，持有数据、校验和流与所属卷引用，并尝试获取数据文件描述符
   * @param dataStream 数据输入流
   * @param checksumStream 校验和输入流
   * @param volumeRef 所属卷的引用，用于资源管理
   * @param fileIoProvider 文件IO操作提供者，用于提供本地文件操作能力
   */
  public ReplicaInputStreams(
      InputStream dataStream, InputStream checksumStream,
      FsVolumeReference volumeRef, FileIoProvider fileIoProvider) {
    this.volumeRef = volumeRef;
    this.fileIoProvider = fileIoProvider;
    this.dataIn = dataStream;
    this.checksumIn = checksumStream;
    // 如果数据输入流是FileInputStream，尝试获取其文件描述符
    if (dataIn instanceof FileInputStream) {
      try {
        dataInFd = ((FileInputStream) dataIn).getFD();
      } catch (Exception e) {
        // 获取失败记录警告日志
        LOG.warn("Could not get file descriptor for inputstream of class " +
            this.dataIn.getClass());
      }
    } else {
      // 非文件输入流，无法获取文件描述符，记录调试日志
      LOG.debug("Could not get file descriptor for inputstream of class " +
          this.dataIn.getClass());
    }
  }

  /** @return 数据输入流 */
  public InputStream getDataIn() {
    return dataIn;
  }

  /** @return 校验和输入流 */
  public InputStream getChecksumIn() {
    return checksumIn;
  }

  /** @return 数据输入流对应的文件描述符，如果不是文件输入流则返回null */
  public FileDescriptor getDataInFd() {
    return dataInFd;
  }

  /** @return 副本所属卷的引用 */
  public FsVolumeReference getVolumeRef() {
    return volumeRef;
  }

  /**
   * 从数据输入流完全读取指定长度的数据到缓冲区
   * @param buf 目标字节缓冲区
   * @param off 缓冲区偏移量
   * @param len 需要读取的长度
   * @throws IOException 读取失败抛出IO异常
   */
  public void readDataFully(byte[] buf, int off, int len)
      throws IOException {
    IOUtils.readFully(dataIn, buf, off, len);
  }

  /**
   * 从校验和输入流完全读取指定长度的校验和数据到缓冲区
   * @param buf 目标字节缓冲区
   * @param off 缓冲区偏移量
   * @param len 需要读取的长度
   * @throws IOException 读取失败抛出IO异常
   */
  public void readChecksumFully(byte[] buf, int off, int len)
      throws IOException {
    IOUtils.readFully(checksumIn, buf, off, len);
  }

  /**
   * 从数据输入流完全跳过指定长度的数据
   * @param len 需要跳过的长度
   * @throws IOException 跳过失败抛出IO异常
   */
  public void skipDataFully(long len) throws IOException {
    IOUtils.skipFully(dataIn, len);
  }

  /**
   * 从校验和输入流完全跳过指定长度的数据
   * @param len 需要跳过的长度
   * @throws IOException 跳过失败抛出IO异常
   */
  public void skipChecksumFully(long len) throws IOException {
    IOUtils.skipFully(checksumIn, len);
  }

  /**
   * 关闭校验和输入流并置空引用
   * @throws IOException 关闭失败抛出IO异常
   */
  public void closeChecksumStream() throws IOException {
    IOUtils.closeStream(checksumIn);
    checksumIn = null;
  }

  /**
   * 通过posix fadvise通知内核释放已读取区域的页缓存，优化内存使用
   * @param identifier 文件名标识
   * @param offset 区域起始偏移量
   * @param len 区域长度
   * @param flags posix fadvise操作标志
   * @throws NativeIOException 本地IO操作失败抛出原生IO异常
   */
  public void dropCacheBehindReads(String identifier, long offset, long len,
      int flags) throws NativeIOException {
    assert this.dataInFd != null : "null dataInFd!";
    fileIoProvider.posixFadvise(getVolumeRef().getVolume(),
        identifier, dataInFd, offset, len, flags);
  }

  /**
   * 关闭所有流并释放卷引用，收集并抛出第一个遇到的IO异常
   * @throws IOException 关闭过程中出现IO异常抛出
   */
  public void closeStreams() throws IOException {
    IOException ioe = null;
    // 关闭校验和流
    if(checksumIn!=null) {
      try {
        checksumIn.close(); // 关闭校验和文件
      } catch (IOException e) {
        ioe = e;
      }
      checksumIn = null;
    }
    // 关闭数据流
    if(dataIn!=null) {
      try {
        dataIn.close(); // 关闭数据文件
      } catch (IOException e) {
        ioe = e;
      }
      dataIn = null;
      dataInFd = null;
    }
    // 释放卷引用
    if (volumeRef != null) {
      IOUtils.cleanupWithLogger(null, volumeRef);
      volumeRef = null;
    }
    // 如果有异常，抛出第一个遇到的异常
    if(ioe!= null) {
      throw ioe;
    }
  }

  @Override
  /**
   * 实现Closeable接口的关闭方法，静默关闭所有流并释放资源
   */
  public void close() {
    IOUtils.closeStream(dataIn);
    dataIn = null;
    dataInFd = null;
    IOUtils.closeStream(checksumIn);
    checksumIn = null;
    IOUtils.cleanupWithLogger(null, volumeRef);
    volumeRef = null;
  }
}