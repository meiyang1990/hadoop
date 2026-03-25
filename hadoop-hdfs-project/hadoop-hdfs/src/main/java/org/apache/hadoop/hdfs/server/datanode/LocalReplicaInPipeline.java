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
package org.apache.hadoop.hdfs.server.datanode;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.ReplicaState;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.ReplicaOutputStreams;
import org.apache.hadoop.hdfs.server.protocol.ReplicaRecoveryInfo;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.DataChecksum;
import org.apache.hadoop.util.StringUtils;

/**
 * @file org/apache/hadoop/hdfs/server/datanode/LocalReplicaInPipeline.java
 * @brief 数据节点管道写入过程中的本地副本实现，负责管理正在写入或复制的临时副本状态
 * 
 * 该类表示HDFS数据节点上处于流水线写入过程中的副本，包含两种场景：
 * 1. 客户端正在写入的持久化副本
 * 2. 用于数据节点间复制、数据均衡的临时副本
 * 继承LocalReplica实现了ReplicaInPipeline接口，管理副本写入过程中的状态和空间预留
 */
public class LocalReplicaInPipeline extends LocalReplica
    implements ReplicaInPipeline {

  // 保护副本状态并发访问的锁
  private final Lock lock = new ReentrantLock();
  // 磁盘已写入字节变化条件变量，用于等待数据写入完成
  private final Condition bytesOnDiskChange = lock.newCondition();

  // 已被上游节点确认写入成功的字节数
  private long bytesAcked;
  // 已经写入磁盘的字节数
  private long bytesOnDisk;
  // 最后一个数据块的校验和
  private byte[] lastChecksum;
  // 当前写入该副本的线程原子引用，支持多线程竞争写入时的原子切换
  private AtomicReference<Thread> writer = new AtomicReference<Thread>();

  /**
   * Bytes reserved for this replica on the containing volume.
   * Based off difference between the estimated maximum block length and
   * the bytes already written to this block.
   */
  // 当前副本在卷上预留的磁盘字节数，基于预估最大块长度计算
  private long bytesReserved;
  // 初始预留的磁盘字节总数
  private final long originalBytesReserved;

  /**
   * 零长度副本构造函数，用于创建新的写入管道副本
   * @param blockId 块ID
   * @param genStamp 副本生成时间戳
   * @param vol 副本所在的卷
   * @param dir 存储块文件和元数据文件的目录
   * @param bytesToReserve 根据预估最大块长度需要预留的磁盘空间
   */
  public LocalReplicaInPipeline(long blockId, long genStamp,
        FsVolumeSpi vol, File dir, long bytesToReserve) {
    this(blockId, 0L, genStamp, vol, dir, Thread.currentThread(),
        bytesToReserve);
  }

  /**
   * 基于已有块构造管道写入副本
   * @param block 块对象
   * @param vol 副本所在的卷
   * @param dir 存储块文件和元数据文件的目录
   * @param writer 正在写入该副本的线程
   */
  LocalReplicaInPipeline(Block block,
      FsVolumeSpi vol, File dir, Thread writer) {
    this(block.getBlockId(), block.getNumBytes(), block.getGenerationStamp(),
        vol, dir, writer, 0L);
  }

  /**
   * 完整构造管道写入副本
   * @param blockId 块ID
   * @param len 副本当前长度
   * @param genStamp 副本生成时间戳
   * @param vol 副本所在的卷
   * @param dir 存储块文件和元数据文件的目录
   * @param writer 正在写入该副本的线程
   * @param bytesToReserve 根据预估最大块长度需要预留的磁盘空间
   */
  LocalReplicaInPipeline(long blockId, long len, long genStamp,
      FsVolumeSpi vol, File dir, Thread writer, long bytesToReserve) {
    super(blockId, len, genStamp, vol, dir);
    this.bytesAcked = len;
    this.bytesOnDisk = len;
    this.writer.set(writer);
    this.bytesReserved = bytesToReserve;
    this.originalBytesReserved = bytesToReserve;
  }

  /**
   * 拷贝构造函数，基于已有副本创建新的副本对象
   * @param from 源副本对象
   */
  public LocalReplicaInPipeline(LocalReplicaInPipeline from) {
    super(from);
    this.bytesAcked = from.getBytesAcked();
    this.bytesOnDisk = from.getBytesOnDisk();
    this.writer.set(from.writer.get());
    this.bytesReserved = from.bytesReserved;
    this.originalBytesReserved = from.originalBytesReserved;
  }

  @Override
  public long getVisibleLength() {
    return -1;
  }

  @Override  //ReplicaInfo
  public ReplicaState getState() {
    return ReplicaState.TEMPORARY;
  }

  @Override // ReplicaInPipeline
  public long getBytesAcked() {
    return bytesAcked;
  }

  @Override // ReplicaInPipeline
  public void setBytesAcked(long bytesAcked) {
    long newBytesAcked = bytesAcked - this.bytesAcked;
    this.bytesAcked = bytesAcked;

    // 确认成功后，释放已写入部分对应的预留空间，减少预留计数
    getVolume().releaseReservedSpace(newBytesAcked);
    bytesReserved -= newBytesAcked;
  }

  @Override // ReplicaInPipeline
  public long getBytesOnDisk() {
    return bytesOnDisk;
  }

  @Override
  public long getBytesReserved() {
    return bytesReserved;
  }

  @Override
  public long getOriginalBytesReserved() {
    return originalBytesReserved;
  }

  @Override // ReplicaInPipeline
  public void releaseAllBytesReserved() {
    // 释放所有剩余预留空间
    getVolume().releaseReservedSpace(bytesReserved);
    getVolume().releaseLockedMemory(bytesReserved);
    bytesReserved = 0;
  }

  @Override
  public void releaseReplicaInfoBytesReserved() {
    bytesReserved = 0;
  }

  @Override
  public void setLastChecksumAndDataLen(long dataLength, byte[] checksum) {
    lock.lock();
    try {
      // 更新磁盘已写入长度和最后一个块校验和
      this.bytesOnDisk = dataLength;
      this.lastChecksum = checksum;
      // 通知所有等待数据写入的线程
      bytesOnDiskChange.signalAll();
    } finally {
      lock.unlock();
    }
  }

  @Override
  public ChunkChecksum getLastChecksumAndDataLen() {
    lock.lock();
    try {
      return new ChunkChecksum(getBytesOnDisk(), lastChecksum);
    } finally {
      lock.unlock();
    }
  }

  @Override
  /**
   * 等待副本磁盘长度达到最小要求长度，用于数据恢复场景
   * @param minLength 需要达到的最小长度
   * @param time 最长等待时间
   * @param unit 时间单位
   * @throws IOException 等待超时或被中断时抛出异常
   */
  public void waitForMinLength(long minLength, long time, TimeUnit unit)
      throws IOException {
    long nanos = unit.toNanos(time);
    lock.lock();
    try {
      // 循环等待直到长度满足要求或超时
      while (bytesOnDisk < minLength) {
        if (nanos <= 0L) {
          throw new IOException(
              String.format("Need %d bytes, but only %d bytes available",
                  minLength, bytesOnDisk));
        }
        // 等待数据变化，返回剩余等待时间
        nanos = bytesOnDiskChange.awaitNanos(nanos);
      }
    } catch (InterruptedException e) {
      throw new IOException(e);
    } finally {
      lock.unlock();
    }
  }

  @Override // ReplicaInPipeline
  public void setWriter(Thread writer) {
    this.writer.set(writer);
  }

  @Override
  /**
   * 中断当前写入线程，如果写入线程存在且活跃
   */
  public void interruptThread() {
    Thread thread = writer.get();
    if (thread != null && thread != Thread.currentThread()
        && thread.isAlive()) {
      thread.interrupt();
    }
  }

  @Override  // Object
  public boolean equals(Object o) {
    return super.equals(o);
  }

  /**
   * Attempt to set the writer to a new value.
   */
  @Override // ReplicaInPipeline
  /**
   * 原子尝试切换写入线程，使用CAS操作保证并发安全
   * @param prevWriter 当前预期的旧写入线程
   * @param newWriter 需要设置的新写入线程
   * @return 切换成功返回true，失败返回false
   */
  public boolean attemptToSetWriter(Thread prevWriter, Thread newWriter) {
    return writer.compareAndSet(prevWriter, newWriter);
  }

  /**
   * Interrupt the writing thread and wait until it dies.
   * @throws IOException the waiting is interrupted
   */
  @Override // ReplicaInPipeline
  /**
   * 停止当前写入线程，中断并等待线程退出
   * @param xceiverStopTimeout 线程退出超时时间
   * @throws IOException 等待超时或被中断时抛出异常
   */
  public void stopWriter(long xceiverStopTimeout) throws IOException {
    while (true) {
      Thread thread = writer.get();
      if ((thread == null) || (thread == Thread.currentThread()) ||
          (!thread.isAlive())) {
        // 尝试原子清空写入线程，若成功则返回，否则重试处理新写入线程
        if (writer.compareAndSet(thread, null)) {
          return; // Done
        }
        // 写入线程已变更，回到循环开头处理新写入线程
        continue;
      }
      // 中断写入线程
      thread.interrupt();
      try {
        // 等待线程退出，超时则抛出异常
        thread.join(xceiverStopTimeout);
        if (thread.isAlive()) {
          // 线程退出超时，记录日志并抛出异常
          final String msg = "Join on writer thread " + thread + " timed out";
          DataNode.LOG.warn(msg + "\n" + StringUtils.getStackTrace(thread));
          throw new IOException(msg);
        }
      } catch (InterruptedException e) {
        throw new IOException("Waiting for writer thread is interrupted.");
      }
    }
  }

  @Override  // Object
  public int hashCode() {
    return super.hashCode();
  }

  @Override // ReplicaInPipeline
  /**
   * 创建副本的输出流，用于写入块数据和校验和元数据
   * @param isCreate 是否是新建块，false表示追加或恢复场景
   * @param requestedChecksum 请求使用的校验和
   * @return 封装好的输出流对象
   * @throws IOException 创建输出流或校验检查失败时抛出异常
   */
  public ReplicaOutputStreams createStreams(boolean isCreate,
      DataChecksum requestedChecksum) throws IOException {
    final File blockFile = getBlockFile();
    final File metaFile = getMetaFile();
    if (DataNode.LOG.isDebugEnabled()) {
      DataNode.LOG.debug("writeTo blockfile is " + blockFile +
                         " of size " + blockFile.length());
      DataNode.LOG.debug("writeTo metafile is " + metaFile +
                         " of size " + metaFile.length());
    }
    long blockDiskSize = 0L;
    long crcDiskSize = 0L;

    // 最终实际使用的校验和，追加场景下需要和原有校验和保持一致
    final DataChecksum checksum;

    // 打开元数据文件的随机访问流
    final RandomAccessFile metaRAF =
        getFileIoProvider().getRandomAccessFile(getVolume(), metaFile, "rw");

    if (!isCreate) {
      // 追加/恢复场景：必须沿用原有块的校验和，同时验证文件长度正确性
      boolean checkedMeta = false;
      try {
        // 读取元数据头部获取原有校验和信息
        BlockMetadataHeader header =
            BlockMetadataHeader.readHeader(metaRAF);
        checksum = header.getChecksum();

        // 检查请求校验和与原有校验和的块大小是否一致
        if (checksum.getBytesPerChecksum() !=
            requestedChecksum.getBytesPerChecksum()) {
          throw new IOException("Client requested checksum " +
              requestedChecksum + " when appending to an existing block " +
              "with different chunk size: " + checksum);
        }

        int bytesPerChunk = checksum.getBytesPerChecksum();
        int checksumSize = checksum.getChecksumSize();

        // 计算当前数据块和校验和文件应有的长度
        blockDiskSize = bytesOnDisk;
        crcDiskSize = BlockMetadataHeader.getHeaderSize() +
          (blockDiskSize+bytesPerChunk-1)/bytesPerChunk*checksumSize;
        // 检查实际磁盘文件长度是否符合预期，检测损坏
        if (blockDiskSize > 0 &&
            (blockDiskSize > blockFile.length() ||
               crcDiskSize>metaFile.length())) {
          throw new IOException("Corrupted block: " + this);
        }
        checkedMeta = true;
      } finally {
        if (!checkedMeta) {
          // 异常情况下清理已打开的流
          IOUtils.closeStream(metaRAF);
        }
      }
    } else {
      // 新建场景：直接使用客户端请求的校验和
      checksum = requestedChecksum;
    }

    final FileIoProvider fileIoProvider = getFileIoProvider();
    FileOutputStream blockOut = null;
    FileOutputStream crcOut = null;
    try {
      // 获取块文件和元数据文件的输出流
      blockOut = fileIoProvider.getFileOutputStream(
          getVolume(), new RandomAccessFile(blockFile, "rw").getFD());
      crcOut = fileIoProvider.getFileOutputStream(getVolume(), metaRAF.getFD());
      // 追加场景：移动文件指针到已写入位置，继续写入
      if (!isCreate) {
        blockOut.getChannel().position(blockDiskSize);
        crcOut.getChannel().position(crcDiskSize);
      }
      // 封装返回输出流对象
      return new ReplicaOutputStreams(blockOut, crcOut, checksum,
          getVolume(), fileIoProvider);
    } catch (IOException e) {
      // 异常情况下关闭所有已打开的流
      IOUtils.closeStream(blockOut);
      IOUtils.closeStream(crcOut);
      IOUtils.closeStream(metaRAF);
      throw e;
    }
  }

  @Override
  /**
   * 创建用于重启恢复的元数据输出流，保存恢复过程中的元数据
   * @return 重启元数据文件输出流
   * @throws IOException 创建流失败时抛出异常
   */
  public OutputStream createRestartMetaStream() throws IOException {
    File blockFile = getBlockFile();
    // 构造重启元数据文件名，存储在块所在目录
    File restartMeta = new File(blockFile.getParent()  +
        File.pathSeparator + "." + blockFile.getName() + ".restart");
    // 删除已存在的旧重启元数据文件
    if (!getFileIoProvider().deleteWithExistsCheck(getVolume(), restartMeta)) {
      DataNode.LOG.warn("Failed to delete restart meta file: " +
          restartMeta.getPath());
    }
    // 返回新重启元数据文件的输出流
    return getFileIoProvider().getFileOutputStream(getVolume(), restartMeta);
  }

  @Override
  public String toString() {
    return super.toString()
        + "\n  bytesAcked=" + bytesAcked
        + "\n  bytesOnDisk=" + bytesOnDisk;
  }

  @Override
  public ReplicaInfo getOriginalReplica() {
    throw new UnsupportedOperationException("Replica of type " + getState() +
        " does not