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

package org.apache.hadoop.hdfs.server.namenode;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.LayoutFlags;
import org.apache.hadoop.io.IOUtils;

import org.apache.hadoop.classification.VisibleForTesting;

/**
 * 文件级编辑日志输出流实现，将NameNode操作日志写入本地磁盘文件。
 * 继承自抽象类{@link EditLogOutputStream}，负责HDFS编辑日志的本地文件持久化。
 */
@InterfaceAudience.Private
public class EditLogFileOutputStream extends EditLogOutputStream {
  private static final Logger LOG =
      LoggerFactory.getLogger(EditLogFileOutputStream.class);
  // 最小预分配长度，单位字节
  public static final int MIN_PREALLOCATION_LENGTH = 1024 * 1024;

  // 当前输出对应的编辑日志文件
  private File file;
  // 文件输出流，用于写入编辑日志
  private FileOutputStream fp;
  // 文件通道，用于同步数据到磁盘
  private FileChannel fc;
  // 双缓冲，用于实现写入与刷盘并行
  private EditsDoubleBuffer doubleBuf;
  // 预分配填充缓冲区，预分配空间时填充固定值
  static final ByteBuffer fill = ByteBuffer.allocateDirect(MIN_PREALLOCATION_LENGTH);
  // 是否使用同步写入模式并跳过fsync
  private boolean shouldSyncWritesAndSkipFsync = false;

  // 测试用标记：是否跳过fsync调用加速测试
  private static boolean shouldSkipFsyncForTests = false;

  static {
    // 初始化预分配缓冲区，全部填充无效操作码
    fill.position(0);
    for (int i = 0; i < fill.capacity(); i++) {
      fill.put(FSEditLogOpCodes.OP_INVALID.getOpCode());
    }
  }

  /**
   * 构造编辑日志文件输出流，初始化缓冲和文件对象。
   * 
   * @param conf Hadoop配置对象
   * @param name 编辑日志存储文件路径
   * @param size 刷写缓冲区大小
   * @throws IOException 初始化文件失败时抛出IO异常
   */
  public EditLogFileOutputStream(Configuration conf, File name, int size)
      throws IOException {
    super();
    shouldSyncWritesAndSkipFsync = conf.getBoolean(
            DFSConfigKeys.DFS_NAMENODE_EDITS_NOEDITLOGCHANNELFLUSH,
            DFSConfigKeys.DFS_NAMENODE_EDITS_NOEDITLOGCHANNELFLUSH_DEFAULT);

    file = name;
    doubleBuf = new EditsDoubleBuffer(size);
    RandomAccessFile rp;
    // 根据配置选择文件打开模式：同步写入/普通读写
    if (shouldSyncWritesAndSkipFsync) {
      rp = new RandomAccessFile(name, "rwd");
    } else {
      rp = new RandomAccessFile(name, "rw");
    }
    try {
      fp = new FileOutputStream(rp.getFD()); // 以追加模式打开
    } catch (IOException e) {
      IOUtils.closeStream(rp);
      throw e;
    }
    fc = rp.getChannel();
    // 将写入位置移动到文件末尾，支持追加写入
    fc.position(fc.size());
  }

  @Override
  public void write(FSEditLogOp op) throws IOException {
    doubleBuf.writeOp(op, getCurrentLogVersion());
  }

  /**
   * 将序列化好的事务原始字节写入流中。
   * 事务格式为：操作码(1字节) + 事务ID(long) + 事务Writable序列化数据
   * */
  @Override
  public void writeRaw(byte[] bytes, int offset, int length) throws IOException {
    doubleBuf.writeRaw(bytes, offset, length);
  }

  /**
   * 创建空的编辑日志文件，写入文件头。
   */
  @Override
  public void create(int layoutVersion) throws IOException {
    // 截断文件到0长度，清空原有内容
    fc.truncate(0);
    fc.position(0);
    // 写入版本头信息
    writeHeader(layoutVersion, doubleBuf.getCurrentBuf());
    setReadyToFlush();
    // 将头信息刷入磁盘
    flush();
    setCurrentLogVersion(layoutVersion);
  }

  /**
   * 将编辑日志文件头写入指定输出流。
   * 
   * @param layoutVersion 编辑日志的布局版本号
   * @param out 目标输出流
   * @throws IOException 写入头信息失败时抛出IO异常
   */
  @VisibleForTesting
  public static void writeHeader(int layoutVersion, DataOutputStream out)
      throws IOException {
    out.writeInt(layoutVersion);
    LayoutFlags.write(out);
  }

  @Override
  public void close() throws IOException {
    if (fp == null) {
      throw new IOException("Trying to use aborted output stream");
    }

    try {
      // close should have been called after all pending transactions
      // have been flushed & synced.
      // if already closed, just skip
      if (doubleBuf != null) {
        doubleBuf.close();
        doubleBuf = null;
      }
      
      // 截断文件，移除预分配的空白填充字节
      if (fc != null && fc.isOpen()) {
        fc.truncate(fc.position());
        fc.close();
        fc = null;
      }
      fp.close();
      fp = null;
    } finally {
      // 最终清理资源，避免资源泄漏
      IOUtils.cleanupWithLogger(LOG, fc, fp);
      doubleBuf = null;
      fc = null;
      fp = null;
    }
    fp = null;
  }
  
  @Override
  public void abort() throws IOException {
    if (fp == null) {
      return;
    }
    // 异常中止时直接清理关闭输出流
    IOUtils.cleanupWithLogger(LOG, fp);
    fp = null;
  }

  /**
   * 将当前缓冲区标记为可刷盘，允许写入线程继续写入新数据，后台执行刷盘。
   */
  @Override
  public void setReadyToFlush() throws IOException {
    doubleBuf.setReadyToFlush();
  }

  /**
   * 将已标记就绪的缓冲区数据刷入持久化存储，当前缓冲区继续接收新数据。
   */
  @Override
  public void flushAndSync(boolean durable) throws IOException {
    if (fp == null) {
      throw new IOException("Trying to use aborted output stream");
    }
    if (doubleBuf.isFlushed()) {
      LOG.info("Nothing to flush");
      return;
    }
    // 根据需要提前预分配文件空间，减少写入时的分配开销
    preallocate();
    // 将就绪缓冲区数据写入文件流
    doubleBuf.flushTo(fp);
    // 如果需要持久化且未跳过fsync，则强制同步到磁盘
    if (durable && !shouldSkipFsyncForTests && !shouldSyncWritesAndSkipFsync) {
      fc.force(false); // 不需要同步元数据变更
    }
  }

  @Override
  public boolean shouldForceSync() {
    return doubleBuf.shouldForceSync();
  }

  /**
   * 预分配编辑日志文件空间，提升写入性能，避免分配碎片化。
   * @throws IOException 预分配失败时抛出IO异常
   */
  private void preallocate() throws IOException {
    long position = fc.position();
    long size = fc.size();
    int bufSize = doubleBuf.getReadyBuf().getLength();
    long need = bufSize - (size - position);
    if (need <= 0) {
      // 已有空间足够，无需预分配
      return;
    }
    long oldSize = size;
    long total = 0;
    long fillCapacity = fill.capacity();
    // 循环预分配，每次分配最小预分配块大小，直到满足需求
    while (need > 0) {
      fill.position(0);
      IOUtils.writeFully(fc, fill, size);
      need -= fillCapacity;
      size += fillCapacity;
      total += fillCapacity;
    }
    if(LOG.isDebugEnabled()) {
      LOG.debug("Preallocated " + total + " bytes at the end of " +
      		"the edit log (offset " + oldSize + ")");
    }
  }

  /**
   * 获取当前输出流对应的日志文件对象。
   */
  File getFile() {
    return file;
  }
  
  @Override
  public String toString() {
    return "EditLogFileOutputStream(" + file + ")";
  }

  /**
   * 判断当前流是否处于打开状态。
   * @return true表示已打开，false表示已关闭/中止
   */
  public boolean isOpen() {
    return fp != null;
  }
  
  @VisibleForTesting
  public void setFileChannelForTesting(FileChannel fc) {
    this.fc = fc;
  }
  
  @VisibleForTesting
  public FileChannel getFileChannelForTesting() {
    return fc;
  }
  
  /**
   * 单元测试专用设置：跳过实际fsync调用以提升测试执行速度。
   * @param skip true表示不调用fsync
   */
  @VisibleForTesting
  public static void setShouldSkipFsyncForTesting(boolean skip) {
    shouldSkipFsyncForTests = skip;
  }
}