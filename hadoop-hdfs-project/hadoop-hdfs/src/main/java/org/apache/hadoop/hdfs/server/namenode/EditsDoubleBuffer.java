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

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;

import org.apache.commons.codec.binary.Hex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.Writer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.IOUtils;

import org.apache.hadoop.util.Preconditions;

/**
 * HDFS元数据操作日志（edits）双缓冲实现类。
 * 一个缓冲区用于持续接收新写入的编辑日志，另一个缓冲区用于将已有日志刷写到磁盘，刷写完成后交换两个缓冲区角色。
 * 这种设计允许写入和刷盘并发进行，无需每次刷盘都重新分配缓冲区，提升编辑日志写入性能。
 */
@InterfaceAudience.Private
public class EditsDoubleBuffer {
  protected static final Logger LOG =
      LoggerFactory.getLogger(EditsDoubleBuffer.class);

  private TxnBuffer bufCurrent; // 当前用于写入新日志的缓冲区
  private TxnBuffer bufReady; // 已写满、等待刷盘的缓冲区
  private final int initBufferSize; // 缓冲区初始大小

  /**
   * 构造双缓冲区，初始化两个空缓冲区
   * @param defaultBufferSize 缓冲区初始大小
   */
  public EditsDoubleBuffer(int defaultBufferSize) {
    initBufferSize = defaultBufferSize;
    bufCurrent = new TxnBuffer(initBufferSize);
    bufReady = new TxnBuffer(initBufferSize);

  }

  /**
   * 将一条编辑日志操作写入当前缓冲区
   * @param op 编辑日志操作对象
   * @param logVersion 日志版本号
   * @throws IOException 写入失败时抛出异常
   */
  public void writeOp(FSEditLogOp op, int logVersion) throws IOException {
    bufCurrent.writeOp(op, logVersion);
  }

  /**
   * 将原始字节写入当前缓冲区
   * @param bytes 原始字节数组
   * @param offset 起始偏移量
   * @param length 写入长度
   * @throws IOException 写入失败时抛出异常
   */
  public void writeRaw(byte[] bytes, int offset, int length) throws IOException {
    bufCurrent.write(bytes, offset, length);
  }
  
  /**
   * 关闭双缓冲区，检查是否有未刷写数据，清理资源
   * @throws IOException 存在未刷写数据或清理失败时抛出异常
   */
  public void close() throws IOException {
    Preconditions.checkNotNull(bufCurrent);
    Preconditions.checkNotNull(bufReady);

    int bufSize = bufCurrent.size();
    if (bufSize != 0) {
      bufCurrent.dumpRemainingEditLogs();
      throw new IOException("FSEditStream has " + bufSize
          + " bytes still to be flushed and cannot be closed.");
    }

    IOUtils.cleanupWithLogger(null, bufCurrent, bufReady);
    bufCurrent = bufReady = null;
  }
  
  /**
   * 将当前写缓冲区交换为待刷写缓冲区，交换后新的写入缓冲区为空，原有数据进入待刷写状态
   */
  public void setReadyToFlush() {
    assert isFlushed() : "previous data not flushed yet";
    TxnBuffer tmp = bufReady;
    bufReady = bufCurrent;
    bufCurrent = tmp;
  }
  
  /**
   * 将待刷写缓冲区的所有数据写到输出流，刷写完成后重置待刷写缓冲区，不交换缓冲区角色
   * @param out 目标输出流
   * @throws IOException 写入输出流失败时抛出异常
   */
  public void flushTo(OutputStream out) throws IOException {
    bufReady.writeTo(out); // 将数据写入文件
    bufReady.reset(); // 清空缓冲区数据
  }
  
  /**
   * 检查当前写缓冲区是否达到初始容量，需要强制刷盘同步
   * @return true表示需要强制同步，false表示不需要
   */
  public boolean shouldForceSync() {
    return bufCurrent.size() >= initBufferSize;
  }

  /**
   * 获取待刷写缓冲区
   * @return 待刷写缓冲区对象
   */
  DataOutputBuffer getReadyBuf() {
    return bufReady;
  }
  
  /**
   * 获取当前写缓冲区
   * @return 当前写缓冲区对象
   */
  DataOutputBuffer getCurrentBuf() {
    return bufCurrent;
  }

  /**
   * 检查待刷写缓冲区是否已经完成刷写
   * @return true表示已刷写完成，false表示还有待刷写数据
   */
  public boolean isFlushed() {
    return bufReady.size() == 0;
  }

  /**
   * 计算两个缓冲区总共缓存的字节数
   * @return 总缓存字节数
   */
  public int countBufferedBytes() {
    return bufReady.size() + bufCurrent.size();
  }

  /**
   * 获取待刷写缓冲区中第一个事务的事务ID
   * @return 第一个待刷写事务ID
   */
  public long getFirstReadyTxId() {
    assert bufReady.firstTxId > 0;
    return bufReady.firstTxId;
  }

  /**
   * 获取待刷写缓冲区中的事务总数
   * @return 待刷写事务数量
   */
  public int countReadyTxns() {
    return bufReady.numTxns;
  }

  /**
   * 获取待刷写缓冲区中的字节总数
   * @return 待刷写字节数
   */
  public int countReadyBytes() {
    return bufReady.size();
  }
  
  /**
   * 事务缓存内部类，继承DataOutputBuffer，额外存储事务ID计数等元信息
   */
  private static class TxnBuffer extends DataOutputBuffer {
    long firstTxId; // 缓冲区中第一个事务ID
    int numTxns; // 缓冲区中事务总数
    private final Writer writer; // 编辑日志写入器
    
    /**
     * 构造事务缓冲区
     * @param initBufferSize 初始缓冲区大小
     */
    public TxnBuffer(int initBufferSize) {
      super(initBufferSize);
      writer = new FSEditLogOp.Writer(this);
      reset();
    }

    /**
     * 将一条编辑日志操作写入缓冲区，更新事务元信息
     * @param op 编辑日志操作对象
     * @param logVersion 日志版本号
     * @throws IOException 写入失败时抛出异常
     */
    public void writeOp(FSEditLogOp op, int logVersion) throws IOException {
      if (firstTxId == HdfsServerConstants.INVALID_TXID) {
        firstTxId = op.txid;
      } else {
        assert op.txid > firstTxId;
      }
      writer.writeOp(op, logVersion);
      numTxns++;
    }
    
    @Override
    public DataOutputBuffer reset() {
      super.reset();
      firstTxId = HdfsServerConstants.INVALID_TXID;
      numTxns = 0;
      return this;
    }

    /**
     * 打印缓冲区中未刷写的编辑日志详情到日志，用于异常时调试
     */
    private void dumpRemainingEditLogs() {
      byte[] buf = this.getData();
      byte[] remainingRawEdits = Arrays.copyOfRange(buf, 0, this.size());
      ByteArrayInputStream bis = new ByteArrayInputStream(remainingRawEdits);
      DataInputStream dis = new DataInputStream(bis);
      FSEditLogLoader.PositionTrackingInputStream tracker =
          new FSEditLogLoader.PositionTrackingInputStream(bis);
      FSEditLogOp.Reader reader = FSEditLogOp.Reader.create(dis, tracker,
          NameNodeLayoutVersion.CURRENT_LAYOUT_VERSION);
      FSEditLogOp op;
      LOG.warn("The edits buffer is " + size() + " bytes long with " + numTxns +
          " unflushed transactions. " +
          "Below is the list of unflushed transactions:");
      int numTransactions = 0;
      try {
        while ((op = reader.readOp(false)) != null) {
          LOG.warn("Unflushed op [" + numTransactions + "]: " + op);
          numTransactions++;
        }
      } catch (IOException ioe) {
        // 解析失败打印原始字节后停止
        LOG.warn("Unable to dump remaining ops. Remaining raw bytes: " +
            Hex.encodeHexString(remainingRawEdits), ioe);
      }
    }
  }

}