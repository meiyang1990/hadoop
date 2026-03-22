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
package org.apache.hadoop.hdfs.qjournal.client;

import java.io.IOException;

import org.apache.hadoop.hdfs.server.namenode.EditLogOutputStream;
import org.apache.hadoop.hdfs.server.namenode.EditsDoubleBuffer;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp;
import org.apache.hadoop.io.DataOutputBuffer;

/**
 * 写入远程仲裁日志节点集群的编辑日志输出流实现
 * 用于HDFS QJM（Quorum Journal Manager）高可用方案中，向多数节点日志集群写入编辑日志
 */
class QuorumOutputStream extends EditLogOutputStream {
  private final AsyncLoggerSet loggers;
  private EditsDoubleBuffer buf;
  private final long segmentTxId;
  private final int writeTimeoutMs;

  /**
   * 构造仲裁日志输出流
   * @param loggers 异步日志节点集合
   * @param txId 当前日志段起始事务ID
   * @param outputBufferCapacity 输出缓冲区容量
   * @param writeTimeoutMs 写操作超时时间（毫秒）
   * @param logVersion 日志版本号
   * @throws IOException 如果构造失败抛出IO异常
   */
  public QuorumOutputStream(AsyncLoggerSet loggers,
      long txId, int outputBufferCapacity,
      int writeTimeoutMs, int logVersion) throws IOException {
    super();
    this.buf = new EditsDoubleBuffer(outputBufferCapacity);
    this.loggers = loggers;
    this.segmentTxId = txId;
    this.writeTimeoutMs = writeTimeoutMs;
    setCurrentLogVersion(logVersion);
  }

  @Override
  public void write(FSEditLogOp op) throws IOException {
    buf.writeOp(op, getCurrentLogVersion());
  }

  @Override
  public void writeRaw(byte[] bytes, int offset, int length) throws IOException {
    buf.writeRaw(bytes, offset, length);
  }

  @Override
  public void create(int layoutVersion) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void close() throws IOException {
    if (buf != null) {
      buf.close();
      buf = null;
    }
  }

  @Override
  public void abort() throws IOException {
    QuorumJournalManager.LOG.warn("Aborting " + this);
    buf = null;
    close();
  }

  @Override
  public void setReadyToFlush() throws IOException {
    buf.setReadyToFlush();
  }

  @Override
  public boolean shouldForceSync() {
    return buf.shouldForceSync();
  }

  @Override
  protected void flushAndSync(boolean durable) throws IOException {
    int numReadyBytes = buf.countReadyBytes();
    if (numReadyBytes > 0) {
      int numReadyTxns = buf.countReadyTxns();
      long firstTxToFlush = buf.getFirstReadyTxId();

      assert numReadyTxns > 0;

      // 从双缓冲拷贝数据到新字节数组，原因：
      // 1) IPC无法直接发送大数组的切片部分
      // 2) 底层调用是异步的，需要保护性拷贝避免缓冲区在发送前被修改
      DataOutputBuffer bufToSend = new DataOutputBuffer(numReadyBytes);
      buf.flushTo(bufToSend);
      assert bufToSend.getLength() == numReadyBytes;
      byte[] data = bufToSend.getData();
      assert data.length == bufToSend.getLength();

      // 向所有日志节点异步发送编辑日志数据
      QuorumCall<AsyncLogger, Void> qcall = loggers.sendEdits(
          segmentTxId, firstTxToFlush,
          numReadyTxns, data);
      // 等待多数节点写入成功，完成仲裁确认
      loggers.waitForWriteQuorum(qcall, writeTimeoutMs, "sendEdits");
      
      // 写入成功后更新已提交事务ID，落后的节点可通过后续RPC获取最新事务进度
      loggers.setCommittedTxId(firstTxToFlush + numReadyTxns - 1);
    }
  }

  /**
   * 生成当前输出流状态报告，用于诊断
   * @return 状态报告字符串
   */
  @Override
  public String generateReport() {
    StringBuilder sb = new StringBuilder();
    sb.append("Writing segment beginning at txid " + segmentTxId + ". \n");
    loggers.appendReport(sb);
    return sb.toString();
  }
  
  @Override
  public String toString() {
    return "QuorumOutputStream starting at txid " + segmentTxId;
  }
}