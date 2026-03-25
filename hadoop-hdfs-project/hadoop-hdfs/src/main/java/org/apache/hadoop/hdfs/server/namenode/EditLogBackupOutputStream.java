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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.NameNodeProxies;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.protocol.JournalInfo;
import org.apache.hadoop.hdfs.server.protocol.JournalProtocol;
import org.apache.hadoop.hdfs.server.protocol.NamenodeRegistration;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * 编辑日志备份输出流，将主NameNode的编辑日志流式传输到备份节点
 * 继承自EditLogOutputStream抽象类，实现将 edits 输出到备份节点的逻辑
 * 
 * @see org.apache.hadoop.hdfs.server.protocol.NamenodeProtocol#journal
 * (org.apache.hadoop.hdfs.server.protocol.NamenodeRegistration,
 *  int, int, byte[])
 */
class EditLogBackupOutputStream extends EditLogOutputStream {
  private static final Logger LOG =
      LoggerFactory.getLogger(EditLogFileOutputStream.class);
  static final int DEFAULT_BUFFER_SIZE = 256;

  // 备份节点的RPC代理对象，用于调用备份节点的日志同步接口
  private final JournalProtocol backupNode;
  // 备份节点的注册信息，包含地址等标识信息
  private final NamenodeRegistration bnRegistration;
  // 活动NameNode的日志信息
  private final JournalInfo journalInfo;
  // 序列化输出缓冲区，用于将编辑日志序列化后发送给备份节点
  private final DataOutputBuffer out;
  // 双缓冲实现，用于编辑日志的读写缓冲，提升写入性能
  private EditsDoubleBuffer doubleBuf;

  /**
   * 构造编辑日志备份输出流，建立到备份节点的RPC连接并初始化缓冲区
   * 
   * @param bnReg 备份节点的注册信息
   * @param journalInfo 活动NameNode的日志信息
   * @param logVersion 编辑日志版本号
   * @throws IOException 创建RPC连接失败时抛出异常
   */
  EditLogBackupOutputStream(NamenodeRegistration bnReg, // backup node
      JournalInfo journalInfo, int logVersion) // active name-node
      throws IOException {
    super();
    this.bnRegistration = bnReg;
    this.journalInfo = journalInfo;
    // 从注册信息解析出备份节点的网络地址
    InetSocketAddress bnAddress =
      NetUtils.createSocketAddr(bnRegistration.getAddress());
    try {
      // 创建非HA模式下的备份节点RPC代理
      this.backupNode = NameNodeProxies.createNonHAProxy(new HdfsConfiguration(),
          bnAddress, JournalProtocol.class, UserGroupInformation.getCurrentUser(),
          true).getProxy();
    } catch(IOException e) {
      Storage.LOG.error("Error connecting to: " + bnAddress, e);
      throw e;
    }
    // 初始化双缓冲和输出缓冲区
    this.doubleBuf = new EditsDoubleBuffer(DEFAULT_BUFFER_SIZE);
    this.out = new DataOutputBuffer(DEFAULT_BUFFER_SIZE);
    setCurrentLogVersion(logVersion);
  }
  
  @Override // EditLogOutputStream
  public void write(FSEditLogOp op) throws IOException {
    doubleBuf.writeOp(op, getCurrentLogVersion());
 }

  @Override
  public void writeRaw(byte[] bytes, int offset, int length) throws IOException {
    throw new IOException("Not supported");
  }

  /**
   * 创建新的日志段，备份节点无持久化存储，仅清空缓冲区重置状态
   */
  @Override // EditLogOutputStream
  public void create(int layoutVersion) throws IOException {
    assert doubleBuf.isFlushed() : "previous data is not flushed yet";
    this.doubleBuf = new EditsDoubleBuffer(DEFAULT_BUFFER_SIZE);
    setCurrentLogVersion(layoutVersion);
  }

  @Override // EditLogOutputStream
  public void close() throws IOException {
    // close should have been called after all pending transactions 
    // have been flushed & synced.
    int size = doubleBuf.countBufferedBytes();
    if (size != 0) {
      throw new IOException("BackupEditStream has " + size +
                          " records still to be flushed and cannot be closed.");
    } 
    // 停止RPC代理，释放相关线程资源
    RPC.stopProxy(backupNode); // stop the RPC threads
    doubleBuf.close();
    doubleBuf = null;
  }

  @Override
  public void abort() throws IOException {
    RPC.stopProxy(backupNode);
    doubleBuf = null;
  }

  @Override // EditLogOutputStream
  public void setReadyToFlush() throws IOException {
    doubleBuf.setReadyToFlush();
  }

  @Override // EditLogOutputStream
  protected void flushAndSync(boolean durable) throws IOException {
    assert out.getLength() == 0 : "Output buffer is not empty";
    
    // 无数据需要刷新直接返回
    if (doubleBuf.isFlushed()) {
      LOG.info("Nothing to flush");
      return;
    }

    // 获取待刷新的事务数量和第一个事务ID
    int numReadyTxns = doubleBuf.countReadyTxns();
    long firstTxToFlush = doubleBuf.getFirstReadyTxId();
    
    // 将双缓冲中的数据刷新到输出缓冲区
    doubleBuf.flushTo(out);
    if (out.getLength() > 0) {
      assert numReadyTxns > 0;
      
      // 复制数据并重置输出缓冲区
      byte[] data = Arrays.copyOf(out.getData(), out.getLength());
      out.reset();
      assert out.getLength() == 0 : "Output buffer is not empty";

      // RPC调用备份节点的journal接口同步编辑日志
      backupNode.journal(journalInfo, 0, firstTxToFlush, numReadyTxns, data);
    }
  }

  /**
   * 获取备份节点的注册信息
   * 
   * @return 备份节点的注册对象
   */
  NamenodeRegistration getRegistration() {
    return bnRegistration;
  }

  /**
   * 通知备份节点启动新的日志段
   * 
   * @param txId 新日志段的起始事务ID
   * @throws IOException RPC调用失败抛出异常
   */
  void startLogSegment(long txId) throws IOException {
    backupNode.startLogSegment(journalInfo, 0, txId);
  }
}