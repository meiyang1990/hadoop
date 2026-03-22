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

import java.io.DataInputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;

import org.apache.hadoop.util.Preconditions;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;

/**
 * 文件级注释：HDFS元数据编辑日志备份输入流，为备份节点提供从主节点接收增量元数据更新的能力
 * 
 * 该类继承自EditLogInputStream抽象类，专门用于BackupNode场景，接收主Namenode推送的
 * 编辑日志增量数据，用于同步更新备份节点的元数据状态。
 * 
 * @see org.apache.hadoop.hdfs.server.protocol.NamenodeProtocol#journal
 * (org.apache.hadoop.hdfs.server.protocol.NamenodeRegistration,
 *  int, int, byte[])
 */
class EditLogBackupInputStream extends EditLogInputStream {
  final String address; // 发送方地址（主Namenode地址）
  private final ByteBufferInputStream inner;
  private DataInputStream in;
  private FSEditLogOp.Reader reader = null;
  private FSEditLogLoader.PositionTrackingInputStream tracker = null;
  private int version = 0;

  /**
   * 内部类：支持动态修改底层字节数组的 ByteArrayInputStream 扩展实现
   * 允许重复复用输入流对象，动态替换需要读取的字节数据，避免重复创建对象
   */
  private static class ByteBufferInputStream extends ByteArrayInputStream {
    ByteBufferInputStream() {
      super(new byte[0]);
    }

    /**
     * 更新输入流要读取的字节数据，重置流读取位置
     * @param newBytes 新的字节数据数组
     */
    void setData(byte[] newBytes) {
      super.buf = newBytes;
      super.count = newBytes == null ? 0 : newBytes.length;
      super.mark = 0;
      reset();
    }

    /**
     * 获取当前字节数据的总长度
     * @return 字节数组总长度
     */
    int length() {
      return count;
    }
  }

  /**
   * 构造方法：初始化编辑日志备份输入流
   * @param name 发送方（主Namenode）地址
   * @throws IOException 初始化异常
   */
  EditLogBackupInputStream(String name) throws IOException {
    address = name;
    inner = new ByteBufferInputStream();
    in = null;
    reader = null;
  }

  @Override
  public String getName() {
    return address;
  }

  @Override
  protected FSEditLogOp nextOp() throws IOException {
    // 检查必须先调用setBytes设置数据才能读取操作
    Preconditions.checkState(reader != null,
        "Must call setBytes() before readOp()");
    return reader.readOp(false);
  }

  @Override
  protected FSEditLogOp nextValidOp() {
    try {
      return reader.readOp(true);
    } catch (IOException e) {
      throw new RuntimeException("got unexpected IOException " + e, e);
    }
  }

  @Override
  public int getVersion(boolean verifyVersion) throws IOException {
    return this.version;
  }

  @Override
  public long getPosition() {
    return tracker.getPos();
  }

  @Override
  public void close() throws IOException {
    in.close();
  }

  @Override
  public long length() throws IOException {
    // 返回当前存储的字节数据总长度
    return inner.length();
  }

  /**
   * 更新输入流中的字节数据和日志版本，重新初始化编辑日志读取器
   * @param newBytes 新接收到的编辑日志字节数据
   * @param version 编辑日志版本号
   * @throws IOException 初始化读取器异常
   */
  void setBytes(byte[] newBytes, int version) throws IOException {
    inner.setData(newBytes);
    tracker = new FSEditLogLoader.PositionTrackingInputStream(inner);
    in = new DataInputStream(tracker);

    this.version = version;

    reader = FSEditLogOp.Reader.create(in, tracker, version);
  }

  /**
   * 清空输入流中的数据，重置状态
   * @throws IOException 清空过程异常
   */
  void clear() throws IOException {
    setBytes(null, 0);
    reader = null;
    this.version = 0;
  }

  @Override
  public long getFirstTxId() {
    return HdfsServerConstants.INVALID_TXID;
  }

  @Override
  public long getLastTxId() {
    return HdfsServerConstants.INVALID_TXID;
  }

  @Override
  public boolean isInProgress() {
    // 备份流始终处于接收增量更新的进行中状态
    return true;
  }

  @Override
  public void setMaxOpSize(int maxOpSize) {
    reader.setMaxOpSize(maxOpSize);
  }

  @Override
  public boolean isLocalLog() {
    // 备份日志属于本地可读取日志
    return true;
  }
}