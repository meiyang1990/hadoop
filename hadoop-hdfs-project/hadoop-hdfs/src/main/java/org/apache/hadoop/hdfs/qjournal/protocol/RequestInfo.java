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
package org.apache.hadoop.hdfs.qjournal.protocol;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;

/**
 * QJournal编辑日志请求信息封装类，用于在NameNode和JournalNode之间传递请求上下文信息
 * 保存请求关联的日志节点ID、周期号、序列号和已提交事务ID等核心上下文
 */
@InterfaceAudience.Private
public class RequestInfo {
  private final String jid;
  private long epoch;
  private long ipcSerialNumber;
  private final long committedTxId;
  private final String nameServiceId;
  
  /**
   * 构造QJournal请求信息对象
   * @param jid 日志节点ID
   * @param nameServiceId 名称服务ID，联邦HDFS场景下用于标识不同命名空间
   * @param epoch 当前Epoch周期号，用于角色仲裁
   * @param ipcSerialNumber RPC请求序列号，用于保证请求有序性
   * @param committedTxId 已提交的最大事务ID
   */
  public RequestInfo(String jid, String nameServiceId,
                     long epoch, long ipcSerialNumber,
                     long committedTxId) {
    this.jid = jid;
    this.nameServiceId = nameServiceId;
    this.epoch = epoch;
    this.ipcSerialNumber = ipcSerialNumber;
    this.committedTxId = committedTxId;
  }

  /**
   * 获取名称服务ID
   * @return 当前请求所属的名称服务ID
   */
  public String getNameServiceId() {
    return nameServiceId;
  }

  /**
   * 获取当前Epoch周期号
   * @return Epoch周期号，越大表示越新的主角色
   */
  public long getEpoch() {
    return epoch;
  }

  /**
   * 设置Epoch周期号
   * @param epoch 新的Epoch周期号
   */
  public void setEpoch(long epoch) {
    this.epoch = epoch;
  }
  
  /**
   * 获取日志节点ID
   * @return 当前请求关联的日志节点ID
   */
  public String getJournalId() {
    return jid;
  }

  /**
   * 获取IPC请求序列号
   * @return 当前请求的唯一序列号，用于保证请求有序
   */
  public long getIpcSerialNumber() {
    return ipcSerialNumber;
  }

  /**
   * 设置IPC请求序列号
   * @param ipcSerialNumber 新的请求序列号
   */
  public void setIpcSerialNumber(long ipcSerialNumber) {
    this.ipcSerialNumber = ipcSerialNumber;
  }

  /**
   * 获取已提交的最大事务ID
   * @return 已提交事务ID
   */
  public long getCommittedTxId() {
    return committedTxId;
  }

  /**
   * 检查是否存在有效的已提交事务ID
   * @return true表示存在有效已提交事务ID，false表示无有效事务ID
   */
  public boolean hasCommittedTxId() {
    return (committedTxId != HdfsServerConstants.INVALID_TXID);
  }
}