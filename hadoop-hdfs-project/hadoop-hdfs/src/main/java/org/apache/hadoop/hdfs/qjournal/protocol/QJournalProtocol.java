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

import java.io.IOException;
import java.net.URL;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.qjournal.client.QuorumJournalManager;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.GetEditLogManifestResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.GetJournaledEditsResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.GetJournalStateResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.NewEpochResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.PrepareRecoveryResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.SegmentStateProto;
import org.apache.hadoop.hdfs.qjournal.server.JournalNode;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.namenode.JournalManager;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.io.retry.Idempotent;
import org.apache.hadoop.security.KerberosInfo;

/**
 * QJournal协议接口，用于{@link QuorumJournalManager}（NameNode侧日志管理器）
 * 和各个{@link JournalNode}（日志节点）之间的RPC通信。
 * 
 * 负责编辑日志的写入传输，以及日志节点之间的恢复协调，是HDFS QJM共享存储HA方案的核心通信协议。
 */
@KerberosInfo(
    serverPrincipal = DFSConfigKeys.DFS_JOURNALNODE_KERBEROS_PRINCIPAL_KEY,
    clientPrincipal = DFSConfigKeys.DFS_NAMENODE_KERBEROS_PRINCIPAL_KEY)
@InterfaceAudience.Private
public interface QJournalProtocol {
  /** 协议版本ID */
  public static final long versionID = 1L;

  /**
   * 检查指定日志是否已完成格式化并包含有效数据。
   * @param journalId 日志ID
   * @param nameServiceId 命名服务ID
   * @return 如果已格式化返回true，否则返回false
   * @throws IOException 通信或IO异常
   */
  boolean isFormatted(String journalId,
                      String nameServiceId) throws IOException;

  /**
   * 获取日志节点当前的日志状态，包含最新的epoch编号和HTTP端口信息。
   * @param journalId 日志ID
   * @param nameServiceId 命名服务ID
   * @return 日志状态响应对象
   * @throws IOException 通信或IO异常
   */
  GetJournalStateResponseProto getJournalState(String journalId,
                                               String nameServiceId)
      throws IOException;
  
  /**
   * 对指定命名空间的底层存储进行格式化。
   * @param journalId 日志ID
   * @param nameServiceId 命名服务ID
   * @param nsInfo 命名空间信息
   * @param force 是否强制格式化
   * @throws IOException 通信或IO异常
   */
  void format(String journalId, String nameServiceId,
      NamespaceInfo nsInfo, boolean force) throws IOException;

  /**
   * 开启一个新的epoch，用于选举激活后的版本号隔离，详见HDFS-3077设计文档。
   * @param journalId 日志ID
   * @param nameServiceId 命名服务ID
   * @param nsInfo 命名空间信息
   * @param epoch 新的epoch编号
   * @return 新纪元响应对象
   * @throws IOException 通信或IO异常
   */
  NewEpochResponseProto newEpoch(String journalId,
                                        String nameServiceId,
                                        NamespaceInfo nsInfo,
                                        long epoch) throws IOException;
  
  /**
   * 写入编辑日志记录，由激活的NameNode发送给JournalNode，将编辑日志写入本地磁盘。
   * @param reqInfo 请求信息（包含epoch等身份校验信息）
   * @param segmentTxId 日志分段起始事务ID
   * @param firstTxnId 本次写入第一个事务ID
   * @param numTxns 本次写入事务数量
   * @param records 序列化后的编辑记录字节数组
   * @throws IOException 通信或IO异常
   */
  public void journal(RequestInfo reqInfo,
                      long segmentTxId,
                      long firstTxnId,
                      int numTxns,
                      byte[] records) throws IOException;

  
  /**
   * 心跳检查，服务端默认无操作，仅用于校验调用方仍然是激活的写入者，同时返回最新已提交事务ID。
   * @param reqInfo 请求信息
   * @throws IOException 通信或IO异常
   */
  public void heartbeat(RequestInfo reqInfo) throws IOException;
  
  /**
   * 在JournalNode上开始写入一个新的编辑日志分段。调用此方法前需要先通过
   * {@link #finalizeLogSegment(RequestInfo, long, long)}完成上一个分段。
   * @param reqInfo 请求信息
   * @param txid 新分段第一个事务ID
   * @param layoutVersion 新日志的布局版本号
   * @throws IOException 通信或IO异常
   */
  public void startLogSegment(RequestInfo reqInfo,
      long txid, int layoutVersion) throws IOException;

  /**
   * 在JournalNode上完成指定日志分段，该分段必须处于正在写入状态，且从指定startTxId开始。
   * @param reqInfo 请求信息
   * @param startTxId 分段起始事务ID
   * @param endTxId 分段最后一个事务ID
   * @throws IOException 如果对应分段不存在则抛出异常
   */
  public void finalizeLogSegment(RequestInfo reqInfo,
      long startTxId, long endTxId) throws IOException;

  /**
   * 清理早于指定事务ID的旧日志，对应{@link JournalManager#purgeLogsOlderThan(long)}。
   * @param requestInfo 请求信息
   * @param minTxIdToKeep 需要保留的最小事务ID
   * @throws IOException 通信或IO异常
   */
  public void purgeLogsOlderThan(RequestInfo requestInfo, long minTxIdToKeep)
      throws IOException;
  
  /**
   * 获取自指定事务ID之后的所有编辑日志分段清单。
   * @param jid 日志ID
   * @param nameServiceId 命名服务ID
   * @param sinceTxId 客户端关注的第一个事务ID
   * @param inProgressOk 是否返回正在写入中的未完成分段
   * @return 编辑日志清单响应对象
   * @throws IOException 通信或IO异常
   */
  GetEditLogManifestResponseProto getEditLogManifest(String jid,
                                                     String nameServiceId,
                                                     long sinceTxId,
                                                     boolean inProgressOk)
      throws IOException;

  /**
   * 从JournalNode的内存编辑缓存中拉取编辑日志，缓存由{@link org.apache.hadoop.hdfs.qjournal.server.JournaledEditsCache}实现。
   * 需要通过{@value DFSConfigKeys#DFS_HA_TAILEDITS_INPROGRESS_KEY}配置开启渐进式尾日志才能启用该缓存。
   *
   * @param jid 日志ID
   * @param nameServiceId 命名空间ID
   * @param sinceTxId 从该事务ID开始拉取编辑
   * @param maxTxns 本次最多返回的事务数量
   * @throws IOException 如果拉取失败（包括缓存未命中请求的事务），则抛出异常，调用方需要回退到通过getEditLogManifest的流式拉取机制
   * @return 包含序列化编辑日志的响应对象
   * @see org.apache.hadoop.hdfs.qjournal.server.JournaledEditsCache
   */
  GetJournaledEditsResponseProto getJournaledEdits(String jid,
      String nameServiceId, long sinceTxId, int maxTxns) throws IOException;

  /**
   * 开始指定分段的恢复流程，详见HDFS-3077设计文档。
   * @param reqInfo 请求信息
   * @param segmentTxId 需要恢复的分段起始事务ID
   * @return 恢复准备响应对象
   * @throws IOException 通信或IO异常
   */
  public PrepareRecoveryResponseProto prepareRecovery(RequestInfo reqInfo,
      long segmentTxId) throws IOException;

  /**
   * 确认接受指定事务ID的恢复提议，完成分段恢复。
   * @param reqInfo 请求信息
   * @param stateToAccept 需要接受的分段状态
   * @param fromUrl 提议来源URL
   * @throws IOException 通信或IO异常
   */
  public void acceptRecovery(RequestInfo reqInfo,
      SegmentStateProto stateToAccept, URL fromUrl) throws IOException;

  /**
   * 执行升级前的准备操作。
   * @param journalId 日志ID
   * @throws IOException 通信或IO异常
   */
  void doPreUpgrade(String journalId) throws IOException;

  /**
   * 执行存储升级操作。
   * @param journalId 日志ID
   * @param sInfo 存储信息
   * @throws IOException 通信或IO异常
   */
  public void doUpgrade(String journalId, StorageInfo sInfo) throws IOException;

  /**
   * 执行升级完成后的最终化操作。
   * @param journalId 日志ID
   * @param nameServiceid 命名服务ID
   * @throws IOException 通信或IO异常
   */
  void doFinalize(String journalId,
                         String nameServiceid) throws IOException;

  /**
   * 检查是否可以回滚到目标布局版本。
   * @param journalId 日志ID
   * @param nameServiceid 命名服务ID
   * @param storage 当前存储信息
   * @param prevStorage 回滚目标存储信息
   * @param targetLayoutVersion 目标布局版本
   * @return 如果可以回滚返回true，否则返回false
   * @throws IOException 通信或IO异常
   */
  Boolean canRollBack(String journalId, String nameServiceid,
                      StorageInfo storage, StorageInfo prevStorage,
                      int targetLayoutVersion) throws IOException;

  /**
   * 执行回滚操作，恢复到升级前状态。
   * @param journalId 日志ID
   * @param nameServiceid 命名服务ID
   * @throws IOException 通信或IO异常
   */
  void doRollback(String journalId,
                         String nameServiceid) throws IOException;

  /**
   * 丢弃所有起始事务ID大于等于指定txid的日志分段。
   * @param journalId 日志ID
   * @param nameServiceId 命名服务ID
   * @param startTxId 起始事务ID阈值
   * @throws IOException 通信或IO异常
   */
  @Idempotent
  void discardSegments(String journalId,
                       String nameServiceId,
                       long startTxId)
      throws IOException;

  /**
   * 获取日志的创建时间戳。
   * @param journalId 日志ID
   * @param nameServiceId 命名服务ID
   * @return 日志创建时间戳
   * @throws IOException 通信或IO异常
   */
  Long getJournalCTime(String journalId,
                       String nameServiceId) throws IOException;
}