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

import java.net.InetSocketAddress;
import java.net.URL;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocol;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.GetJournaledEditsResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.GetJournalStateResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.NewEpochResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.PrepareRecoveryResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.SegmentStateProto;
import org.apache.hadoop.hdfs.qjournal.protocol.RequestInfo;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.server.protocol.RemoteEditLogManifest;

import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ListenableFuture;

/**
 * HDFS QJM 异步远程日志接口，对 QJournalProtocol 进行异步封装，用于与远程 JournalNode 通信。
 * 核心区别：
 * <ul>
 * <li>所有操作返回 {@link ListenableFuture} 异步结果，而非同步返回对象</li>
 * <li>{@link RequestInfo} 对象由底层实现自动创建，无需调用方构造</li>
 * </ul>
 * 在 QJM 联邦日志方案中，负责将编辑日志异步发送到多个远程 JournalNode，不阻塞主线程。
 */
interface AsyncLogger {
  
  /**
   * AsyncLogger 工厂接口，用于创建 AsyncLogger 实例
   */
  interface Factory {
    /**
     * 创建异步日志记录器实例
     * @param conf Hadoop 配置对象
     * @param nsInfo HDFS 命名空间信息
     * @param journalId 日志分组ID
     * @param nameServiceId 名称服务ID
     * @param addr 远程 JournalNode 地址
     * @return 新建的 AsyncLogger 实例
     */
    AsyncLogger createLogger(Configuration conf, NamespaceInfo nsInfo,
        String journalId, String nameServiceId, InetSocketAddress addr);
  }

  /**
   * 批量发送编辑日志事务到远程 JournalNode
   * @param segmentTxId 当前日志分段的起始事务ID
   * @param firstTxnId 本次批量发送的第一个事务ID
   * @param numTxns 本次批量发送的事务数量
   * @param data 序列化后的编辑日志数据
   * @return 异步操作结果对象
   */
  public ListenableFuture<Void> sendEdits(
      final long segmentTxId, final long firstTxnId,
      final int numTxns, final byte[] data);

  /**
   * 开始一个新的编辑日志分段
   * @param txid 新日志分段的第一个事务ID
   * @param layoutVersion HDFS 存储版本号
   * @return 异步操作结果对象
   */
  public ListenableFuture<Void> startLogSegment(long txid, int layoutVersion);

  /**
   * 完成并持久化一个编辑日志分段
   * @param startTxId 日志分段起始事务ID
   * @param endTxId 日志分段结束事务ID
   * @return 异步操作结果对象
   */
  public ListenableFuture<Void> finalizeLogSegment(
      long startTxId, long endTxId);

  /**
   * 请求远程 JournalNode 清理早于指定事务ID的旧日志
   * @param minTxIdToKeep 需要保留的最小事务ID
   * @return 异步操作结果对象
   */
  public ListenableFuture<Void> purgeLogsOlderThan(long minTxIdToKeep);

  /**
   * 格式化远程 JournalNode 的日志目录
   * @param nsInfo 命名空间信息，用于初始化存储
   * @param force 是否强制格式化
   * @return 异步操作结果对象
   */
  public ListenableFuture<Void> format(NamespaceInfo nsInfo, boolean force);

  /**
   * 检查远程 JournalNode 是否已完成格式化，存在有效数据
   * @return 异步结果，true 表示已格式化
   */
  public ListenableFuture<Boolean> isFormatted();
  
  /**
   * 获取远程 JournalNode 上最新 epoch 的状态信息
   * @return 异步结果，返回日志状态响应
   */
  public ListenableFuture<GetJournalStateResponseProto> getJournalState();

  /**
   * 在远程 JournalNode 上开启一个新的 epoch
   * @param epoch 新 epoch 编号
   * @return 异步结果，返回新 epoch 响应
   */
  public ListenableFuture<NewEpochResponseProto> newEpoch(long epoch);

  /**
   * 从远程 JournalNode 缓存获取已记录的编辑日志
   * @param fromTxnId 起始事务ID
   * @param maxTransactions 最大返回事务数
   * @return 异步结果，返回编辑日志响应
   */
  public ListenableFuture<GetJournaledEditsResponseProto> getJournaledEdits(
      long fromTxnId, int maxTransactions);
  
  /**
   * 获取远程 JournalNode 上可用的编辑日志清单
   * @param fromTxnId 起始事务ID
   * @param inProgressOk 是否包含未完成的正在写入的日志分段
   * @return 异步结果，返回编辑日志清单
   */
  public ListenableFuture<RemoteEditLogManifest> getEditLogManifest(
      long fromTxnId, boolean inProgressOk);

  /**
   * 准备日志恢复，用于 JournalNode 故障恢复流程（HDFS-3077 设计文档）
   * @param segmentTxId 需要恢复的日志分段起始事务ID
   * @return 异步结果，返回恢复准备响应
   */
  public ListenableFuture<PrepareRecoveryResponseProto> prepareRecovery(
      long segmentTxId);

  /**
   * 接受恢复提案，完成故障恢复流程（HDFS-3077 设计文档）
   * @param log 需要恢复的日志分段状态
   * @param fromUrl 获取日志分段数据的URL地址
   * @return 异步操作结果对象
   */
  public ListenableFuture<Void> acceptRecovery(SegmentStateProto log,
      URL fromUrl);

  /**
   * 设置后续所有请求使用的 epoch 编号
   * @param e epoch 编号
   */
  public void setEpoch(long e);

  /**
   * 更新所有日志节点中已提交的最高事务ID，用于同步状态（HDFS-3863）
   * 当前节点自身已提交事务ID可能低于该全局值
   * @param txid 全局已提交最高事务ID
   */
  public void setCommittedTxId(long txid);

  /**
   * 构造用于下载指定日志分段的HTTP URL
   * @param segmentTxId 日志分段起始事务ID
   * @return 下载日志分段的HTTP URL
   */
  public URL buildURLToFetchLogs(long segmentTxId);
  
  /**
   * 关闭并释放资源（连接、线程等），关闭后不可再使用该实例
   */
  public void close();

  /**
   * 将当前日志记录器的状态以HTML格式追加到StringBuilder，用于NameNode WebUI展示
   * @param sb 用于拼接HTML状态报告的StringBuilder
   */
  public void appendReport(StringBuilder sb);

  /**
   * 执行存储升级前的准备步骤
   * @return 异步操作结果对象
   */
  public ListenableFuture<Void> doPreUpgrade();

  /**
   * 执行存储版本升级
   * @param sInfo 新存储版本信息
   * @return 异步操作结果对象
   */
  public ListenableFuture<Void> doUpgrade(StorageInfo sInfo);

  /**
   * 确认完成存储版本升级
   * @return 异步操作结果对象
   */
  public ListenableFuture<Void> doFinalize();

  /**
   * 检查是否支持回滚到指定存储版本
   * @param storage 当前存储信息
   * @param prevStorage 回滚目标存储信息
   * @param targetLayoutVersion 目标存储版本号
   * @return 异步结果，true 表示支持回滚
   */
  public ListenableFuture<Boolean> canRollBack(StorageInfo storage,
      StorageInfo prevStorage, int targetLayoutVersion);

  /**
   * 执行存储版本回滚
   * @return 异步操作结果对象
   */
  public ListenableFuture<Void> doRollback();

  /**
   * 丢弃指定起始事务ID的未完成日志分段
   * @param startTxId 日志分段起始事务ID
   * @return 异步操作结果对象
   */
  public ListenableFuture<Void> discardSegments(long startTxId);

  /**
   * 获取远程 JournalNode 日志的创建时间
   * @return 异步结果，返回日志创建时间戳
   */
  public ListenableFuture<Long> getJournalCTime();
}