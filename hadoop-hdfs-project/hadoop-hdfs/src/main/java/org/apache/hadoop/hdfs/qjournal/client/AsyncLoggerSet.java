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
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.GetJournalStateResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.GetJournaledEditsResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.NewEpochResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.PrepareRecoveryResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.SegmentStateProto;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.server.protocol.RemoteEditLogManifest;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.base.Joiner;
import org.apache.hadoop.util.Preconditions;
import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableList;
import org.apache.hadoop.thirdparty.com.google.common.collect.Maps;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ListenableFuture;

/**
 * @file AsyncLoggerSet.java
 * @brief 基于QJM日志写入的异步日志器集合管理类，负责向所有远程日志节点广播请求并构造法定人数调用实例
 *
 * QJM(Quorum Journal Manager)是HDFS HA架构中共享编辑日志的实现，该类管理多个远程JournalNode的
 * 异步日志器实例，统一分发操作请求，处理法定人数一致性判断。
 */
class AsyncLoggerSet {
  static final Logger LOG = LoggerFactory.getLogger(AsyncLoggerSet.class);

  private final List<AsyncLogger> loggers;
  
  private static final long INVALID_EPOCH = -1;
  private long myEpoch = INVALID_EPOCH;
  
  /**
   * 构造异步日志器集合，将输入列表转为不可变列表存储
   * @param loggers 异步日志器列表，对应多个JournalNode节点
   */
  public AsyncLoggerSet(List<AsyncLogger> loggers) {
    this.loggers = ImmutableList.copyOf(loggers);
  }
  
  /**
   * 设置当前写入周期的epoch编号，同步到所有日志器
   * @param e epoch编号
   */
  void setEpoch(long e) {
    Preconditions.checkState(!isEpochEstablished(),
        "Epoch already established: epoch=%s", myEpoch);
    myEpoch = e;
    for (AsyncLogger l : loggers) {
      l.setEpoch(e);
    }
  }

  /**
   * 设置当前成功提交的最大事务ID，同步到所有日志器
   * 用于协议层面的一致性检查，防止错乱写入，详见HDFS-3863
   * @param txid 已提交的最高事务ID
   */
  public void setCommittedTxId(long txid) {
    for (AsyncLogger logger : loggers) {
      logger.setCommittedTxId(txid);
    }
  }

  /**
   * 检查当前是否已经建立合法epoch
   * @return true表示epoch已建立，false表示还未建立
   */
  boolean isEpochEstablished() {
    return myEpoch != INVALID_EPOCH;
  }
  
  /**
   * 获取当前writer的epoch编号，仅在成功创建唯一epoch后才能调用
   * @return 当前epoch编号
   */
  long getEpoch() {
    Preconditions.checkState(myEpoch != INVALID_EPOCH,
        "No epoch created yet");
    return myEpoch;
  }

  /**
   * 关闭所有底层异步日志器，释放连接资源
   */
  void close() {
    for (AsyncLogger logger : loggers) {
      logger.close();
    }
  }
  
  /**
   * 清理所有日志节点上早于指定事务ID的旧日志
   * @param minTxIdToKeep 需要保留的最小事务ID，小于该ID的日志将被清理
   */
  void purgeLogsOlderThan(long minTxIdToKeep) {
    for (AsyncLogger logger : loggers) {
      logger.purgeLogsOlderThan(minTxIdToKeep);
    }
  }


  /**
   * 等待法定人数节点对写入操作响应成功，若达不到法定人数则抛出异常
   * @param q 法定人数调用实例
   * @param timeoutMs 等待超时时间（毫秒）
   * @param operationName 操作名称，用于日志输出
   * @return 所有成功响应的结果映射表，键为日志器，值为响应结果
   * @throws QuorumException 无法达到法定人数成功响应时抛出
   * @throws IOException 线程被中断或等待超时时抛出
   */
  <V> Map<AsyncLogger, V> waitForWriteQuorum(QuorumCall<AsyncLogger, V> q,
      int timeoutMs, String operationName) throws IOException {
    int majority = getMajoritySize();
    try {
      q.waitFor(
          loggers.size(), // 等待所有节点响应
          majority, // 成功响应数达到法定人数即可返回
          majority, // 失败响应数达到法定人数即可返回
          timeoutMs, operationName);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("Interrupted waiting " + timeoutMs + "ms for a " +
          "quorum of nodes to respond.");
    } catch (TimeoutException e) {
      throw new IOException("Timed out waiting " + timeoutMs + "ms for a " +
          "quorum of nodes to respond.");
    }
    
    if (q.countSuccesses() < majority) {
      q.rethrowException("Got too many exceptions to achieve quorum size " +
          getMajorityString());
    }
    
    return q.getResults();
  }
  
  /**
   * 计算达成法定人数需要的最少节点数，公式为 n/2 + 1
   * @return 法定人数最少节点数
   */
  int getMajoritySize() {
    return loggers.size() / 2 + 1;
  }
  
  /**
   * 生成法定人数占比的文本描述，例如"2/3"或"3/5"
   * @return 法定人数占比字符串
   */
  String getMajorityString() {
    return getMajoritySize() + "/" + loggers.size();
  }

  /**
   * 获取当前集合管理的日志器总数
   * @return 日志器数量
   */
  int size() {
    return loggers.size();
  }
  
  @Override
  public String toString() {
    return "[" + Joiner.on(", ").join(loggers) + "]";
  }

  /**
   * 将所有底层日志器的当前状态格式化为HTML报告，追加到输入StringBuilder
   * @param sb 用于接收HTML报告的StringBuilder
   */
  void appendReport(StringBuilder sb) {
    for (int i = 0, len = loggers.size(); i < len; ++i) {
      AsyncLogger l = loggers.get(i);
      if (i != 0) {
        sb.append(", ");
      }
      sb.append(l).append(" (");
      l.appendReport(sb);
      sb.append(")");
    }
  }

  /**
   * 获取日志器列表，仅用于测试场景构造mock对象
   * @return 不可变的日志器列表
   */
  @VisibleForTesting
  List<AsyncLogger> getLoggersForTests() {
    return loggers;
  }
  
  ///////////////////////////////////////////////////////////////////////////
  // 以下均为模板包装方法，将各类RPC调用广播分发到底层所有异步日志器，
  // 然后将结果封装为QuorumCall实例返回给上层处理法定人数一致性
  ///////////////////////////////////////////////////////////////////////////
  
  /**
   * 向所有日志节点发起获取日志状态请求
   * @return 封装好的法定人数调用实例
   */
  public QuorumCall<AsyncLogger, GetJournalStateResponseProto> getJournalState() {
    Map<AsyncLogger, ListenableFuture<GetJournalStateResponseProto>> calls =
        Maps.newHashMap();
    for (AsyncLogger logger : loggers) {
      calls.put(logger, logger.getJournalState());
    }
    return QuorumCall.create(calls);    
  }
  
  /**
   * 向所有日志节点发起检查是否已格式化请求
   * @return 封装好的法定人数调用实例
   */
  public QuorumCall<AsyncLogger, Boolean> isFormatted() {
    Map<AsyncLogger, ListenableFuture<Boolean>> calls = Maps.newHashMap();
    for (AsyncLogger logger : loggers) {
      calls.put(logger, logger.isFormatted());
    }
    return QuorumCall.create(calls);
  }

  /**
   * 向所有日志节点发起创建新epoch请求
   * @param nsInfo 命名空间信息
   * @param epoch 新epoch编号
   * @return 封装好的法定人数调用实例
   */
  public QuorumCall<AsyncLogger,NewEpochResponseProto> newEpoch(
      NamespaceInfo nsInfo,
      long epoch) {
    Map<AsyncLogger, ListenableFuture<NewEpochResponseProto>> calls =
        Maps.newHashMap();
    for (AsyncLogger logger : loggers) {
      calls.put(logger, logger.newEpoch(epoch));
    }
    return QuorumCall.create(calls);    
  }

  /**
   * 向所有日志节点发起启动新日志段请求
   * @param txid 日志段起始事务ID
   * @param layoutVersion HDFS存储布局版本
   * @return 封装好的法定人数调用实例
   */
  public QuorumCall<AsyncLogger, Void> startLogSegment(
      long txid, int layoutVersion) {
    Map<AsyncLogger, ListenableFuture<Void>> calls = Maps.newHashMap();
    for (AsyncLogger logger : loggers) {
      calls.put(logger, logger.startLogSegment(txid, layoutVersion));
    }
    return QuorumCall.create(calls);
  }
  
  /**
   * 向所有日志节点发起完成日志段请求
   * @param firstTxId 日志段起始事务ID
   * @param lastTxId 日志段结束事务ID
   * @return 封装好的法定人数调用实例
   */
  public QuorumCall<AsyncLogger, Void> finalizeLogSegment(long firstTxId,
      long lastTxId) {
    Map<AsyncLogger, ListenableFuture<Void>> calls = Maps.newHashMap();
    for (AsyncLogger logger : loggers) {
      calls.put(logger, logger.finalizeLogSegment(firstTxId, lastTxId));
    }
    return QuorumCall.create(calls);
  }
  
  /**
   * 向所有日志节点发送编辑日志数据写入请求
   * @param segmentTxId 当前日志段的起始事务ID
   * @param firstTxnId 本次写入的第一个事务ID
   * @param numTxns 本次写入的事务数量
   * @param data 序列化后的编辑日志字节数据
   * @return 封装好的法定人数调用实例
   */
  public QuorumCall<AsyncLogger, Void> sendEdits(
      long segmentTxId, long firstTxnId, int numTxns, byte[] data) {
    Map<AsyncLogger, ListenableFuture<Void>> calls = Maps.newHashMap();
    for (AsyncLogger logger : loggers) {
      ListenableFuture<Void> future = 
        logger.sendEdits(segmentTxId, firstTxnId, numTxns, data);
      calls.put(logger, future);
    }
    return QuorumCall.create(calls);
  }

  /**
   * 向所有日志节点获取从指定事务ID开始的编辑日志
   * @param fromTxnId 起始事务ID
   * @param maxTransactions 最大返回事务数量
   * @return 封装好的法定人数调用实例
   */
  public QuorumCall<AsyncLogger, GetJournaledEditsResponseProto>
  getJournaledEdits(long fromTxnId, int maxTransactions) {
    Map<AsyncLogger,
        ListenableFuture<GetJournaledEditsResponseProto>> calls
        = Maps.newHashMap();
    for (AsyncLogger logger : loggers) {
      ListenableFuture<GetJournaledEditsResponseProto> future =
          logger.getJournaledEdits(fromTxnId, maxTransactions);
      calls.put(logger, future);
    }
    return QuorumCall.create(calls);
  }

  /**
   * 向所有日志节点获取从指定事务ID开始的编辑日志清单
   * @param fromTxnId 起始事务ID
   * @param inProgressOk 是否允许返回未完成的日志段
   * @return 封装好的法定人数调用实例
   */
  public QuorumCall<AsyncLogger, RemoteEditLogManifest> getEditLogManifest(
      long fromTxnId, boolean inProgressOk) {
    Map<AsyncLogger,
        ListenableFuture<RemoteEditLogManifest>> calls
        = Maps.newHashMap();
    for (AsyncLogger logger : loggers) {
      ListenableFuture<RemoteEditLogManifest> future =
          logger.getEditLogManifest(fromTxnId, inProgressOk);
      calls.put(logger, future);
    }
    return QuorumCall.create(calls);
  }

  /**
   * 向所有日志节点发起日志恢复准备请求
   * @param segmentTxId 需要恢复的日志段起始事务ID
   * @return 封装好的法定人数调用实例
   */
  QuorumCall<AsyncLogger, PrepareRecoveryResponseProto>
      prepareRecovery(long segmentTxId) {
    Map<AsyncLogger,
      ListenableFuture<PrepareRecoveryResponseProto>> calls
      = Maps.newHashMap();
    for (AsyncLogger logger : loggers) {
      ListenableFuture<PrepareRecoveryResponseProto> future =
          logger.prepareRecovery(segmentTxId);
      calls.put(logger, future);
    }
    return QuorumCall.create(calls);
  }

  /**
   * 向所有日志节点提交恢复结果，接受恢复后的日志段
   * @param log 恢复后的日志段状态信息
   * @param fromURL 源日志段数据地址，用于拉取数据
   * @return 封装好的法定人数调用实例
   */
  QuorumCall<AsyncLogger,Void>
      acceptRecovery(SegmentStateProto log, URL fromURL) {
    Map<AsyncLogger, ListenableFuture<Void>> calls
      = Maps.newHashMap();
    for (AsyncLogger logger : loggers) {
      ListenableFuture<Void> future =
          logger.acceptRecovery(log, fromURL);
      calls.put(logger, future);
    }
    return QuorumCall.create(calls);
  }

  /**
   * 向所有日志节点发起格式化共享存储请求
   * @param nsInfo 命名空间信息
   * @param force 是否强制格式化
   * @return 封装好的法定人数调用实例
   */
  QuorumCall<AsyncLogger, Void> format(NamespaceInfo nsInfo, boolean force) {
    Map<AsyncLogger, ListenableFuture<Void>> calls =
        Maps.newHashMap();
    for (AsyncLogger logger : loggers) {
      ListenableFuture<Void> future =
          logger.format(nsInfo, force);
      calls.put(logger, future);
    }
    return QuorumCall.create(calls);
  }
  
  /**
   * 向所有日志节点发起升级前准备请求
   * @return 封装好的法定人数调用实例
   */
  QuorumCall<AsyncLogger, Void> doPreUpgrade() {
    Map<AsyncLogger, ListenableFuture<Void>> calls =
        Maps.newHashMap();
    for (AsyncLogger logger : loggers) {
      ListenableFuture<Void> future =
          logger.doPreUpgrade();
      calls.put(logger, future);
    }
    return QuorumCall.create(calls);
  }

  /**
   * 向所有日志节点执行存储升级请求
   * @param sInfo 升级后的存储信息
   * @return 封装好的法定人数调用实例
   */
  public QuorumCall<AsyncLogger, Void> doUpgrade(StorageInfo sInfo) {
    Map<AsyncLogger, ListenableFuture<Void>> calls =
        Maps.newHashMap();
    for (AsyncLogger logger : loggers) {
      ListenableFuture<Void> future =
          logger.doUpgrade(sInfo);
      calls.put(logger, future);
    }
    return QuorumCall.create(calls);
  }

  /**
   * 向所有日志节点执行升级完成确认请求
   * @return 封装好的法定人数调用实例
   */
  public QuorumCall<AsyncLogger, Void> doFinalize() {