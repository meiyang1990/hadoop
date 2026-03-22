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
package org.apache.hadoop.hdfs.qjournal.protocolPB;

import java.io.IOException;
import java.net.URL;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.protocolPB.JournalProtocolPB;
import org.apache.hadoop.hdfs.protocolPB.PBHelper;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocol;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.AcceptRecoveryRequestProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.AcceptRecoveryResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.CanRollBackRequestProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.CanRollBackResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.DiscardSegmentsRequestProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.DiscardSegmentsResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.DoFinalizeRequestProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.DoFinalizeResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.DoPreUpgradeRequestProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.DoPreUpgradeResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.DoRollbackRequestProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.DoRollbackResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.DoUpgradeRequestProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.DoUpgradeResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.FinalizeLogSegmentRequestProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.FinalizeLogSegmentResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.FormatRequestProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.FormatResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.GetEditLogManifestRequestProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.GetEditLogManifestResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.GetJournaledEditsRequestProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.GetJournaledEditsResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.GetJournalCTimeRequestProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.GetJournalCTimeResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.GetJournalStateRequestProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.GetJournalStateResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.HeartbeatRequestProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.HeartbeatResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.IsFormattedRequestProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.IsFormattedResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.JournalIdProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.JournalRequestProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.JournalResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.NewEpochRequestProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.NewEpochResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.PrepareRecoveryRequestProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.PrepareRecoveryResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.PurgeLogsRequestProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.PurgeLogsResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.StartLogSegmentRequestProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.StartLogSegmentResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.RequestInfo;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.NodeType;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.namenode.NameNodeLayoutVersion;
import org.apache.hadoop.hdfs.server.protocol.JournalProtocol;

import org.apache.hadoop.thirdparty.protobuf.RpcController;
import org.apache.hadoop.thirdparty.protobuf.ServiceException;

/**
 * QJournal协议服务端Protobuf转换器，将来自RPC层的Protobuf格式请求
 * 转发到原生QJournalProtocol实现进行处理，并将处理结果转换回Protobuf格式响应
 * 负责HDFS QJournal高可用编辑日志共享协议的PB序列化/反序列化中转
 */
@InterfaceAudience.Private
public class QJournalProtocolServerSideTranslatorPB implements QJournalProtocolPB {
  /** 被代理的原生QJournal协议服务端实现 */
  private final QJournalProtocol impl;

  private final static JournalResponseProto VOID_JOURNAL_RESPONSE =
  JournalResponseProto.newBuilder().build();

  private final static StartLogSegmentResponseProto
  VOID_START_LOG_SEGMENT_RESPONSE =
      StartLogSegmentResponseProto.newBuilder().build();

  /**
   * 构造方法，注入需要代理的原生服务端实现
   * @param impl 原生QJournal协议服务端实现
   */
  public QJournalProtocolServerSideTranslatorPB(QJournalProtocol impl) {
    this.impl = impl;
  }

  
  @Override
  /**
   * 检查日志节点是否已经完成格式化
   * @param controller RPC控制器
   * @param request Protobuf格式请求
   * @return Protobuf格式响应，包含是否已格式化结果
   * @throws ServiceException 服务异常封装
   */
  public IsFormattedResponseProto isFormatted(RpcController controller,
      IsFormattedRequestProto request) throws ServiceException {
    try {
      boolean ret = impl.isFormatted(
          convert(request.getJid()),
          request.hasNameServiceId() ? request.getNameServiceId() : null);
      return IsFormattedResponseProto.newBuilder()
          .setIsFormatted(ret)
          .build();
    } catch (IOException ioe) {
      throw new ServiceException(ioe);
    }
  }


  @Override
  /**
   * 获取日志节点的当前日志状态
   * @param controller RPC控制器
   * @param request Protobuf格式请求
   * @return Protobuf格式响应，包含日志状态信息
   * @throws ServiceException 服务异常封装
   */
  public GetJournalStateResponseProto getJournalState(RpcController controller,
      GetJournalStateRequestProto request) throws ServiceException {
    try {
      return impl.getJournalState(
          convert(request.getJid()),
          request.hasNameServiceId() ? request.getNameServiceId() : null);
    } catch (IOException ioe) {
      throw new ServiceException(ioe);
    }
  }

  /**
   * 将Protobuf格式的JournalId转换为原生字符串ID
   * @param jid Protobuf格式JournalId
   * @return 原生字符串JournalId
   */
  private String convert(JournalIdProto jid) {
    return jid.getIdentifier();
  }

  @Override
  /**
   * 开启新的Epoch，用于选主后角色切换
   * @param controller RPC控制器
   * @param request Protobuf格式请求
   * @return Protobuf格式响应，包含新Epoch信息
   * @throws ServiceException 服务异常封装
   */
  public NewEpochResponseProto newEpoch(RpcController controller,
      NewEpochRequestProto request) throws ServiceException {
    try {
      return impl.newEpoch(
          request.getJid().getIdentifier(),
          request.hasNameServiceId() ? request.getNameServiceId() : null,
          PBHelper.convert(request.getNsInfo()),
          request.getEpoch());
    } catch (IOException ioe) {
      throw new ServiceException(ioe);
    }
  }

  /**
   * 格式化日志节点存储
   * @param controller RPC控制器
   * @param request Protobuf格式请求
   * @return 空Protobuf响应
   * @throws ServiceException 服务异常封装
   */
  public FormatResponseProto format(RpcController controller,
      FormatRequestProto request) throws ServiceException {
    try {
      impl.format(request.getJid().getIdentifier(),
          request.hasNameServiceId() ? request.getNameServiceId() : null,
          PBHelper.convert(request.getNsInfo()), request.getForce());
      return FormatResponseProto.getDefaultInstance();
    } catch (IOException ioe) {
      throw new ServiceException(ioe);
    }
  }
  
  /** @see JournalProtocol#journal */
  @Override
  public JournalResponseProto journal(RpcController unused,
      JournalRequestProto req) throws ServiceException {
    try {
      impl.journal(convert(req.getReqInfo()),
          req.getSegmentTxnId(), req.getFirstTxnId(),
          req.getNumTxns(), req.getRecords().toByteArray());
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return VOID_JOURNAL_RESPONSE;
  }

  /** @see QJournalProtocol#heartbeat */
  @Override
  public HeartbeatResponseProto heartbeat(RpcController controller,
      HeartbeatRequestProto req) throws ServiceException {
    try {
      impl.heartbeat(convert(req.getReqInfo()));
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return HeartbeatResponseProto.getDefaultInstance();
  }

  /** @see JournalProtocol#startLogSegment */
  @Override
  public StartLogSegmentResponseProto startLogSegment(RpcController controller,
      StartLogSegmentRequestProto req) throws ServiceException {
    try {
      // 请求未指定布局版本时使用当前最新版本
      int layoutVersion = req.hasLayoutVersion() ? req.getLayoutVersion()
          : NameNodeLayoutVersion.CURRENT_LAYOUT_VERSION;
      impl.startLogSegment(convert(req.getReqInfo()), req.getTxid(),
          layoutVersion);
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return VOID_START_LOG_SEGMENT_RESPONSE;
  }
  
  @Override
  /**
   * 完成日志段的写入，正式固化日志段
   * @param controller RPC控制器
   * @param req Protobuf格式请求
   * @return 空Protobuf响应
   * @throws ServiceException 服务异常封装
   */
  public FinalizeLogSegmentResponseProto finalizeLogSegment(
      RpcController controller, FinalizeLogSegmentRequestProto req)
      throws ServiceException {
    try {
      impl.finalizeLogSegment(convert(req.getReqInfo()),
          req.getStartTxId(), req.getEndTxId());
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return FinalizeLogSegmentResponseProto.newBuilder().build();
  }
  
  @Override
  /**
   * 清理早于指定事务ID的旧日志
   * @param controller RPC控制器
   * @param req Protobuf格式请求
   * @return 空Protobuf响应
   * @throws ServiceException 服务异常封装
   */
  public PurgeLogsResponseProto purgeLogs(RpcController controller,
      PurgeLogsRequestProto req) throws ServiceException {
    try {
      impl.purgeLogsOlderThan(convert(req.getReqInfo()),
          req.getMinTxIdToKeep());
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return PurgeLogsResponseProto.getDefaultInstance();
  }

  @Override
  /**
   * 获取编辑日志清单，列出所有可用的日志段
   * @param controller RPC控制器
   * @param request Protobuf格式请求
   * @return Protobuf格式响应，包含日志清单信息
   * @throws ServiceException 服务异常封装
   */
  public GetEditLogManifestResponseProto getEditLogManifest(
      RpcController controller, GetEditLogManifestRequestProto request)
      throws ServiceException {
    try {
      return impl.getEditLogManifest(
          request.getJid().getIdentifier(),
          request.hasNameServiceId() ? request.getNameServiceId() : null,
          request.getSinceTxId(),
          request.getInProgressOk());
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  /**
   * 获取从指定事务ID开始的日志内容
   * @param controller RPC控制器
   * @param request Protobuf格式请求
   * @return Protobuf格式响应，包含日志内容
   * @throws ServiceException 服务异常封装
   */
  public GetJournaledEditsResponseProto getJournaledEdits(
      RpcController controller, GetJournaledEditsRequestProto request)
      throws ServiceException {
    try {
      return impl.getJournaledEdits(request.getJid().getIdentifier(),
          request.hasNameServiceId() ? request.getNameServiceId() : null,
          request.getSinceTxId(), request.getMaxTxns());
    } catch (IOException ioe) {
      throw new ServiceException(ioe);
    }
  }

  @Override
  /**
   * 准备恢复操作，获取日志节点当前未完成日志段的状态
   * @param controller RPC控制器
   * @param request Protobuf格式请求
   * @return Protobuf格式响应，包含日志段状态
   * @throws ServiceException 服务异常封装
   */
  public PrepareRecoveryResponseProto prepareRecovery(RpcController controller,
      PrepareRecoveryRequestProto request) throws ServiceException {
    try {
      return impl.prepareRecovery(convert(request.getReqInfo()),
          request.getSegmentTxId());
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  /**
   * 接受指定的恢复状态，完成日志恢复
   * @param controller RPC控制器
   * @param request Protobuf格式请求
   * @return 空Protobuf响应
   * @throws ServiceException 服务异常封装
   */
  public AcceptRecoveryResponseProto acceptRecovery(RpcController controller,
      AcceptRecoveryRequestProto request) throws ServiceException {
    try {
      impl.acceptRecovery(convert(request.getReqInfo()),
          request.getStateToAccept(),
          new URL(request.getFromURL()));
      return AcceptRecoveryResponseProto.getDefaultInstance();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  
  /**
   * 将Protobuf格式的RequestInfo转换为原生RequestInfo对象
   * @param reqInfo Protobuf格式请求信息
   * @return 原生请求信息对象
   */
  private RequestInfo convert(
      QJournalProtocolProtos.RequestInfoProto reqInfo) {
    return new RequestInfo(
        reqInfo.getJournalId().getIdentifier(),
        reqInfo.hasNameServiceId() ?
            reqInfo.getNameServiceId() : null,
        reqInfo.getEpoch(),
        reqInfo.getIpcSerialNumber(),
        reqInfo.hasCommittedTxId() ?
          reqInfo.getCommittedTxId() : HdfsServerConstants.INVALID_TXID);
  }


  @Override
  /**
   * 执行升级前准备操作
   * @param controller RPC控制器
   * @param request Protobuf格式请求
   * @return 空Protobuf响应
   * @throws ServiceException 服务异常封装
   */
  public DoPreUpgradeResponseProto doPreUpgrade(RpcController controller,
      DoPreUpgradeRequestProto request) throws ServiceException {
    try {
      impl.doPreUpgrade(convert(request.getJid()));
      return DoPreUpgradeResponseProto.getDefaultInstance();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  /**
   * 执行版本升级操作
   * @param controller RPC控制器
   * @param request Protobuf格式请求
   * @return 空Protobuf响应
   * @throws ServiceException 服务异常封装
   */
  public DoUpgradeResponseProto doUpgrade(RpcController controller,
      DoUpgradeRequestProto request) throws ServiceException {
    StorageInfo si = PBHelper.convert(request.getSInfo(), NodeType.JOURNAL_NODE);
    try {
      impl.doUpgrade(convert(request.getJid()), si);
      return DoUpgradeResponseProto.getDefaultInstance();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  /**
   * 完成升级操作，固化新版本
   * @param controller RPC控制器
   * @param request Protobuf格式请求
   * @return 空Protobuf响应
   * @throws ServiceException 服务异常封装
   */
  public DoFinalizeResponseProto doFinalize(RpcController controller,
      DoFinalizeRequestProto request) throws ServiceException {
    try {
      impl.doFinalize(convert(request.getJid()),
          request.hasNameServiceId() ? request.getNameServiceId() : null