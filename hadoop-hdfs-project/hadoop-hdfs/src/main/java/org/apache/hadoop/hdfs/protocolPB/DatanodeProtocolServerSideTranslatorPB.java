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

package org.apache.hadoop.hdfs.protocolPB;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hdfs.protocol.BlockListAsLongs;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.RollingUpgradeStatus;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.BlockReceivedAndDeletedRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.BlockReceivedAndDeletedResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.BlockReportRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.BlockReportResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.CacheReportRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.CacheReportResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.CommitBlockSynchronizationRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.CommitBlockSynchronizationResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.ErrorReportRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.ErrorReportResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.HeartbeatRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.HeartbeatResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.ReceivedDeletedBlockInfoProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.RegisterDatanodeRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.RegisterDatanodeResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.ReportBadBlocksRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.ReportBadBlocksResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.StorageBlockReportProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.StorageReceivedDeletedBlocksProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.DatanodeIDProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.LocatedBlockProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.RollingUpgradeStatusProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsServerProtos.VersionRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsServerProtos.VersionResponseProto;
import org.apache.hadoop.hdfs.server.protocol.DatanodeCommand;
import org.apache.hadoop.hdfs.server.protocol.DatanodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.apache.hadoop.hdfs.server.protocol.HeartbeatResponse;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.server.protocol.ReceivedDeletedBlockInfo;
import org.apache.hadoop.hdfs.server.protocol.StorageBlockReport;
import org.apache.hadoop.hdfs.server.protocol.StorageReceivedDeletedBlocks;
import org.apache.hadoop.hdfs.server.protocol.StorageReport;
import org.apache.hadoop.hdfs.server.protocol.VolumeFailureSummary;

import org.apache.hadoop.util.Preconditions;
import org.apache.hadoop.thirdparty.protobuf.RpcController;
import org.apache.hadoop.thirdparty.protobuf.ServiceException;

/**
 * DataNode协议服务端PB翻译器，负责将Protobuf格式的RPC请求转换为原生Java对象调用，并将返回结果转换回Protobuf格式
 * 实现DatanodeProtocolPB接口，作为NameNode端处理DataNode RPC请求的转换器，衔接Protobuf序列化层和原生服务实现层
 */
public class DatanodeProtocolServerSideTranslatorPB implements
    DatanodeProtocolPB {

  private final DatanodeProtocol impl;
  private final int maxDataLength;

  // 预定义空错误报告响应，减少重复对象创建
  private static final ErrorReportResponseProto
      VOID_ERROR_REPORT_RESPONSE_PROTO = 
          ErrorReportResponseProto.newBuilder().build();
  // 预定义空块接收删除响应，减少重复对象创建
  private static final BlockReceivedAndDeletedResponseProto 
      VOID_BLOCK_RECEIVED_AND_DELETE_RESPONSE = 
          BlockReceivedAndDeletedResponseProto.newBuilder().build();
  // 预定义空坏块报告响应，减少重复对象创建
  private static final ReportBadBlocksResponseProto
      VOID_REPORT_BAD_BLOCK_RESPONSE = 
          ReportBadBlocksResponseProto.newBuilder().build();
  // 预定义空块同步提交响应，减少重复对象创建
  private static final CommitBlockSynchronizationResponseProto 
      VOID_COMMIT_BLOCK_SYNCHRONIZATION_RESPONSE_PROTO =
          CommitBlockSynchronizationResponseProto.newBuilder().build();

  /**
   * 构造函数，初始化转换器，持有DatanodeProtocol服务实现
   * @param impl DatanodeProtocol原生服务实现实例
   * @param maxDataLength 块报告最大允许数据长度，用于解析块列表时做长度校验
   */
  public DatanodeProtocolServerSideTranslatorPB(DatanodeProtocol impl,
      int maxDataLength) {
    this.impl = impl;
    this.maxDataLength = maxDataLength;
  }

  /**
   * 处理DataNode注册请求，完成Protobuf和原生对象互转并调用服务端注册逻辑
   * @param controller RPC控制器
   * @param request Protobuf格式的注册请求
   * @return Protobuf格式的注册响应
   * @throws ServiceException 服务调用异常封装
   */
  @Override
  public RegisterDatanodeResponseProto registerDatanode(
      RpcController controller, RegisterDatanodeRequestProto request)
      throws ServiceException {
    DatanodeRegistration registration = PBHelper.convert(request
        .getRegistration());
    DatanodeRegistration registrationResp;
    try {
      registrationResp = impl.registerDatanode(registration);
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return RegisterDatanodeResponseProto.newBuilder()
        .setRegistration(PBHelper.convert(registrationResp)).build();
  }

  /**
   * 处理DataNode心跳请求，完成Protobuf和原生对象互转并调用服务端心跳处理逻辑
   * @param controller RPC控制器
   * @param request Protobuf格式的心跳请求
   * @return Protobuf格式的心跳响应
   * @throws ServiceException 服务调用异常封装
   */
  @Override
  public HeartbeatResponseProto sendHeartbeat(RpcController controller,
      HeartbeatRequestProto request) throws ServiceException {
    HeartbeatResponse response;
    try {
      // 转换存储报告列表为原生对象
      final StorageReport[] report = PBHelperClient.convertStorageReports(
          request.getReportsList());
      // 转换卷故障摘要（如果存在）
      VolumeFailureSummary volumeFailureSummary =
          request.hasVolumeFailureSummary() ? PBHelper.convertVolumeFailureSummary(
              request.getVolumeFailureSummary()) : null;
      // 调用原生服务处理心跳
      response = impl.sendHeartbeat(PBHelper.convert(request.getRegistration()),
          report, request.getCacheCapacity(), request.getCacheUsed(),
          request.getXmitsInProgress(),
          request.getXceiverCount(), request.getFailedVolumes(),
          volumeFailureSummary, request.getRequestFullBlockReportLease(),
          PBHelper.convertSlowPeerInfo(request.getSlowPeersList()),
          PBHelper.convertSlowDiskInfo(request.getSlowDisksList()));
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    HeartbeatResponseProto.Builder builder = HeartbeatResponseProto
        .newBuilder();
    // 添加所有DataNode命令到响应
    DatanodeCommand[] cmds = response.getCommands();
    if (cmds != null) {
      for (int i = 0; i < cmds.length; i++) {
        if (cmds[i] != null) {
          builder.addCmds(PBHelper.convert(cmds[i]));
        }
      }
    }
    // 设置NameNode HA状态
    builder.setHaStatus(PBHelper.convert(response.getNameNodeHaState()));
    // 设置滚动升级状态，兼容新旧版本DataNode
    RollingUpgradeStatus rollingUpdateStatus = response
        .getRollingUpdateStatus();
    if (rollingUpdateStatus != null) {
      // V2版本始终设置给新版本DataNode
      // 为兼容旧版本DataNode，已完成的滚动升级不设置V1字段
      RollingUpgradeStatusProto rus = PBHelperClient.
          convertRollingUpgradeStatus(rollingUpdateStatus);
      builder.setRollingUpgradeStatusV2(rus);
      if (!rollingUpdateStatus.isFinalized()) {
        builder.setRollingUpgradeStatus(rus);
      }
    }

    builder.setFullBlockReportLeaseId(response.getFullBlockReportLeaseId());
    builder.setIsSlownode(response.getIsSlownode());
    return builder.build();
  }

  /**
   * 处理DataNode块报告请求，完成Protobuf和原生对象互转并调用服务端块报告处理逻辑
   * @param controller RPC控制器
   * @param request Protobuf格式的块报告请求
   * @return Protobuf格式的块报告响应
   * @throws ServiceException 服务调用异常封装
   */
  @Override
  public BlockReportResponseProto blockReport(RpcController controller,
      BlockReportRequestProto request) throws ServiceException {
    DatanodeCommand cmd = null;
    // 初始化存储块报告数组
    StorageBlockReport[] report = 
        new StorageBlockReport[request.getReportsCount()];
    
    int index = 0;
    // 遍历处理每个存储的块报告
    for (StorageBlockReportProto s : request.getReportsList()) {
      final BlockListAsLongs blocks;
      if (s.hasNumberOfBlocks()) { // 处理新版本基于缓冲区的块报告格式
        int num = (int)s.getNumberOfBlocks();
        Preconditions.checkState(s.getBlocksCount() == 0,
            "cannot send both blocks list and buffers");
        // 从缓冲区解码块列表
        blocks = BlockListAsLongs.decodeBuffers(num, s.getBlocksBuffersList(),
            maxDataLength);
      } else { // 处理旧版本基于long数组的块报告格式
        // 从long数组解码块列表
        blocks = BlockListAsLongs.decodeLongs(s.getBlocksList(), maxDataLength);
      }
      // 构造存储块报告对象
      report[index++] = new StorageBlockReport(PBHelperClient.convert(s.getStorage()),
          blocks);
    }
    try {
      // 调用原生服务处理块报告
      cmd = impl.blockReport(PBHelper.convert(request.getRegistration()),
          request.getBlockPoolId(), report,
          request.hasContext() ?
              PBHelper.convert(request.getContext()) : null);
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    BlockReportResponseProto.Builder builder = 
        BlockReportResponseProto.newBuilder();
    if (cmd != null) {
      builder.setCmd(PBHelper.convert(cmd));
    }
    return builder.build();
  }

  /**
   * 处理DataNode缓存报告请求，完成Protobuf和原生对象互转并调用服务端缓存报告处理逻辑
   * @param controller RPC控制器
   * @param request Protobuf格式的缓存报告请求
   * @return Protobuf格式的缓存报告响应
   * @throws ServiceException 服务调用异常封装
   */
  @Override
  public CacheReportResponseProto cacheReport(RpcController controller,
      CacheReportRequestProto request) throws ServiceException {
    DatanodeCommand cmd = null;
    try {
      // 调用原生服务处理缓存报告
      cmd = impl.cacheReport(
          PBHelper.convert(request.getRegistration()),
          request.getBlockPoolId(),
          request.getBlocksList());
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    CacheReportResponseProto.Builder builder =
        CacheReportResponseProto.newBuilder();
    if (cmd != null) {
      builder.setCmd(PBHelper.convert(cmd));
    }
    return builder.build();
  }


  /**
   * 处理块接收删除通知请求，完成Protobuf和原生对象互转并调用服务端处理逻辑
   * @param controller RPC控制器
   * @param request Protobuf格式的块接收删除请求
   * @return Protobuf格式的空响应
   * @throws ServiceException 服务调用异常封装
   */
  @Override
  public BlockReceivedAndDeletedResponseProto blockReceivedAndDeleted(
      RpcController controller, BlockReceivedAndDeletedRequestProto request)
      throws ServiceException {
    List<StorageReceivedDeletedBlocksProto> sBlocks = request.getBlocksList();
    StorageReceivedDeletedBlocks[] info = 
        new StorageReceivedDeletedBlocks[sBlocks.size()];
    // 遍历转换每个存储的接收删除块信息
    for (int i = 0; i < sBlocks.size(); i++) {
      StorageReceivedDeletedBlocksProto sBlock = sBlocks.get(i);
      List<ReceivedDeletedBlockInfoProto> list = sBlock.getBlocksList();
      ReceivedDeletedBlockInfo[] rdBlocks = 
          new ReceivedDeletedBlockInfo[list.size()];
      for (int j = 0; j < list.size(); j++) {
        rdBlocks[j] = PBHelper.convert(list.get(j));
      }
      // 兼容新旧存储信息格式
      if (sBlock.hasStorage()) {
        info[i] = new StorageReceivedDeletedBlocks(
            PBHelperClient.convert(sBlock.getStorage()), rdBlocks);
      } else {
        info[i] = new StorageReceivedDeletedBlocks(
            new DatanodeStorage(sBlock.getStorageUuid()), rdBlocks);
      }
    }
    try {
      // 调用原生服务处理块接收删除通知
      impl.blockReceivedAndDeleted(PBHelper.convert(request.getRegistration()),
          request.getBlockPoolId(), info);
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return VOID_BLOCK_RECEIVED_AND_DELETE_RESPONSE;
  }

  /**
   * 处理错误报告请求，完成Protobuf和原生对象互转并调用服务端错误处理逻辑
   * @param controller RPC控制器
   * @param request Protobuf格式的错误报告请求
   * @return Protobuf格式的空响应
   * @throws ServiceException 服务调用异常封装
   */
  @Override
  public ErrorReportResponseProto errorReport(RpcController controller,
      ErrorReportRequestProto request) throws ServiceException {
    try {
      // 调用原生服务处理错误报告
      impl.errorReport(PBHelper.convert(request.getRegistartion()),
          request.getErrorCode(), request.getMsg());
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return VOID_ERROR_REPORT_RESPONSE_PROTO;
  }

  /**
   * 处理版本信息请求，完成Protobuf和原生对象互转并调用服务端版本查询逻辑
   * @param controller RPC控制器
   * @param request Protobuf格式的版本请求
   * @return Protobuf格式的版本响应
   * @throws ServiceException 服务调用异常封装
   */
  @Override
  public VersionResponseProto versionRequest(RpcController controller,
      VersionRequestProto request) throws ServiceException {
    NamespaceInfo info;
    try {
      // 调用原生服务获取命名空间版本信息
      info = impl.versionRequest();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return VersionResponseProto.newBuilder()
        .setInfo(PBHelper.convert(info)).build();
  }

  /**
   * 处理坏块报告请求，完成Protobuf和原生对象互转并调用服务端坏块处理逻辑
   * @param controller RPC控制器
   * @param request Protobuf格式的坏块报告请求
   * @return Protobuf格式的空响应
   * @throws ServiceException 服务调用异常封装
   */
  @Override
  public ReportBadBlocksResponseProto reportBadBlocks(RpcController controller,
      ReportBadBlocksRequestProto request) throws ServiceException {
    List<LocatedBlockProto> lbps = request.getBlocksList();
    LocatedBlock [] blocks = new LocatedBlock [lbps.size()];
    // 转换所有坏块信息为原生对象
    for(int i=0; i<lbps.size(); i++) {
      blocks[i] = PBHelperClient.convertLocatedBlockProto(lbps.get(i));
    }
    try {
      // 调用原生服务处理坏块报告
      impl.reportBadBlocks(blocks);
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return VOID_REPORT_BAD_BLOCK_RESPONSE;
  }

  /**
   * 处理块同步提交请求，完成Protobuf和原生对象互转并调用服务端块同步逻辑
   * @param controller RPC控制器
   * @param request Protobuf格式的块同步提交请求
   * @return Protobuf格式的空响应
   * @throws ServiceException 服务调用异常封装
   */
  @Override
  public CommitBlockS