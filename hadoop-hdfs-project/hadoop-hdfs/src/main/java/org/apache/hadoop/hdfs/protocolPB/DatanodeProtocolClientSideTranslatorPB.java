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

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.BlockListAsLongs;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.RollingUpgradeStatus;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.BlockReceivedAndDeletedRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.BlockReportRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.BlockReportResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.CacheReportRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.CacheReportResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.CommitBlockSynchronizationRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.DatanodeCommandProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.ErrorReportRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.HeartbeatRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.HeartbeatResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.RegisterDatanodeRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.RegisterDatanodeResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.ReportBadBlocksRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.StorageBlockReportProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.StorageReceivedDeletedBlocksProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsServerProtos.VersionRequestProto;
import org.apache.hadoop.hdfs.server.protocol.BlockReportContext;
import org.apache.hadoop.hdfs.server.protocol.DatanodeCommand;
import org.apache.hadoop.hdfs.server.protocol.DatanodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.HeartbeatResponse;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo.Capability;
import org.apache.hadoop.hdfs.server.protocol.ReceivedDeletedBlockInfo;
import org.apache.hadoop.hdfs.server.protocol.SlowDiskReports;
import org.apache.hadoop.hdfs.server.protocol.SlowPeerReports;
import org.apache.hadoop.hdfs.server.protocol.StorageBlockReport;
import org.apache.hadoop.hdfs.server.protocol.StorageReceivedDeletedBlocks;
import org.apache.hadoop.hdfs.server.protocol.StorageReport;
import org.apache.hadoop.hdfs.server.protocol.VolumeFailureSummary;
import org.apache.hadoop.ipc.ProtobufRpcEngine2;
import org.apache.hadoop.ipc.ProtocolMetaInterface;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RpcClientUtil;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.thirdparty.protobuf.RpcController;

import javax.annotation.Nonnull;

import static org.apache.hadoop.ipc.internal.ShadedProtobufHelper.ipc;

/**
 * 数据节点到NameNode的PB协议客户端转换器，将DatanodeProtocol接口的请求转换为Protobuf格式RPC调用
 * 实现了DatanodeProtocol接口，封装了PB序列化逻辑，对上层提供原生Java对象接口
 */
@InterfaceAudience.Private
@InterfaceStability.Stable
public class DatanodeProtocolClientSideTranslatorPB implements
    ProtocolMetaInterface, DatanodeProtocol, Closeable {
  
  /** RpcController is not used and hence is set to null */
  // RPC代理对象，指向DatanodeProtocolPB服务端
  private final DatanodeProtocolPB rpcProxy;
  // 空版本请求对象，复用单例
  private static final VersionRequestProto VOID_VERSION_REQUEST = 
      VersionRequestProto.newBuilder().build();
  // RpcController未使用，固定为null
  private final static RpcController NULL_CONTROLLER = null;
  
  /**
   * 测试用构造函数，直接注入RPC代理对象
   * @param rpcProxy DatanodeProtocolPB RPC代理对象
   */
  @VisibleForTesting
  public DatanodeProtocolClientSideTranslatorPB(DatanodeProtocolPB rpcProxy) {
    this.rpcProxy = rpcProxy;
  }

  /**
   * 构造函数，根据NameNode地址创建RPC客户端连接
   * @param nameNodeAddr NameNode地址
   * @param conf Hadoop配置对象
   * @throws IOException 创建连接失败时抛出异常
   */
  public DatanodeProtocolClientSideTranslatorPB(InetSocketAddress nameNodeAddr,
      Configuration conf) throws IOException {
    RPC.setProtocolEngine(conf, DatanodeProtocolPB.class,
        ProtobufRpcEngine2.class);
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    rpcProxy = createNamenode(nameNodeAddr, conf, ugi);
  }

  /**
   * 创建NameNode的RPC代理对象
   * @param nameNodeAddr NameNode地址
   * @param conf Hadoop配置对象
   * @param ugi 当前用户凭证信息
   * @return DatanodeProtocolPB RPC代理对象
   * @throws IOException 创建代理失败时抛出异常
   */
  private static DatanodeProtocolPB createNamenode(
      InetSocketAddress nameNodeAddr, Configuration conf,
      UserGroupInformation ugi) throws IOException {
    return RPC.getProxy(DatanodeProtocolPB.class,
        RPC.getProtocolVersion(DatanodeProtocolPB.class), nameNodeAddr, ugi,
        conf, NetUtils.getSocketFactory(conf, DatanodeProtocolPB.class));
  }

  /**
   * 关闭RPC连接，释放代理资源
   * @throws IOException 关闭失败时抛出异常
   */
  @Override
  public void close() throws IOException {
    RPC.stopProxy(rpcProxy);
  }

  /**
   * 向NameNode注册数据节点，转换为PB格式RPC调用
   * @param registration 数据节点注册信息
   * @return NameNode返回的注册确认信息
   * @throws IOException RPC调用失败时抛出异常
   */
  @Override
  public DatanodeRegistration registerDatanode(DatanodeRegistration registration
      ) throws IOException {
    RegisterDatanodeRequestProto.Builder builder = RegisterDatanodeRequestProto
        .newBuilder().setRegistration(PBHelper.convert(registration));
    RegisterDatanodeResponseProto resp;
    resp = ipc(() -> rpcProxy.registerDatanode(NULL_CONTROLLER, builder.build()));

    return PBHelper.convert(resp.getRegistration());
  }

  /**
   * 向NameNode发送心跳，上报数据节点状态，转换为PB格式RPC调用
   * @param registration 数据节点注册信息
   * @param reports 存储报告数组
   * @param cacheCapacity 缓存总容量
   * @param cacheUsed 已用缓存容量
   * @param xmitsInProgress 正在进行的数据传输数
   * @param xceiverCount 数据传输线程数
   * @param failedVolumes 失败卷数量
   * @param volumeFailureSummary 卷故障汇总信息
   * @param requestFullBlockReportLease 是否请求全量块报告租约
   * @param slowPeers 慢节点报告
   * @param slowDisks 慢磁盘报告
   * @return NameNode返回的心跳响应，包含命令、HA状态等信息
   * @throws IOException RPC调用失败时抛出异常
   */
  @Override
  public HeartbeatResponse sendHeartbeat(DatanodeRegistration registration,
      StorageReport[] reports, long cacheCapacity, long cacheUsed,
      int xmitsInProgress, int xceiverCount, int failedVolumes,
      VolumeFailureSummary volumeFailureSummary,
      boolean requestFullBlockReportLease,
      @Nonnull SlowPeerReports slowPeers,
      @Nonnull SlowDiskReports slowDisks)
          throws IOException {
    HeartbeatRequestProto.Builder builder = HeartbeatRequestProto.newBuilder()
        .setRegistration(PBHelper.convert(registration))
        .setXmitsInProgress(xmitsInProgress).setXceiverCount(xceiverCount)
        .setFailedVolumes(failedVolumes)
        .setRequestFullBlockReportLease(requestFullBlockReportLease);
    // 转换存储报告列表为PB格式
    builder.addAllReports(PBHelperClient.convertStorageReports(reports));
    // 非零时添加缓存容量信息
    if (cacheCapacity != 0) {
      builder.setCacheCapacity(cacheCapacity);
    }
    // 非零时添加已用缓存信息
    if (cacheUsed != 0) {
      builder.setCacheUsed(cacheUsed);
    }
    // 不为空时添加卷故障汇总
    if (volumeFailureSummary != null) {
      builder.setVolumeFailureSummary(PBHelper.convertVolumeFailureSummary(
          volumeFailureSummary));
    }
    // 存在慢节点时添加慢节点报告
    if (slowPeers.haveSlowPeers()) {
      builder.addAllSlowPeers(PBHelper.convertSlowPeerInfo(slowPeers));
    }
    // 存在慢磁盘时添加慢磁盘报告
    if (slowDisks.haveSlowDisks()) {
      builder.addAllSlowDisks(PBHelper.convertSlowDiskInfo(slowDisks));
    }

    HeartbeatResponseProto resp;
    resp = ipc(() -> rpcProxy.sendHeartbeat(NULL_CONTROLLER, builder.build()));

    // 转换PB格式命令数组为Java对象数组
    DatanodeCommand[] cmds = new DatanodeCommand[resp.getCmdsList().size()];
    int index = 0;
    for (DatanodeCommandProto p : resp.getCmdsList()) {
      cmds[index] = PBHelper.convert(p);
      index++;
    }
    RollingUpgradeStatus rollingUpdateStatus = null;
    // 优先使用v2格式的滚动升级状态，兼容旧版本
    if (resp.hasRollingUpgradeStatusV2()) {
      rollingUpdateStatus = PBHelperClient.convert(resp.getRollingUpgradeStatusV2());
    } else if (resp.hasRollingUpgradeStatus()) {
      rollingUpdateStatus = PBHelperClient.convert(resp.getRollingUpgradeStatus());
    }
    // 构造并返回心跳响应对象
    return new HeartbeatResponse(cmds, PBHelper.convert(resp.getHaStatus()),
        rollingUpdateStatus, resp.getFullBlockReportLeaseId(),
        resp.getIsSlownode());
  }

  /**
   * 向NameNode发送块报告，上报数据节点存储的所有块信息，转换为PB格式RPC调用
   * @param registration 数据节点注册信息
   * @param poolId 块池ID
   * @param reports 各存储的块报告数组
   * @param context 块报告上下文信息
   * @return NameNode返回给数据节点的命令，无命令则返回null
   * @throws IOException RPC调用失败时抛出异常
   */
  @Override
  public DatanodeCommand blockReport(DatanodeRegistration registration,
      String poolId, StorageBlockReport[] reports,
      BlockReportContext context)
        throws IOException {
    BlockReportRequestProto.Builder builder = BlockReportRequestProto
        .newBuilder().setRegistration(PBHelper.convert(registration))
        .setBlockPoolId(poolId);

    // 判断是否支持缓冲区格式的块报告（更高效的序列化方式）
    boolean useBlocksBuffer = registration.getNamespaceInfo()
        .isCapabilitySupported(Capability.STORAGE_BLOCK_REPORT_BUFFERS);

    // 遍历转换每个存储的块报告
    for (StorageBlockReport r : reports) {
      StorageBlockReportProto.Builder reportBuilder = StorageBlockReportProto
          .newBuilder().setStorage(PBHelperClient.convert(r.getStorage()));
      BlockListAsLongs blocks = r.getBlocks();
      // 根据能力选择序列化方式
      if (useBlocksBuffer) {
        // 使用缓冲区格式，减少序列化开销
        reportBuilder.setNumberOfBlocks(blocks.getNumberOfBlocks());
        reportBuilder.addAllBlocksBuffers(blocks.getBlocksBuffers());
      } else {
        // 兼容旧版本，逐个添加块长值
        for (long value : blocks.getBlockListAsLongs()) {
          reportBuilder.addBlocks(value);
        }
      }
      builder.addReports(reportBuilder.build());
    }
    // 设置块报告上下文
    builder.setContext(PBHelper.convert(context));
    BlockReportResponseProto resp;
    resp = ipc(() -> rpcProxy.blockReport(NULL_CONTROLLER, builder.build()));
    // 如果响应包含命令则转换返回，否则返回null
    return resp.hasCmd() ? PBHelper.convert(resp.getCmd()) : null;
  }

  /**
   * 向NameNode发送缓存块报告，上报数据节点缓存的块列表，转换为PB格式RPC调用
   * @param registration 数据节点注册信息
   * @param poolId 块池ID
   * @param blockIds 缓存的块ID列表
   * @return NameNode返回的命令，无命令则返回null
   * @throws IOException RPC调用失败时抛出异常
   */
  @Override
  public DatanodeCommand cacheReport(DatanodeRegistration registration,
      String poolId, List<Long> blockIds) throws IOException {
    CacheReportRequestProto.Builder builder =
        CacheReportRequestProto.newBuilder()
        .setRegistration(PBHelper.convert(registration))
        .setBlockPoolId(poolId);
    // 添加所有缓存块ID
    for (Long blockId : blockIds) {
      builder.addBlocks(blockId);
    }
    
    CacheReportResponseProto resp;
    resp = ipc(() -> rpcProxy.cacheReport(NULL_CONTROLLER, builder.build()));
    if (resp.hasCmd()) {
      return PBHelper.convert(resp.getCmd());
    }
    return null;
  }

  /**
   * 向NameNode上报数据节点收到和删除的块信息，转换为PB格式RPC调用
   * @param registration 数据节点注册信息
   * @param poolId 块池ID
   * @param receivedAndDeletedBlocks 各存储收到和删除的块数组
   * @throws IOException RPC调用失败时抛出异常
   */
  @Override
  public void blockReceivedAndDeleted(DatanodeRegistration registration,
      String poolId, StorageReceivedDeletedBlocks[] receivedAndDeletedBlocks)
      throws IOException {
    BlockReceivedAndDeletedRequestProto.Builder builder = 
        BlockReceivedAndDeletedRequestProto.newBuilder()
        .setRegistration(PBHelper.convert(registration))
        .setBlockPoolId(poolId);
    // 遍历转换每个存储的上报信息
    for (StorageReceivedDeletedBlocks storageBlock : receivedAndDeletedBlocks) {
      StorageReceivedDeletedBlocksProto.Builder repBuilder = 
          StorageReceivedDeletedBlocksProto.newBuilder();
      repBuilder.setStorageUuid(storageBlock.getStorage().getStorageID());  // Set for wire compatibility.
      repBuilder.setStorage(PBHelperClient.convert(storageBlock.getStorage()));
      // 添加每个块的信息
      for (ReceivedDeletedBlockInfo rdBlock : storageBlock.getBlocks()) {
        repBuilder.addBlocks(PBHelper.convert(rdBlock));
      }
      builder.addBlocks(repBuilder.build());
    }
    ipc(() -> rpcProxy.blockReceivedAndDeleted(NULL_CONTROLLER, builder.build()));
  }

  /**
   * 向NameNode发送错误报告，转换为PB格式RPC调用
   * @param registration 数据节点注册信息
   * @param errorCode 错误代码
   * @param msg 错误信息
   * @throws IOException RPC调用失败时抛出异常
   */
  @Override
  public void errorReport(DatanodeRegistration registration, int errorCode,
      String msg) throws IOException {
    ErrorReportRequestProto req = ErrorReportRequestProto.newBuilder()
        .setRegistartion(PBHelper.convert(registration))
        .setErrorCode(errorCode).setMsg(msg).build();
    ipc(() -> rpcProxy.errorReport(NULL_CONTROLLER, req));
  }

  /**
   * 向NameNode请求版本信息，转换为PB格式RPC调用
   * @return NameNode返回的命名空间版本信息
   * @throws IOException RPC调用失败时抛出异常
   */
  @Override
  public NamespaceInfo versionRequest() throws IOException {
    return PBHelper.convert(ipc(() -> rpcProxy.versionRequest(NULL_CONTROLLER,
        VOID_VERSION_REQUEST).getInfo()));
  }

  /**
   * 向NameNode上报损坏块，转换为PB格式RPC调用
   * @param blocks 损坏的块数组
   * @throws IOException RPC调用失败时抛出异常
   */
  @Override
  public void reportBadBlocks(LocatedBlock[] blocks) throws IOException {
    ReportBadBlocksRequestProto.Builder builder = ReportBadBlocksRequestProto
        .newBuilder();
    // 逐个转换损坏块信息
    for (int i = 0