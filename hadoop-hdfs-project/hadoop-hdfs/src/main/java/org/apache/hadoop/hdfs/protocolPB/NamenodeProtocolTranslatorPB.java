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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.proto.HdfsServerProtos.NamenodeCommandProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsServerProtos.VersionRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.NamenodeProtocolProtos.EndCheckpointRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.NamenodeProtocolProtos.ErrorReportRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.NamenodeProtocolProtos.GetBlockKeysRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.NamenodeProtocolProtos.GetBlockKeysResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.NamenodeProtocolProtos.GetBlocksRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.NamenodeProtocolProtos.GetEditLogManifestRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.NamenodeProtocolProtos.GetMostRecentCheckpointTxIdRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.NamenodeProtocolProtos.GetMostRecentNameNodeFileTxIdRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.NamenodeProtocolProtos.GetNextSPSPathRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.NamenodeProtocolProtos.GetNextSPSPathResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.NamenodeProtocolProtos.GetTransactionIdRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.NamenodeProtocolProtos.IsRollingUpgradeRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.NamenodeProtocolProtos.IsRollingUpgradeResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.NamenodeProtocolProtos.IsUpgradeFinalizedRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.NamenodeProtocolProtos.IsUpgradeFinalizedResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.NamenodeProtocolProtos.RegisterRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.NamenodeProtocolProtos.RollEditLogRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.NamenodeProtocolProtos.StartCheckpointRequestProto;
import org.apache.hadoop.hdfs.security.token.block.ExportedBlockKeys;
import org.apache.hadoop.hdfs.server.namenode.CheckpointSignature;
import org.apache.hadoop.hdfs.server.namenode.NNStorage;
import org.apache.hadoop.hdfs.server.protocol.BlocksWithLocations;
import org.apache.hadoop.hdfs.server.protocol.NamenodeCommand;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.NamenodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.server.protocol.RemoteEditLogManifest;
import org.apache.hadoop.ipc.ProtocolMetaInterface;
import org.apache.hadoop.ipc.ProtocolTranslator;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RpcClientUtil;
import org.apache.hadoop.thirdparty.protobuf.RpcController;

import static org.apache.hadoop.ipc.internal.ShadedProtobufHelper.ipc;

/**
 * 文件：NamenodeProtocolTranslatorPB.java
 * 所属模块：HDFS 核心服务端
 * 核心职责：实现NamenodeProtocol接口到Protobuf RPC协议的转换，作为客户端侧代理，将Java对象请求转换为Protobuf格式发送给服务端，并将Protobuf响应转换回Java对象返回
 * 功能说明：主要用于SecondaryNameNode与ActiveNameNode之间通信的协议转换，封装了PB序列化/反序列化逻辑，对上层调用隐藏PB细节
 */
/**
 * This class is the client side translator to translate the requests made on
 * {@link NamenodeProtocol} interfaces to the RPC server implementing
 * {@link NamenodeProtocolPB}.
 */
@InterfaceAudience.Private
@InterfaceStability.Stable
public class NamenodeProtocolTranslatorPB implements NamenodeProtocol,
    ProtocolMetaInterface, Closeable, ProtocolTranslator {
  /** RpcController is not used and hence is set to null */
  private final static RpcController NULL_CONTROLLER = null;
  
  /*
   * Protobuf requests with no parameters instantiated only once
   */
  // 无参请求对象单例，复用避免重复创建
  private static final GetBlockKeysRequestProto VOID_GET_BLOCKKEYS_REQUEST = 
      GetBlockKeysRequestProto.newBuilder().build();
  private static final GetTransactionIdRequestProto VOID_GET_TRANSACTIONID_REQUEST = 
      GetTransactionIdRequestProto.newBuilder().build();
  private static final RollEditLogRequestProto VOID_ROLL_EDITLOG_REQUEST = 
      RollEditLogRequestProto.newBuilder().build();
  private static final VersionRequestProto VOID_VERSION_REQUEST = 
      VersionRequestProto.newBuilder().build();

  // Protobuf RPC代理对象
  final private NamenodeProtocolPB rpcProxy;

  /**
   * 构造函数，初始化协议转换器
   * @param rpcProxy Protobuf RPC服务代理
   */
  public NamenodeProtocolTranslatorPB(NamenodeProtocolPB rpcProxy) {
    this.rpcProxy = rpcProxy;
  }

  @Override
  public void close() {
    // 停止RPC代理，释放资源
    RPC.stopProxy(rpcProxy);
  }

  @Override
  public Object getUnderlyingProxyObject() {
    // 返回底层RPC代理对象
    return rpcProxy;
  }

  /**
   * 获取指定数据节点上满足条件的数据块信息，用于平衡数据块分布
   * @param datanode 目标数据节点信息
   * @param size 需要获取的块总大小
   * @param minBlockSize 最小块大小过滤条件
   * @param timeInterval 时间间隔过滤条件
   * @param storageType 存储类型过滤条件
   * @return 符合条件的块及其位置信息
   * @throws IOException 调用RPC时发生IO异常
   */
  @Override
  public BlocksWithLocations getBlocks(DatanodeInfo datanode, long size, long
      minBlockSize, long timeInterval, StorageType storageType)
      throws IOException {
    // 构造PB请求对象
    GetBlocksRequestProto.Builder builder = GetBlocksRequestProto.newBuilder()
        .setDatanode(PBHelperClient.convert((DatanodeID)datanode)).setSize(size)
        .setMinBlockSize(minBlockSize).setTimeInterval(timeInterval);
    // 如果指定了存储类型，添加到请求中
    if (storageType != null) {
      builder.setStorageType(PBHelperClient.convertStorageType(storageType));
    }
    GetBlocksRequestProto req = builder.build();
    // 调用RPC并转换响应结果
    return PBHelper.convert(ipc(() -> rpcProxy.getBlocks(NULL_CONTROLLER, req)
        .getBlocks()));
  }

  /**
   * 获取当前活跃NameNode的数据块密钥，用于SecondaryNameNode同步
   * @return 导出的块密钥对象
   * @throws IOException 调用RPC时发生IO异常
   */
  @Override
  public ExportedBlockKeys getBlockKeys() throws IOException {
    GetBlockKeysResponseProto rsp = ipc(() -> rpcProxy.getBlockKeys(NULL_CONTROLLER,
        VOID_GET_BLOCKKEYS_REQUEST));
    // 如果响应包含密钥则转换返回，否则返回null
    return rsp.hasKeys() ? PBHelper.convert(rsp.getKeys()) : null;
  }

  /**
   * 获取当前NameNode的最新事务ID
   * @return 最新事务ID
   * @throws IOException 调用RPC时发生IO异常
   */
  @Override
  public long getTransactionID() throws IOException {
    return ipc(() -> rpcProxy.getTransactionId(NULL_CONTROLLER,
        VOID_GET_TRANSACTIONID_REQUEST).getTxId());
  }

  /**
   * 获取最近一次检查点的事务ID
   * @return 最近检查点事务ID
   * @throws IOException 调用RPC时发生IO异常
   */
  @Override
  public long getMostRecentCheckpointTxId() throws IOException {
    return ipc(() -> rpcProxy.getMostRecentCheckpointTxId(NULL_CONTROLLER,
        GetMostRecentCheckpointTxIdRequestProto.getDefaultInstance()).getTxId());
  }

  /**
   * 获取指定NameNode文件的最新事务ID
   * @param nnf 目标NameNode文件类型
   * @return 指定文件的最新事务ID
   * @throws IOException 调用RPC时发生IO异常
   */
  @Override
  public long getMostRecentNameNodeFileTxId(NNStorage.NameNodeFile nnf) throws IOException {
    return ipc(() -> rpcProxy.getMostRecentNameNodeFileTxId(NULL_CONTROLLER,
        GetMostRecentNameNodeFileTxIdRequestProto.newBuilder()
            .setNameNodeFile(nnf.toString()).build()).getTxId());

  }

  /**
   * 滚动编辑日志，生成新的编辑日志段
   * @return 检查点签名，包含检查点元信息
   * @throws IOException 调用RPC时发生IO异常
   */
  @Override
  public CheckpointSignature rollEditLog() throws IOException {
    return PBHelper.convert(ipc(() -> rpcProxy.rollEditLog(NULL_CONTROLLER,
        VOID_ROLL_EDITLOG_REQUEST).getSignature()));
  }

  /**
   * 请求获取NameNode命名空间信息，用于版本和信息校验
   * @return 命名空间信息对象
   * @throws IOException 调用RPC时发生IO异常
   */
  @Override
  public NamespaceInfo versionRequest() throws IOException {
    return PBHelper.convert(ipc(() -> rpcProxy.versionRequest(NULL_CONTROLLER,
        VOID_VERSION_REQUEST).getInfo()));
  }

  /**
   * 向NameNode上报错误信息
   * @param registration 从属NameNode注册信息
   * @param errorCode 错误码
   * @param msg 错误消息
   * @throws IOException 调用RPC时发生IO异常
   */
  @Override
  public void errorReport(NamenodeRegistration registration, int errorCode,
      String msg) throws IOException {
    // 构造PB请求
    ErrorReportRequestProto req = ErrorReportRequestProto.newBuilder()
        .setErrorCode(errorCode).setMsg(msg)
        .setRegistration(PBHelper.convert(registration)).build();
    // 调用RPC
    ipc(() -> rpcProxy.errorReport(NULL_CONTROLLER, req));
  }

  /**
   * 向活跃NameNode注册从属NameNode（如SecondaryNameNode）
   * @param registration 从属NameNode注册信息
   * @return 注册后的NameNode注册信息
   * @throws IOException 调用RPC时发生IO异常
   */
  @Override
  public NamenodeRegistration registerSubordinateNamenode(
      NamenodeRegistration registration) throws IOException {
    RegisterRequestProto req = RegisterRequestProto.newBuilder()
        .setRegistration(PBHelper.convert(registration)).build();
    // 调用RPC并转换返回结果
    return PBHelper.convert(
        ipc(() -> rpcProxy.registerSubordinateNamenode(NULL_CONTROLLER, req)
            .getRegistration()));
  }

  /**
   * 请求活跃NameNode开始检查点流程
   * @param registration 从属NameNode注册信息
   * @return NameNode返回的检查点命令
   * @throws IOException 调用RPC时发生IO异常
   */
  @Override
  public NamenodeCommand startCheckpoint(NamenodeRegistration registration)
      throws IOException {
    StartCheckpointRequestProto req = StartCheckpointRequestProto.newBuilder()
        .setRegistration(PBHelper.convert(registration)).build();
    NamenodeCommandProto cmd;
    // 调用RPC获取命令
    cmd = ipc(() -> rpcProxy.startCheckpoint(NULL_CONTROLLER, req).getCommand());
    // 转换为Java对象返回
    return PBHelper.convert(cmd);
  }

  /**
   * 通知活跃NameNode检查点已经完成
   * @param registration 从属NameNode注册信息
   * @param sig 检查点签名
   * @throws IOException 调用RPC时发生IO异常
   */
  @Override
  public void endCheckpoint(NamenodeRegistration registration,
      CheckpointSignature sig) throws IOException {
    EndCheckpointRequestProto req = EndCheckpointRequestProto.newBuilder()
        .setRegistration(PBHelper.convert(registration))
        .setSignature(PBHelper.convert(sig)).build();
    // 调用RPC完成通知
    ipc(() -> rpcProxy.endCheckpoint(NULL_CONTROLLER, req));
  }

  /**
   * 获取从指定事务ID之后的编辑日志清单
   * @param sinceTxId 起始事务ID
   * @return 远程编辑日志清单
   * @throws IOException 调用RPC时发生IO异常
   */
  @Override
  public RemoteEditLogManifest getEditLogManifest(long sinceTxId)
      throws IOException {
    GetEditLogManifestRequestProto req = GetEditLogManifestRequestProto
        .newBuilder().setSinceTxId(sinceTxId).build();
    // 调用RPC并转换结果
    return PBHelper.convert(ipc(() -> rpcProxy.getEditLogManifest(NULL_CONTROLLER, req)
        .getManifest()));
  }

  /**
   * 检查RPC服务端是否支持指定方法
   * @param methodName 方法名称
   * @return 如果支持返回true，否则返回false
   * @throws IOException 检查过程中发生IO异常
   */
  @Override
  public boolean isMethodSupported(String methodName) throws IOException {
    // 使用RPC工具类检查方法是否支持
    return RpcClientUtil.isMethodSupported(rpcProxy, NamenodeProtocolPB.class,
        RPC.RpcKind.RPC_PROTOCOL_BUFFER,
        RPC.getProtocolVersion(NamenodeProtocolPB.class), methodName);
  }

  /**
   * 检查HDFS升级是否已经完成
   * @return 如果升级已完成返回true，否则返回false
   * @throws IOException 调用RPC时发生IO异常
   */
  @Override
  public boolean isUpgradeFinalized() throws IOException {
    IsUpgradeFinalizedRequestProto req = IsUpgradeFinalizedRequestProto
        .newBuilder().build();
    IsUpgradeFinalizedResponseProto response = ipc(() -> rpcProxy.isUpgradeFinalized(
        NULL_CONTROLLER, req));
    return response.getIsUpgradeFinalized();
  }

  /**
   * 检查当前是否处于滚动升级状态
   * @return 如果处于滚动升级返回true，否则返回false
   * @throws IOException 调用RPC时发生IO异常
   */
  @Override
  public boolean isRollingUpgrade() throws IOException {
    IsRollingUpgradeRequestProto req = IsRollingUpgradeRequestProto
        .newBuilder().build();
    IsRollingUpgradeResponseProto response = ipc(() -> rpcProxy.isRollingUpgrade(
        NULL_CONTROLLER, req));
    return response.getIsRollingUpgrade();
  }

  /**
   * 获取下一个存储策略满足器（SPS）需要处理的路径ID
   * @return 下一个需要处理的路径ID，如果没有待处理路径返回null
   * @throws IOException 调用RPC时发生IO异常
   */
  @Override
  public Long getNextSPSPath() throws IOException {
    GetNextSPSPathRequestProto req =
        GetNextSPSPathRequestProto.newBuilder().build();
    GetNextSPSPathResponseProto nextSPSPath =
        ipc(() -> rpcProxy.getNextSPSPath(NULL_CONTROLLER, req));
    return nextSPSPath.hasSpsPath() ? nextSPSPath.getSpsPath() : null;
  }
}