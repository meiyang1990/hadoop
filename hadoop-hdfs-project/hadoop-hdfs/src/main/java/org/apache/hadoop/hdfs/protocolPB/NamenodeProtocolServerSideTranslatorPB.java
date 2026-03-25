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

import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo.DatanodeInfoBuilder;
import org.apache.hadoop.hdfs.protocol.proto.HdfsServerProtos.VersionRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsServerProtos.VersionResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.NamenodeProtocolProtos.EndCheckpointRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.NamenodeProtocolProtos.EndCheckpointResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.NamenodeProtocolProtos.ErrorReportRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.NamenodeProtocolProtos.ErrorReportResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.NamenodeProtocolProtos.GetBlockKeysRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.NamenodeProtocolProtos.GetBlockKeysResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.NamenodeProtocolProtos.GetBlocksRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.NamenodeProtocolProtos.GetBlocksResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.NamenodeProtocolProtos.GetEditLogManifestRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.NamenodeProtocolProtos.GetEditLogManifestResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.NamenodeProtocolProtos.GetMostRecentCheckpointTxIdRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.NamenodeProtocolProtos.GetMostRecentCheckpointTxIdResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.NamenodeProtocolProtos.GetMostRecentNameNodeFileTxIdRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.NamenodeProtocolProtos.GetMostRecentNameNodeFileTxIdResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.NamenodeProtocolProtos.GetNextSPSPathRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.NamenodeProtocolProtos.GetNextSPSPathResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.NamenodeProtocolProtos.GetTransactionIdRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.NamenodeProtocolProtos.GetTransactionIdResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.NamenodeProtocolProtos.IsRollingUpgradeRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.NamenodeProtocolProtos.IsRollingUpgradeResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.NamenodeProtocolProtos.IsUpgradeFinalizedRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.NamenodeProtocolProtos.IsUpgradeFinalizedResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.NamenodeProtocolProtos.RegisterRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.NamenodeProtocolProtos.RegisterResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.NamenodeProtocolProtos.RollEditLogRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.NamenodeProtocolProtos.RollEditLogResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.NamenodeProtocolProtos.StartCheckpointRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.NamenodeProtocolProtos.StartCheckpointResponseProto;
import org.apache.hadoop.hdfs.security.token.block.ExportedBlockKeys;
import org.apache.hadoop.hdfs.server.namenode.CheckpointSignature;
import org.apache.hadoop.hdfs.server.namenode.NNStorage;
import org.apache.hadoop.hdfs.server.protocol.BlocksWithLocations;
import org.apache.hadoop.hdfs.server.protocol.NamenodeCommand;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.NamenodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.server.protocol.RemoteEditLogManifest;

import org.apache.hadoop.thirdparty.protobuf.RpcController;
import org.apache.hadoop.thirdparty.protobuf.ServiceException;

/**
 * 文件级注释：NamenodeProtocol协议PB服务端转换器，负责将Protobuf序列化的RPC请求转换为原生Java对象，
 * 转发给Active NameNode的NamenodeProtocol服务实现，再将返回结果转换回Protobuf格式返回给客户端。
 * 主要用于SecondaryNameNode、StandbyNameNode等从属节点与主NameNode之间的RPC通信。
 * Implementation for protobuf service that forwards requests
 * received on {@link NamenodeProtocolPB} to the
 * {@link NamenodeProtocol} server implementation.
 */
public class NamenodeProtocolServerSideTranslatorPB implements
    NamenodeProtocolPB {
  /** 底层原生NamenodeProtocol服务实现实例 */
  private final NamenodeProtocol impl;

  /** 空错误报告响应，错误报告不需要返回数据，复用单例 */
  protected final static ErrorReportResponseProto VOID_ERROR_REPORT_RESPONSE =
      ErrorReportResponseProto.newBuilder().build();

  /** 空结束检查点响应，结束检查点不需要返回数据，复用单例 */
  protected final static EndCheckpointResponseProto VOID_END_CHECKPOINT_RESPONSE =
      EndCheckpointResponseProto.newBuilder().build();

  /**
   * 构造函数，传入原生NamenodeProtocol服务实现
   * @param impl 原生NamenodeProtocol服务实例
   */
  public NamenodeProtocolServerSideTranslatorPB(NamenodeProtocol impl) {
    this.impl = impl;
  }

  /**
   * 获取数据节点上满足条件的块信息请求处理
   * @param unused RPC控制器，未使用
   * @param request Protobuf格式的请求
   * @return Protobuf格式的响应
   * @throws ServiceException 服务异常封装
   */
  @Override
  public GetBlocksResponseProto getBlocks(RpcController unused,
      GetBlocksRequestProto request) throws ServiceException {
    // 将Protobuf格式的DataNode信息转换为原生Java对象
    DatanodeInfo dnInfo = new DatanodeInfoBuilder()
        .setNodeID(PBHelperClient.convert(request.getDatanode()))
        .build();
    BlocksWithLocations blocks;
    try {
      // 调用底层服务获取块信息，处理可选存储类型参数
      blocks = impl.getBlocks(dnInfo, request.getSize(),
          request.getMinBlockSize(), request.getTimeInterval(),
          request.hasStorageType() ?
              PBHelperClient.convertStorageType(request.getStorageType()): null);
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    // 将结果转换为Protobuf格式返回
    return GetBlocksResponseProto.newBuilder()
        .setBlocks(PBHelper.convert(blocks)).build();
  }

  /**
   * 获取数据块加密密钥请求处理
   * @param unused RPC控制器，未使用
   * @param request Protobuf格式的请求
   * @return Protobuf格式的响应
   * @throws ServiceException 服务异常封装
   */
  @Override
  public GetBlockKeysResponseProto getBlockKeys(RpcController unused,
      GetBlockKeysRequestProto request) throws ServiceException {
    ExportedBlockKeys keys;
    try {
      // 调用底层服务获取密钥
      keys = impl.getBlockKeys();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    GetBlockKeysResponseProto.Builder builder = 
        GetBlockKeysResponseProto.newBuilder();
    // 密钥不为空时设置到响应中
    if (keys != null) {
      builder.setKeys(PBHelper.convert(keys));
    }
    return builder.build();
  }

  /**
   * 获取当前事务ID请求处理
   * @param unused RPC控制器，未使用
   * @param request Protobuf格式的请求
   * @return Protobuf格式的响应
   * @throws ServiceException 服务异常封装
   */
  @Override
  public GetTransactionIdResponseProto getTransactionId(RpcController unused,
      GetTransactionIdRequestProto request) throws ServiceException {
    long txid;
    try {
      // 调用底层服务获取事务ID
      txid = impl.getTransactionID();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    // 封装为Protobuf响应返回
    return GetTransactionIdResponseProto.newBuilder().setTxId(txid).build();
  }
  
  /**
   * 获取最近一次检查点事务ID请求处理
   * @param unused RPC控制器，未使用
   * @param request Protobuf格式的请求
   * @return Protobuf格式的响应
   * @throws ServiceException 服务异常封装
   */
  @Override
  public GetMostRecentCheckpointTxIdResponseProto getMostRecentCheckpointTxId(
      RpcController unused, GetMostRecentCheckpointTxIdRequestProto request)
      throws ServiceException {
    long txid;
    try {
      // 调用底层服务获取最近检查点事务ID
      txid = impl.getMostRecentCheckpointTxId();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    // 封装为Protobuf响应返回
    return GetMostRecentCheckpointTxIdResponseProto.newBuilder().setTxId(txid).build();
  }

  /**
   * 获取指定NameNode文件最近的事务ID请求处理
   * @param unused RPC控制器，未使用
   * @param request Protobuf格式的请求，包含目标文件类型
   * @return Protobuf格式的响应
   * @throws ServiceException 服务异常封装
   */
  @Override
  public GetMostRecentNameNodeFileTxIdResponseProto getMostRecentNameNodeFileTxId(
      RpcController unused, GetMostRecentNameNodeFileTxIdRequestProto request)
      throws ServiceException {
    long txid;
    try {
      // 将Protobuf文件类型转换为枚举，调用底层服务获取事务ID
      txid = impl.getMostRecentNameNodeFileTxId(
          NNStorage.NameNodeFile.valueOf(request.getNameNodeFile()));
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    // 封装为Protobuf响应返回
    return GetMostRecentNameNodeFileTxIdResponseProto.newBuilder().setTxId(txid).build();
  }


  /**
   * 滚动编辑日志请求处理，用于检查点完成后触发日志滚动
   * @param unused RPC控制器，未使用
   * @param request Protobuf格式的请求
   * @return Protobuf格式的响应，包含检查点签名
   * @throws ServiceException 服务异常封装
   */
  @Override
  public RollEditLogResponseProto rollEditLog(RpcController unused,
      RollEditLogRequestProto request) throws ServiceException {
    CheckpointSignature signature;
    try {
      // 调用底层服务执行日志滚动，获取检查点签名
      signature = impl.rollEditLog();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    // 将签名转换为Protobuf格式返回
    return RollEditLogResponseProto.newBuilder()
        .setSignature(PBHelper.convert(signature)).build();
  }

  /**
   * 错误报告请求处理，从属节点向主节点上报错误
   * @param unused RPC控制器，未使用
   * @param request Protobuf格式的请求，包含错误信息
   * @return 空Protobuf响应
   * @throws ServiceException 服务异常封装
   */
  @Override
  public ErrorReportResponseProto errorReport(RpcController unused,
      ErrorReportRequestProto request) throws ServiceException {
    try {
      // 转换注册信息，调用底层服务处理错误报告
      impl.errorReport(PBHelper.convert(request.getRegistration()),
          request.getErrorCode(), request.getMsg());
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return VOID_ERROR_REPORT_RESPONSE;
  }

  /**
   * 从属NameNode注册请求处理
   * @param unused RPC控制器，未使用
   * @param request Protobuf格式的请求，包含从属节点注册信息
   * @return Protobuf格式的响应，返回主节点分配的注册信息
   * @throws ServiceException 服务异常封装
   */
  @Override
  public RegisterResponseProto registerSubordinateNamenode(
      RpcController unused, RegisterRequestProto request)
      throws ServiceException {
    NamenodeRegistration reg;
    try {
      // 转换请求中的注册信息，调用底层服务完成注册
      reg = impl.registerSubordinateNamenode(
          PBHelper.convert(request.getRegistration()));
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    // 将返回的注册信息转换为Protobuf格式返回
    return RegisterResponseProto.newBuilder()
        .setRegistration(PBHelper.convert(reg)).build();
  }

  /**
   * 启动检查点请求处理
   * @param unused RPC控制器，未使用
   * @param request Protobuf格式的请求，包含从属节点注册信息
   * @return Protobuf格式的响应，返回主节点下发的命令
   * @throws ServiceException 服务异常封装
   */
  @Override
  public StartCheckpointResponseProto startCheckpoint(RpcController unused,
      StartCheckpointRequestProto request) throws ServiceException {
    NamenodeCommand cmd;
    try {
      // 转换注册信息，调用底层服务启动检查点
      cmd = impl.startCheckpoint(PBHelper.convert(request.getRegistration()));
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    // 将命令转换为Protobuf格式返回
    return StartCheckpointResponseProto.newBuilder()
        .setCommand(PBHelper.convert(cmd)).build();
  }

  /**
   * 结束检查点请求处理，检查点完成后通知主节点
   * @param unused RPC控制器，未使用
   * @param request Protobuf格式的请求，包含注册信息和检查点签名
   * @return 空Protobuf响应
   * @throws ServiceException 服务异常封装
   */
  @Override
  public EndCheckpointResponseProto endCheckpoint(RpcController unused,
      EndCheckpointRequestProto request) throws ServiceException {
    try {
      // 转换参数，调用底层服务完成检查点收尾
      impl.endCheckpoint(PBHelper.convert(request.getRegistration()),
          PBHelper.convert(request.getSignature()));
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return VOID_END_CHECKPOINT_RESPONSE;
  }

  /**
   * 获取编辑日志清单请求处理，用于从属节点同步日志
   * @param unused RPC控制器，未使用
   * @param request Protobuf格式的请求，包含起始事务ID
   * @return Protobuf格式的响应，返回编辑日志清单
   * @throws ServiceException 服务异常封装
   */
  @Override
  public GetEditLogManifestResponseProto getEditLogManifest(
      RpcController unused, GetEditLogManifestRequestProto request)
      throws ServiceException {
    RemoteEditLogManifest manifest;
    try {
      // 调用底层服务获取从指定事务ID开始的日志清单
      manifest = impl.getEditLogManifest(request.getSinceTxId());
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    // 转换为Protobuf格式返回
    return GetEditLogManifestResponseProto.newBuilder()
        .setManifest(PBHelper.convert(manifest)).build();
  }

  /**
   * 获取版本信息请求处理，查询命名空间版本信息
   * @param controller RPC控制器
   * @param request Protobuf格式的请求
   * @return Protobuf格式的响应，包含命名空间信息
   * @throws ServiceException 服务异常封装
   */
  @Override
  public VersionResponseProto versionRequest(RpcController controller,
      VersionRequestProto request) throws ServiceException {
    NamespaceInfo info;
    try {
      // 调用底层服务获取命名空间版本信息
      info = impl.versionRequest();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    // 转换为Protobuf格式返回
    return VersionResponseProto.newBuilder()
        .setInfo(PBHelper.convert(info)).build();
  }

  /**
   * 查询升级是否已完成请求处理
   * @param controller RPC控制器
   * @param request Protobuf格式的请求
   * @return Protobuf格式的响应，包含升级完成状态
   * @throws ServiceException 服务异常封装
   */
  @Override
  public IsUpgradeFinalizedResponseProto isUpgradeFinalized(
      RpcController controller, IsUpgradeFinalizedRequestProto request)
      throws ServiceException {
    boolean isUpgradeFinalized;
    try {
      // 调用底层服务查询升级完成状态
      isUpgradeFinalized = impl.isUpgradeFinalized();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    // 封装为Protobuf响应返回
    return IsUpgradeFinalizedResponseProto.newBuilder()
        .setIsUpgradeFinalized(isUpgradeFinalized).build();
  }

  /**
   * 查询是否处于滚动升级状态请求处理
   * @param controller RPC控制器
   * @param request Protobuf格式的请求
   * @return Protobuf格式的响应，包含滚动升级状态
   * @throws ServiceException 服务异常封装
   */
  @Override
  public IsRolling