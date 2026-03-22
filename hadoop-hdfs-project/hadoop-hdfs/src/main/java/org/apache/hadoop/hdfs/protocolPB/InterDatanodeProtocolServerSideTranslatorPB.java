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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.protocol.proto.InterDatanodeProtocolProtos.InitReplicaRecoveryRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.InterDatanodeProtocolProtos.InitReplicaRecoveryResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.InterDatanodeProtocolProtos.UpdateReplicaUnderRecoveryRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.InterDatanodeProtocolProtos.UpdateReplicaUnderRecoveryResponseProto;
import org.apache.hadoop.hdfs.server.protocol.BlockRecoveryCommand.RecoveringBlock;
import org.apache.hadoop.hdfs.server.protocol.InterDatanodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.ReplicaRecoveryInfo;

import org.apache.hadoop.thirdparty.protobuf.RpcController;
import org.apache.hadoop.thirdparty.protobuf.ServiceException;

/**
 * 文件说明：DataNode间协议的Protobuf服务端转换器，实现了InterDatanodeProtocolPB接口，
 * 负责将Protobuf格式的RPC请求转换为原生Java对象调用，转发给实际的InterDatanodeProtocol服务实现，
 * 并将返回结果转换回Protobuf格式返回给调用方。
 * 该类用于HDFS DataNode之间数据块恢复流程的RPC通信协议转换。
 */
@InterfaceAudience.Private
public class InterDatanodeProtocolServerSideTranslatorPB implements
    InterDatanodeProtocolPB {
  private final InterDatanodeProtocol impl;

  /**
   * 构造函数，注入实际的DataNode间协议服务实现
   * @param impl 原生InterDatanodeProtocol服务实现实例
   */
  public InterDatanodeProtocolServerSideTranslatorPB(InterDatanodeProtocol impl) {
    this.impl = impl;
  }

  /**
   * 初始化副本恢复请求处理，将Protobuf请求转换后转发给服务实现，返回Protobuf格式响应
   * @param unused RPC控制器，未使用
   * @param request Protobuf格式的初始化副本恢复请求
   * @return Protobuf格式的初始化副本恢复响应
   * @throws ServiceException 服务调用异常，包装底层IO异常
   */
  @Override
  public InitReplicaRecoveryResponseProto initReplicaRecovery(
      RpcController unused, InitReplicaRecoveryRequestProto request)
      throws ServiceException {
    // 将Protobuf块信息转换为原生RecoveringBlock对象
    RecoveringBlock b = PBHelper.convert(request.getBlock());
    ReplicaRecoveryInfo r;
    try {
      // 调用实际服务执行初始化副本恢复
      r = impl.initReplicaRecovery(b);
    } catch (IOException e) {
      // 包装IO异常为Protobuf服务异常抛出
      throw new ServiceException(e);
    }
    
    if (r == null) {
      // 未找到对应副本，构造未找到响应
      return InitReplicaRecoveryResponseProto.newBuilder()
          .setReplicaFound(false)
          .build();
    } else {
      // 找到副本，构造包含恢复信息的响应
      return InitReplicaRecoveryResponseProto.newBuilder()
          .setReplicaFound(true)
          .setBlock(PBHelperClient.convert(r))
          .setState(PBHelper.convert(r.getOriginalReplicaState())).build();
    }
  }

  /**
   * 更新恢复中副本信息请求处理，将Protobuf请求转换后转发给服务实现，返回Protobuf格式响应
   * @param unused RPC控制器，未使用
   * @param request Protobuf格式的更新恢复中副本请求
   * @return Protobuf格式的更新恢复中副本响应
   * @throws ServiceException 服务调用异常，包装底层IO异常
   */
  @Override
  public UpdateReplicaUnderRecoveryResponseProto updateReplicaUnderRecovery(
      RpcController unused, UpdateReplicaUnderRecoveryRequestProto request)
      throws ServiceException {
    final String storageID;
    try {
      // 提取请求参数并转换类型，调用实际服务更新恢复中副本信息
      storageID = impl.updateReplicaUnderRecovery(
          PBHelperClient.convert(request.getBlock()), request.getRecoveryId(),
          request.getNewBlockId(), request.getNewLength());
    } catch (IOException e) {
      // 包装IO异常为Protobuf服务异常抛出
      throw new ServiceException(e);
    }
    // 构造包含存储ID的响应返回
    return UpdateReplicaUnderRecoveryResponseProto.newBuilder()
        .setStorageUuid(storageID).build();
  }
}