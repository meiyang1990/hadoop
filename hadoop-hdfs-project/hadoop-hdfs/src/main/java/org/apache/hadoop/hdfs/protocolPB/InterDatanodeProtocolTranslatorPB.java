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

import javax.net.SocketFactory;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.BlockProto;
import org.apache.hadoop.hdfs.protocol.proto.InterDatanodeProtocolProtos.InitReplicaRecoveryRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.InterDatanodeProtocolProtos.InitReplicaRecoveryResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.InterDatanodeProtocolProtos.UpdateReplicaUnderRecoveryRequestProto;
import org.apache.hadoop.hdfs.server.protocol.BlockRecoveryCommand.RecoveringBlock;
import org.apache.hadoop.hdfs.server.protocol.InterDatanodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.ReplicaRecoveryInfo;
import org.apache.hadoop.ipc.ProtobufRpcEngine2;
import org.apache.hadoop.ipc.ProtocolMetaInterface;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RpcClientUtil;
import org.apache.hadoop.security.UserGroupInformation;

import org.apache.hadoop.thirdparty.protobuf.RpcController;

import static org.apache.hadoop.ipc.internal.ShadedProtobufHelper.ipc;

/**
 * 文件说明：数据节点间协议的Protobuf转换客户端
 * 
 * 核心职责：将原生{@link InterDatanodeProtocol}接口调用转换为基于Protobuf序列化的RPC请求，
 * 转发给实现了{@link InterDatanodeProtocolPB}的远端数据节点RPC服务端，完成数据节点间的通信。
 * 主要用于HDFS数据块恢复流程中，数据节点之间的交互。
 */
@InterfaceAudience.Private
@InterfaceStability.Stable
public class InterDatanodeProtocolTranslatorPB implements
    ProtocolMetaInterface, InterDatanodeProtocol, Closeable {
  /** RpcController is not used and hence is set to null */
  private final static RpcController NULL_CONTROLLER = null;
  final private InterDatanodeProtocolPB rpcProxy;

  /**
   * 构造方法：创建数据节点间协议Protobuf转换客户端，建立RPC代理连接
   * @param addr 远端数据节点RPC服务地址
   * @param ugi 用户身份信息
   * @param conf Hadoop配置对象
   * @param factory Socket工厂用于创建网络连接
   * @param socketTimeout Socket超时时间
   * @throws IOException 创建RPC连接失败时抛出异常
   */
  public InterDatanodeProtocolTranslatorPB(InetSocketAddress addr,
      UserGroupInformation ugi, Configuration conf, SocketFactory factory,
      int socketTimeout)
      throws IOException {
    RPC.setProtocolEngine(conf, InterDatanodeProtocolPB.class,
        ProtobufRpcEngine2.class);
    rpcProxy = RPC.getProxy(InterDatanodeProtocolPB.class,
        RPC.getProtocolVersion(InterDatanodeProtocolPB.class), addr, ugi, conf,
        factory, socketTimeout);
  }

  /**
   * 关闭RPC代理连接，释放资源
   */
  @Override
  public void close() {
    RPC.stopProxy(rpcProxy);
  }

  /**
   * 初始化副本恢复流程：向目标数据节点发起副本恢复请求，获取副本恢复信息
   * @param rBlock 待恢复的块信息
   * @return 远端节点上的副本恢复信息，如果未找到副本则返回null
   * @throws IOException RPC调用或响应解析错误时抛出异常
   */
  @Override
  public ReplicaRecoveryInfo initReplicaRecovery(RecoveringBlock rBlock)
      throws IOException {
    // 构造Protobuf格式请求，转换块对象
    InitReplicaRecoveryRequestProto req = InitReplicaRecoveryRequestProto
        .newBuilder().setBlock(PBHelper.convert(rBlock)).build();
    InitReplicaRecoveryResponseProto resp;
    // 发起RPC调用获取响应
    resp = ipc(() -> rpcProxy.initReplicaRecovery(NULL_CONTROLLER, req));
    // 远端节点未找到对应副本，返回null
    if (!resp.getReplicaFound()) {
      // No replica found on the remote node.
      return null;
    } else {
      // 响应校验：检查必填字段是否存在
      if (!resp.hasBlock() || !resp.hasState()) {
        throw new IOException("Replica was found but missing fields. " +
            "Req: " + req + "\n" +
            "Resp: " + resp);
      }
    }
    
    // 解析Protobuf响应，转换为原生ReplicaRecoveryInfo对象返回
    BlockProto b = resp.getBlock();
    return new ReplicaRecoveryInfo(b.getBlockId(), b.getNumBytes(),
        b.getGenStamp(), PBHelper.convert(resp.getState()));
  }

  /**
   * 更新恢复中的副本信息：在块恢复完成后，更新目标节点上副本的状态和元数据
   * @param oldBlock 恢复前的原有块
   * @param recoveryId 恢复操作ID
   * @param newBlockId 恢复后的新块ID
   * @param newLength 恢复后的新块长度
   * @return 目标节点存储的UUID
   * @throws IOException RPC调用错误时抛出异常
   */
  @Override
  public String updateReplicaUnderRecovery(ExtendedBlock oldBlock,
      long recoveryId, long newBlockId, long newLength) throws IOException {
    // 构造Protobuf格式请求，转换块对象并填充参数
    UpdateReplicaUnderRecoveryRequestProto req = 
        UpdateReplicaUnderRecoveryRequestProto.newBuilder()
        .setBlock(PBHelperClient.convert(oldBlock))
        .setNewLength(newLength).setNewBlockId(newBlockId)
        .setRecoveryId(recoveryId).build();
    // 发起RPC调用，返回存储UUID
    return ipc(() -> rpcProxy.updateReplicaUnderRecovery(NULL_CONTROLLER, req)
        .getStorageUuid());

  }

  /**
   * 检查RPC服务端是否支持指定方法
   * @param methodName 方法名称
   * @return 如果支持返回true，否则返回false
   * @throws IOException 检测过程中IO错误时抛出异常
   */
  @Override
  public boolean isMethodSupported(String methodName) throws IOException {
    return RpcClientUtil.isMethodSupported(rpcProxy,
        InterDatanodeProtocolPB.class, RPC.RpcKind.RPC_PROTOCOL_BUFFER,
        RPC.getProtocolVersion(InterDatanodeProtocolPB.class), methodName);
  }
}