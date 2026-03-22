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
import org.apache.hadoop.hdfs.protocol.proto.JournalProtocolProtos.FenceRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.JournalProtocolProtos.FenceResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.JournalProtocolProtos.JournalRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.JournalProtocolProtos.StartLogSegmentRequestProto;
import org.apache.hadoop.hdfs.server.protocol.FenceResponse;
import org.apache.hadoop.hdfs.server.protocol.JournalInfo;
import org.apache.hadoop.hdfs.server.protocol.JournalProtocol;
import org.apache.hadoop.ipc.ProtocolMetaInterface;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RpcClientUtil;
import org.apache.hadoop.thirdparty.protobuf.RpcController;

import static org.apache.hadoop.ipc.internal.ShadedProtobufHelper.ipc;

/**
 * 文件说明：HDFS Journal协议客户端PB转换器，负责将本地JournalProtocol接口的请求转换为Protobuf格式
 *           转发给实现了JournalProtocolPB的RPC服务端，完成客户端与服务端之间的协议转换
 * 
 * This class is the client side translator to translate the requests made on
 * {@link JournalProtocol} interfaces to the RPC server implementing
 * {@link JournalProtocolPB}.
 */
@InterfaceAudience.Private
@InterfaceStability.Stable
public class JournalProtocolTranslatorPB implements ProtocolMetaInterface,
    JournalProtocol, Closeable {
  /** RpcController is not used and hence is set to null */
  private final static RpcController NULL_CONTROLLER = null;
  private final JournalProtocolPB rpcProxy;

  /**
   * 构造方法，使用指定的PB协议RPC代理创建转换器
   * @param rpcProxy JournalProtocolPB协议的RPC代理对象
   */
  public JournalProtocolTranslatorPB(JournalProtocolPB rpcProxy) {
    this.rpcProxy = rpcProxy;
  }

  /**
   * 关闭RPC代理，释放连接资源
   */
  @Override
  public void close() {
    RPC.stopProxy(rpcProxy);
  }

  /**
   * 写入日志条目请求，将本地请求转换为PB格式后发送给服务端
   * @param journalInfo 日志信息
   * @param epoch 当前节点周期
   * @param firstTxnId 第一个事务ID
   * @param numTxns 事务数量
   * @param records 事务记录二进制数据
   * @throws IOException 网络或IO异常
   */
  @Override
  public void journal(JournalInfo journalInfo, long epoch, long firstTxnId,
      int numTxns, byte[] records) throws IOException {
    // 构建Protobuf格式请求
    JournalRequestProto req = JournalRequestProto.newBuilder()
        .setJournalInfo(PBHelper.convert(journalInfo))
        .setEpoch(epoch)
        .setFirstTxnId(firstTxnId)
        .setNumTxns(numTxns)
        .setRecords(PBHelperClient.getByteString(records))
        .build();
    // 通过IPC调用远程服务端方法
    ipc(() -> rpcProxy.journal(NULL_CONTROLLER, req));
  }

  /**
   * 围栏请求，隔离旧的JournalNode，保障HA切换一致性，将本地请求转换为PB格式
   * @param journalInfo 日志信息
   * @param epoch 当前新节点周期
   * @param fencerInfo 围栏者信息
   * @return 围栏响应，包含旧周期、最后事务ID和同步状态
   * @throws IOException 网络或IO异常
   */
  @Override
  public FenceResponse fence(JournalInfo journalInfo, long epoch,
      String fencerInfo) throws IOException {
    // 构建Protobuf格式请求
    FenceRequestProto req = FenceRequestProto.newBuilder().setEpoch(epoch)
        .setJournalInfo(PBHelper.convert(journalInfo)).build();
    // 发送请求并获取PB格式响应
    FenceResponseProto resp = ipc(() -> rpcProxy.fence(NULL_CONTROLLER, req));
    // 将PB响应转换为本地对象返回
    return new FenceResponse(resp.getPreviousEpoch(),
        resp.getLastTransactionId(), resp.getInSync());
  }

  /**
   * 启动新日志段请求，将本地请求转换为PB格式后发送给服务端
   * @param journalInfo 日志信息
   * @param epoch 当前节点周期
   * @param txid 日志段起始事务ID
   * @throws IOException 网络或IO异常
   */
  @Override
  public void startLogSegment(JournalInfo journalInfo, long epoch, long txid)
      throws IOException {
    // 构建Protobuf格式请求
    StartLogSegmentRequestProto req = StartLogSegmentRequestProto.newBuilder()
        .setJournalInfo(PBHelper.convert(journalInfo))
        .setEpoch(epoch)
        .setTxid(txid)
        .build();
    // 通过IPC调用远程服务端方法
    ipc(() -> rpcProxy.startLogSegment(NULL_CONTROLLER, req));
  }

  /**
   * 检查远程服务端是否支持指定方法
   * @param methodName 方法名称
   * @return 如果支持返回true，否则返回false
   * @throws IOException 检查过程中发生IO异常
   */
  @Override
  public boolean isMethodSupported(String methodName) throws IOException {
    return RpcClientUtil.isMethodSupported(rpcProxy, JournalProtocolPB.class,
        RPC.RpcKind.RPC_PROTOCOL_BUFFER,
        RPC.getProtocolVersion(JournalProtocolPB.class), methodName);
  }
}