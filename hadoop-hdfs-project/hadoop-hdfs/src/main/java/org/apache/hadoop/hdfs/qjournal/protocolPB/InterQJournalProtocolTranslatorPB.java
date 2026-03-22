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

import org.apache.hadoop.hdfs.protocol.proto.HdfsServerProtos.StorageInfoProto;
import org.apache.hadoop.hdfs.qjournal.protocol.InterQJournalProtocolProtos;
import org.apache.hadoop.thirdparty.protobuf.RpcController;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.qjournal.protocol.InterQJournalProtocol;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.GetEditLogManifestRequestProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.GetEditLogManifestResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos;
import org.apache.hadoop.ipc.ProtocolMetaInterface;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RpcClientUtil;

import java.io.Closeable;
import java.io.IOException;

import static org.apache.hadoop.ipc.internal.ShadedProtobufHelper.ipc;

/**
 * 文件说明：HDFS QJournal节点间通信协议PB版本翻译器，位于hadoop-hdfs模块，负责将本地
 * InterQJournalProtocol接口调用转换为Protobuf序列化格式的RPC请求，供节点间Journal数据同步使用。
 * <p>
 * 该类是客户端侧协议转换器，将InterQJournalProtocol接口的请求转换为Protobuf格式，
 * 转发给实现了InterQJournalProtocolPB的RPC服务端，实现共享编辑日志节点之间的元数据同步。
 */
@InterfaceAudience.Private
@InterfaceStability.Stable
public class InterQJournalProtocolTranslatorPB implements ProtocolMetaInterface,
    InterQJournalProtocol, Closeable {

  /* RpcController未使用，固定为null */
  private final static RpcController NULL_CONTROLLER = null;
  // Protobuf版本RPC协议代理对象
  private final InterQJournalProtocolPB rpcProxy;

  /**
   * 构造函数，使用给定的RPC代理创建转换器
   * @param rpcProxy Protobuf版本节点间通信协议RPC代理
   */
  public InterQJournalProtocolTranslatorPB(InterQJournalProtocolPB rpcProxy) {
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
   * 从远程Journal节点获取编辑日志清单，用于日志同步
   * @param jid 日志ID
   * @param nameServiceId 命名服务ID
   * @param sinceTxId 起始事务ID，仅返回该事务之后的日志
   * @param inProgressOk 是否允许返回未完成的正在写入的日志
   * @return 编辑日志清单响应对象，包含日志文件信息
   * @throws IOException RPC调用异常
   */
  @Override
  public GetEditLogManifestResponseProto getEditLogManifestFromJournal(
      String jid, String nameServiceId, long sinceTxId, boolean inProgressOk)
      throws IOException {
    GetEditLogManifestRequestProto.Builder req;
    // 构建Protobuf请求对象
    req = GetEditLogManifestRequestProto.newBuilder()
        .setJid(convertJournalId(jid))
        .setSinceTxId(sinceTxId)
        .setInProgressOk(inProgressOk);
    // 设置可选参数命名服务ID
    if (nameServiceId !=null) {
      req.setNameServiceId(nameServiceId);
    }
    // 执行RPC调用，返回响应
    return ipc(() -> rpcProxy.getEditLogManifestFromJournal(NULL_CONTROLLER,
        req.build()));
  }

  /**
   * 从远程Journal节点获取存储信息，用于节点状态校验
   * @param jid 日志ID
   * @param nameServiceId 命名服务ID
   * @return 存储信息Protobuf对象
   * @throws IOException RPC调用异常
   */
  @Override
  public StorageInfoProto getStorageInfo(String jid, String nameServiceId)
      throws IOException {
    InterQJournalProtocolProtos.GetStorageInfoRequestProto.Builder req =
        InterQJournalProtocolProtos.GetStorageInfoRequestProto.newBuilder()
            .setJid(convertJournalId(jid));
    // 设置可选参数命名服务ID
    if (nameServiceId != null) {
      req.setNameServiceId(nameServiceId);
    }
    // 执行RPC调用，返回响应
    return ipc(() -> rpcProxy.getStorageInfo(NULL_CONTROLLER, req.build()));
  }

  /**
   * 将字符串格式JournalID转换为Protobuf格式的JournalID对象
   * @param jid 字符串格式日志ID
   * @return Protobuf格式JournalID对象
   */
  private QJournalProtocolProtos.JournalIdProto convertJournalId(String jid) {
    return QJournalProtocolProtos.JournalIdProto.newBuilder()
        .setIdentifier(jid)
        .build();
  }

  /**
   * 检查远程RPC服务端是否支持指定方法
   * @param methodName 方法名称
   * @return 支持返回true，否则返回false
   * @throws IOException 检查过程中的IO异常
   */
  @Override
  public boolean isMethodSupported(String methodName) throws IOException {
    return RpcClientUtil.isMethodSupported(rpcProxy,
        InterQJournalProtocolPB.class, RPC.RpcKind.RPC_PROTOCOL_BUFFER,
        RPC.getProtocolVersion(InterQJournalProtocolPB.class), methodName);
  }
}