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
import org.apache.hadoop.hdfs.protocol.proto.JournalProtocolProtos.FenceRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.JournalProtocolProtos.FenceResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.JournalProtocolProtos.JournalRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.JournalProtocolProtos.JournalResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.JournalProtocolProtos.StartLogSegmentRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.JournalProtocolProtos.StartLogSegmentResponseProto;
import org.apache.hadoop.hdfs.server.protocol.FenceResponse;
import org.apache.hadoop.hdfs.server.protocol.JournalProtocol;

import org.apache.hadoop.thirdparty.protobuf.RpcController;
import org.apache.hadoop.thirdparty.protobuf.ServiceException;

/**
 * 文件说明：Journal协议服务端Protobuf协议转换器，将Protobuf序列化的请求转换为原生对象调用
 *            实现了JournalProtocolPB接口，负责把RPC请求转发给底层原生JournalProtocol服务实现
 * 核心职责：处理HDFS QJM（共享存储日志）的RPC请求协议转换，完成Protobuf消息到服务端原生API的适配
 */
@InterfaceAudience.Private
public class JournalProtocolServerSideTranslatorPB implements JournalProtocolPB {
  /** 被代理的原生Journal协议服务端实现，所有请求转发到该实例处理 */
  private final JournalProtocol impl;

  // 空的日志响应对象，RPC不需要返回业务数据时直接返回该实例
  private final static JournalResponseProto VOID_JOURNAL_RESPONSE = 
  JournalResponseProto.newBuilder().build();

  // 空的启动日志段响应对象，RPC不需要返回业务数据时直接返回该实例
  private final static StartLogSegmentResponseProto
  VOID_START_LOG_SEGMENT_RESPONSE =
      StartLogSegmentResponseProto.newBuilder().build();

  /**
   * 构造方法，创建协议转换器，绑定底层原生服务实现
   * @param impl 原生Journal协议服务端实现实例
   */
  public JournalProtocolServerSideTranslatorPB(JournalProtocol impl) {
    this.impl = impl;
  }

  /**
   * 处理写入编辑日志请求，完成Protobuf请求转换并转发给原生服务
   * @see JournalProtocol#journal
   * @param unused RPC控制器，此处未使用
   * @param req Protobuf格式的写入请求
   * @return 空响应对象
   * @throws ServiceException 服务异常，封装IO异常
   */
  @Override
  public JournalResponseProto journal(RpcController unused,
      JournalRequestProto req) throws ServiceException {
    try {
      // 转换Protobuf消息为原生对象，调用原生服务写入日志
      impl.journal(PBHelper.convert(req.getJournalInfo()), req.getEpoch(),
          req.getFirstTxnId(), req.getNumTxns(), req.getRecords().toByteArray());
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return VOID_JOURNAL_RESPONSE;
  }

  /**
   * 处理启动新日志段请求，完成Protobuf请求转换并转发给原生服务
   * @see JournalProtocol#startLogSegment
   * @param controller RPC控制器
   * @param req Protobuf格式的启动请求
   * @return 空响应对象
   * @throws ServiceException 服务异常，封装IO异常
   */
  @Override
  public StartLogSegmentResponseProto startLogSegment(RpcController controller,
      StartLogSegmentRequestProto req) throws ServiceException {
    try {
      // 转换Protobuf消息为原生对象，调用原生服务启动日志段
      impl.startLogSegment(PBHelper.convert(req.getJournalInfo()),
          req.getEpoch(), req.getTxid());
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return VOID_START_LOG_SEGMENT_RESPONSE;
  }

  /**
   * 处理围栏请求，隔离旧的JournalNode，完成Protobuf请求响应转换
   * @param controller RPC控制器
   * @param req Protobuf格式的围栏请求
   * @return Protobuf格式的围栏响应
   * @throws ServiceException 服务异常，封装IO异常
   */
  @Override
  public FenceResponseProto fence(RpcController controller,
      FenceRequestProto req) throws ServiceException {
    try {
      // 转换请求并调用原生围栏方法，将原生响应转换为Protobuf格式返回
      FenceResponse resp = impl.fence(PBHelper.convert(req.getJournalInfo()), req.getEpoch(),
          req.getFencerInfo());
      return FenceResponseProto.newBuilder().setInSync(resp.isInSync())
          .setLastTransactionId(resp.getLastTransactionId())
          .setPreviousEpoch(resp.getPreviousEpoch()).build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }
}