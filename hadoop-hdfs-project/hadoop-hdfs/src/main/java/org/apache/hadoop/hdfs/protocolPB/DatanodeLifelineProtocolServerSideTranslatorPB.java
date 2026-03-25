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
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.HeartbeatRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeLifelineProtocolProtos.LifelineResponseProto;
import org.apache.hadoop.hdfs.server.protocol.DatanodeLifelineProtocol;
import org.apache.hadoop.hdfs.server.protocol.StorageReport;
import org.apache.hadoop.hdfs.server.protocol.VolumeFailureSummary;

import org.apache.hadoop.thirdparty.protobuf.RpcController;
import org.apache.hadoop.thirdparty.protobuf.ServiceException;

/**
 * Datanode生命线协议Protobuf服务端转换器，负责将Protobuf格式的请求转换后转发给
 * 原生DatanodeLifelineProtocol实现，并将结果转换为Protobuf格式返回。
 * 用于支撑DataNode向NameNode发送保活生命线消息的RPC通信转换。
 */
@InterfaceAudience.Private
public class DatanodeLifelineProtocolServerSideTranslatorPB implements
    DatanodeLifelineProtocolPB {

  // 空生命线响应实例，无返回内容时复用
  private static final LifelineResponseProto VOID_LIFELINE_RESPONSE_PROTO =
      LifelineResponseProto.newBuilder().build();

  // 原生协议实现实例
  private final DatanodeLifelineProtocol impl;

  /**
   * 构造函数，传入原生协议实现实例
   * @param impl 原生Datanode生命线协议实现
   */
  public DatanodeLifelineProtocolServerSideTranslatorPB(
      DatanodeLifelineProtocol impl) {
    this.impl = impl;
  }

  /**
   * 处理Protobuf格式的发送生命线请求，转换后调用原生实现
   * @param controller RPC控制器
   * @param request Protobuf格式的心跳请求（生命线请求复用心跳请求结构）
   * @return Protobuf格式的空响应
   * @throws ServiceException 服务异常，包装内部IO异常
   */
  @Override
  public LifelineResponseProto sendLifeline(RpcController controller,
      HeartbeatRequestProto request) throws ServiceException {
    try {
      // 将Protobuf格式存储报告转换为原生对象
      final StorageReport[] report = PBHelperClient.convertStorageReports(
          request.getReportsList());
      // 转换卷故障摘要信息
      VolumeFailureSummary volumeFailureSummary =
          request.hasVolumeFailureSummary() ?
              PBHelper.convertVolumeFailureSummary(
                  request.getVolumeFailureSummary()) : null;
      // 调用原生协议实现处理生命线请求
      impl.sendLifeline(PBHelper.convert(request.getRegistration()), report,
          request.getCacheCapacity(), request.getCacheUsed(),
          request.getXmitsInProgress(), request.getXceiverCount(),
          request.getFailedVolumes(), volumeFailureSummary);
      // 返回空响应
      return VOID_LIFELINE_RESPONSE_PROTO;
    } catch (IOException e) {
      // 包装IO异常为RPC服务异常
      throw new ServiceException(e);
    }
  }
}