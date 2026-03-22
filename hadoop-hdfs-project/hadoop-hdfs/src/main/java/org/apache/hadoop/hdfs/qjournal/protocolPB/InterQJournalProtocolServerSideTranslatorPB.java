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

import org.apache.hadoop.thirdparty.protobuf.RpcController;
import org.apache.hadoop.thirdparty.protobuf.ServiceException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.qjournal.protocol.InterQJournalProtocol;
import org.apache.hadoop.hdfs.protocol.proto.HdfsServerProtos.StorageInfoProto;
import org.apache.hadoop.hdfs.qjournal.protocol.InterQJournalProtocolProtos.GetStorageInfoRequestProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.GetEditLogManifestRequestProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.GetEditLogManifestResponseProto;

import java.io.IOException;

/**
 * @file InterQJournalProtocolServerSideTranslatorPB.java
 * @brief  journal节点间通信协议的Protobuf服务端转码器，将PB序列化请求转发给原始接口实现
 *
 * 类职责：将Protobuf序列化格式的节点间通信请求转换为原生Java对象调用，
 * 再把返回结果转换回Protobuf格式，实现PB协议和原始服务接口之间的适配，
 * 用于QJM（Quorum Journal Manager）节点之间的RPC通信转码。
 */
@InterfaceAudience.Private
public class InterQJournalProtocolServerSideTranslatorPB implements
    InterQJournalProtocolPB{

  /* 代理转发的目标服务实现，即原生接口的实际处理逻辑 */
  private final InterQJournalProtocol impl;

  /**
   * 构造转码器，绑定目标服务实现
   * @param impl 原生节点间日志协议的服务端实现
   */
  public InterQJournalProtocolServerSideTranslatorPB(InterQJournalProtocol
                                                         impl) {
    this.impl = impl;
  }

  /**
   * 获取日志清单请求的PB转码处理，转发请求到原生服务实现
   * @param controller RPC控制器
   * @param request PB格式的请求对象
   * @return PB格式的响应对象
   * @throws ServiceException 服务异常，封装IO异常
   */
  @Override
  public GetEditLogManifestResponseProto getEditLogManifestFromJournal(
      RpcController controller, GetEditLogManifestRequestProto request)
      throws ServiceException {
    try {
      // 从PB请求提取参数，调用原生服务方法并直接返回PB响应
      return impl.getEditLogManifestFromJournal(
          request.getJid().getIdentifier(),
          request.hasNameServiceId() ? request.getNameServiceId() : null,
          request.getSinceTxId(),
          request.getInProgressOk());
    } catch (IOException e) {
      // 将IO异常封装为PB RPC框架要求的ServiceException
      throw new ServiceException(e);
    }
  }

  /**
   * 获取存储信息请求的PB转码处理，转发请求到原生服务实现
   * @param controller RPC控制器
   * @param request PB格式的请求对象
   * @return PB格式的存储信息响应
   * @throws ServiceException 服务异常，封装IO异常
   */
  @Override
  public StorageInfoProto getStorageInfo(
      RpcController controller, GetStorageInfoRequestProto request)
      throws ServiceException {
    try {
      // 从PB请求提取参数，调用原生服务方法并直接返回PB响应
      return impl.getStorageInfo(
          request.getJid().getIdentifier(),
          request.hasNameServiceId() ? request.getNameServiceId() : null
      );
    } catch (IOException e) {
      // 将IO异常封装为PB RPC框架要求的ServiceException
      throw new ServiceException(e);
    }
  }
}