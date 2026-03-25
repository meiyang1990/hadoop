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

import org.apache.hadoop.hdfs.protocol.ReconfigurationProtocol;
import org.apache.hadoop.hdfs.protocol.proto.ReconfigurationProtocolProtos.GetReconfigurationStatusRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ReconfigurationProtocolProtos.GetReconfigurationStatusResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ReconfigurationProtocolProtos.ListReconfigurablePropertiesRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ReconfigurationProtocolProtos.ListReconfigurablePropertiesResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ReconfigurationProtocolProtos.StartReconfigurationRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ReconfigurationProtocolProtos.StartReconfigurationResponseProto;

import org.apache.hadoop.thirdparty.protobuf.RpcController;
import org.apache.hadoop.thirdparty.protobuf.ServiceException;

/**
 * 重新配置协议服务端Protobuf转换器，实现ReconfigurationProtocolPB服务端接口
 * 负责将RPC请求中的Protobuf格式数据转换为HDFS内部使用的原生数据类型，
 * 转发给NameNode/DataNode内部的ReconfigurationProtocol实现处理后，
 * 再将结果转换回Protobuf格式返回给客户端。
 */
public class ReconfigurationProtocolServerSideTranslatorPB implements
    ReconfigurationProtocolPB {

  private final ReconfigurationProtocol impl;

  private static final StartReconfigurationResponseProto START_RECONFIG_RESP =
      StartReconfigurationResponseProto.newBuilder().build();

  /**
   * 构造函数，注入重新配置协议的原生实现
   * @param impl 原生重新配置协议实现（NameNode或DataNode端）
   */
  public ReconfigurationProtocolServerSideTranslatorPB(
      ReconfigurationProtocol impl) {
    this.impl = impl;
  }

  /**
   * 处理启动重新配置的RPC请求，转换格式并转发给内部实现
   * @param controller RPC控制器
   * @param request Protobuf格式的启动重新配置请求
   * @return Protobuf格式的响应
   * @throws ServiceException 服务异常，包装内部IO异常
   */
  @Override
  public StartReconfigurationResponseProto startReconfiguration(
      RpcController controller, StartReconfigurationRequestProto request)
      throws ServiceException {
    try {
      impl.startReconfiguration();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return START_RECONFIG_RESP;
  }

  /**
   * 处理获取可重新配置属性列表的RPC请求，转换格式并转发给内部实现
   * @param controller RPC控制器
   * @param request Protobuf格式的请求
   * @return Protobuf格式的可配置属性列表响应
   * @throws ServiceException 服务异常，包装内部IO异常
   */
  @Override
  public ListReconfigurablePropertiesResponseProto listReconfigurableProperties(
      RpcController controller,
      ListReconfigurablePropertiesRequestProto request)
      throws ServiceException {
    try {
      return ReconfigurationProtocolServerSideUtils
          .listReconfigurableProperties(impl.listReconfigurableProperties());
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  /**
   * 处理获取重新配置状态的RPC请求，转换格式并转发给内部实现
   * @param unused RPC控制器（未使用）
   * @param request Protobuf格式的获取状态请求
   * @return Protobuf格式的重新配置状态响应
   * @throws ServiceException 服务异常，包装内部IO异常
   */
  @Override
  public GetReconfigurationStatusResponseProto getReconfigurationStatus(
      RpcController unused, GetReconfigurationStatusRequestProto request)
      throws ServiceException {
    try {
      return ReconfigurationProtocolServerSideUtils
          .getReconfigurationStatus(impl.getReconfigurationStatus());
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }
}