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
package org.apache.hadoop.yarn.server.nodemanager.api.impl.pb.service;

import java.io.IOException;

import org.apache.hadoop.yarn.server.nodemanager.api.protocolrecords.impl.pb.LocalizerHeartbeatResponsePBImpl;
import org.apache.hadoop.yarn.server.nodemanager.api.protocolrecords.impl.pb.LocalizerStatusPBImpl;

import org.apache.hadoop.thirdparty.protobuf.RpcController;
import org.apache.hadoop.thirdparty.protobuf.ServiceException;

import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.proto.YarnServerNodemanagerServiceProtos.LocalizerHeartbeatResponseProto;
import org.apache.hadoop.yarn.proto.YarnServerNodemanagerServiceProtos.LocalizerStatusProto;
import org.apache.hadoop.yarn.server.nodemanager.api.LocalizationProtocol;
import org.apache.hadoop.yarn.server.nodemanager.api.LocalizationProtocolPB;
import org.apache.hadoop.yarn.server.nodemanager.api.protocolrecords.LocalizerHeartbeatResponse;

/**
 * 本地化协议Protobuf服务实现类，负责处理本地化器心跳RPC请求，完成PB格式转换与实际业务逻辑转发
 */
public class LocalizationProtocolPBServiceImpl implements LocalizationProtocolPB {

  // 持有实际业务逻辑处理对象
  private LocalizationProtocol real;
  
  /**
   * 构造函数，注入实际业务处理接口
   * @param impl 实际本地化协议业务逻辑实现
   */
  public LocalizationProtocolPBServiceImpl(LocalizationProtocol impl) {
    this.real = impl;
  }
  
  /**
   * 处理本地化器心跳请求，完成PB格式转换并转发给实际业务逻辑
   * @param controller RPC控制器
   * @param proto Protobuf格式的本地化器状态请求
   * @return Protobuf格式的心跳响应
   * @throws ServiceException 服务异常包装
   */
  @Override
  public LocalizerHeartbeatResponseProto heartbeat(RpcController controller,
      LocalizerStatusProto proto) throws ServiceException {
    // 将Protobuf请求转换为内部业务对象
    LocalizerStatusPBImpl request = new LocalizerStatusPBImpl(proto);
    try {
      // 转发请求给实际业务处理
      LocalizerHeartbeatResponse response = real.heartbeat(request);
      // 将内部响应对象转换为Protobuf格式返回
      return ((LocalizerHeartbeatResponsePBImpl)response).getProto();
    } catch (YarnException e) {
      // 包装Yarn异常为RPC服务异常
      throw new ServiceException(e);
    } catch (IOException e) {
      // 包装IO异常为RPC服务异常
      throw new ServiceException(e);
    }
  }

}