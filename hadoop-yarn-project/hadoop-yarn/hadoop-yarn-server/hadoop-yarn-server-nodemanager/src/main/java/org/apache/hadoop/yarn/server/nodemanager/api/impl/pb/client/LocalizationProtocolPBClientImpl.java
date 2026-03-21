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
package org.apache.hadoop.yarn.server.nodemanager.api.impl.pb.client;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.ProtobufRpcEngine2;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.ipc.RPCUtil;
import org.apache.hadoop.yarn.proto.YarnServerNodemanagerServiceProtos.LocalizerStatusProto;
import org.apache.hadoop.yarn.server.nodemanager.api.LocalizationProtocol;
import org.apache.hadoop.yarn.server.nodemanager.api.LocalizationProtocolPB;
import org.apache.hadoop.yarn.server.nodemanager.api.protocolrecords.LocalizerHeartbeatResponse;
import org.apache.hadoop.yarn.server.nodemanager.api.protocolrecords.LocalizerStatus;
import org.apache.hadoop.yarn.server.nodemanager.api.protocolrecords.impl.pb.LocalizerHeartbeatResponsePBImpl;
import org.apache.hadoop.yarn.server.nodemanager.api.protocolrecords.impl.pb.LocalizerStatusPBImpl;

import org.apache.hadoop.thirdparty.protobuf.ServiceException;

/**
 * 本地化协议Protobuf RPC客户端实现，为资源本地化器提供与NodeManager通信的客户端能力
 */
public class LocalizationProtocolPBClientImpl implements LocalizationProtocol,
    Closeable {

  private LocalizationProtocolPB proxy;

  /**
   * 构造本地化协议Protobuf RPC客户端，建立与NodeManager的RPC连接
   * @param clientVersion 客户端版本号
   * @param addr NodeManager本地化服务地址
   * @param conf Hadoop配置对象
   * @throws IOException 连接建立失败时抛出IO异常
   */
  public LocalizationProtocolPBClientImpl(long clientVersion, InetSocketAddress addr, Configuration conf) throws IOException {
    RPC.setProtocolEngine(conf, LocalizationProtocolPB.class,
        ProtobufRpcEngine2.class);
    proxy = (LocalizationProtocolPB)RPC.getProxy(
        LocalizationProtocolPB.class, clientVersion, addr, conf);
  }

  @Override
  public void close() {
    if (this.proxy != null) {
      RPC.stopProxy(this.proxy);
    }
  }

  /**
   * 向NodeManager发送本地化器心跳，上报本地化状态并获取响应指令
   * @param status 本地化器当前状态
   * @return NodeManager返回的心跳响应
   * @throws YarnException Yarn服务异常
   * @throws IOException RPC通信IO异常
   */
  @Override
  public LocalizerHeartbeatResponse heartbeat(LocalizerStatus status)
    throws YarnException, IOException {
    // 从包装对象获取Protobuf格式的状态对象
    LocalizerStatusProto statusProto = ((LocalizerStatusPBImpl)status).getProto();
    try {
      // 发起RPC调用，将返回的Protobuf响应包装为业务对象返回
      return new LocalizerHeartbeatResponsePBImpl(
          proxy.heartbeat(null, statusProto));
    } catch (ServiceException e) {
      // 解包并重新抛出服务异常
      RPCUtil.unwrapAndThrowException(e);
      return null;
    }
  }

}