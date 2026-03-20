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

package org.apache.hadoop.yarn.server.api.impl.pb.client;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.ProtobufRpcEngine2;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.ipc.RPCUtil;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.NodeHeartbeatRequestProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.RegisterNodeManagerRequestProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.UnRegisterNodeManagerRequestProto;
import org.apache.hadoop.yarn.server.api.ResourceTracker;
import org.apache.hadoop.yarn.server.api.ResourceTrackerPB;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.UnRegisterNodeManagerRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.UnRegisterNodeManagerResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.NodeHeartbeatRequestPBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.NodeHeartbeatResponsePBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.RegisterNodeManagerRequestPBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.RegisterNodeManagerResponsePBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.UnRegisterNodeManagerRequestPBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.UnRegisterNodeManagerResponsePBImpl;

import org.apache.hadoop.thirdparty.protobuf.ServiceException;

// PB 客户端实现，负责与 RM 的 ResourceTracker 服务进行 RPC 交互
public class ResourceTrackerPBClientImpl implements ResourceTracker, Closeable {

  private ResourceTrackerPB proxy;
  
  // 构造代理客户端，使用 ProtobufRpcEngine2 与目标地址建立阻塞式连接
  public ResourceTrackerPBClientImpl(long clientVersion, InetSocketAddress addr, Configuration conf) throws IOException {
    RPC.setProtocolEngine(conf, ResourceTrackerPB.class,
        ProtobufRpcEngine2.class);
    proxy = (ResourceTrackerPB)RPC.getProxy(
        ResourceTrackerPB.class, clientVersion, addr, conf);
  }

  // 关闭底层代理
  @Override
  public void close() {
    if(this.proxy != null) {
      RPC.stopProxy(this.proxy);
    }
  }

  // 注册新 NodeManager，将请求转换为 proto 并处理 ServiceException
  @Override
  public RegisterNodeManagerResponse registerNodeManager(
      RegisterNodeManagerRequest request) throws YarnException,
      IOException {
    RegisterNodeManagerRequestProto requestProto = ((RegisterNodeManagerRequestPBImpl)request).getProto();
    try {
      return new RegisterNodeManagerResponsePBImpl(proxy.registerNodeManager(null, requestProto));
    } catch (ServiceException e) {
      RPCUtil.unwrapAndThrowException(e);
      return null;
    }
  }

  // NodeManager 心跳 RPC，包装/解包 proto 与异常
  @Override
  public NodeHeartbeatResponse nodeHeartbeat(NodeHeartbeatRequest request)
      throws YarnException, IOException {
    NodeHeartbeatRequestProto requestProto = ((NodeHeartbeatRequestPBImpl)request).getProto();
    try {
      return new NodeHeartbeatResponsePBImpl(proxy.nodeHeartbeat(null, requestProto));
    } catch (ServiceException e) {
      RPCUtil.unwrapAndThrowException(e);
      return null;
    }
  }

  // 注销 NodeManager，沿用统一的 proto 转换和异常处理
  @Override
  public UnRegisterNodeManagerResponse unRegisterNodeManager(
      UnRegisterNodeManagerRequest request) throws YarnException, IOException {
    UnRegisterNodeManagerRequestProto requestProto =
        ((UnRegisterNodeManagerRequestPBImpl) request).getProto();
    try {
      return new UnRegisterNodeManagerResponsePBImpl(
          proxy.unRegisterNodeManager(null, requestProto));
    } catch (ServiceException e) {
      RPCUtil.unwrapAndThrowException(e);
      return null;
    }
  }
}
