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

package org.apache.hadoop.yarn.server.api.impl.pb.service;

import java.io.IOException;

import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.NodeHeartbeatRequestProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.NodeHeartbeatResponseProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.RegisterNodeManagerRequestProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.RegisterNodeManagerResponseProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.UnRegisterNodeManagerRequestProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.UnRegisterNodeManagerResponseProto;
import org.apache.hadoop.yarn.server.api.ResourceTracker;
import org.apache.hadoop.yarn.server.api.ResourceTrackerPB;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.UnRegisterNodeManagerResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.NodeHeartbeatRequestPBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.NodeHeartbeatResponsePBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.RegisterNodeManagerRequestPBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.RegisterNodeManagerResponsePBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.UnRegisterNodeManagerRequestPBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.UnRegisterNodeManagerResponsePBImpl;

import org.apache.hadoop.thirdparty.protobuf.RpcController;
import org.apache.hadoop.thirdparty.protobuf.ServiceException;

/**
 * ResourceTracker 协议的 Protobuf 服务实现类，
 * 将 RPC 请求转换为实际的业务逻辑调用。
 */
public class ResourceTrackerPBServiceImpl implements ResourceTrackerPB {

  private ResourceTracker real;
  
  public ResourceTrackerPBServiceImpl(ResourceTracker impl) {
    this.real = impl;
  }
  
  /**
   * 处理节点注册请求，将 ProtocolBuffer 格式转换为内部对象，
   * 调用实际业务实现并返回响应。
   */
  @Override
  public RegisterNodeManagerResponseProto registerNodeManager(
      RpcController controller, RegisterNodeManagerRequestProto proto)
      throws ServiceException {
    RegisterNodeManagerRequestPBImpl request = new RegisterNodeManagerRequestPBImpl(proto);
    try {
      RegisterNodeManagerResponse response = real.registerNodeManager(request);
      return ((RegisterNodeManagerResponsePBImpl)response).getProto();
    } catch (YarnException | IOException e) {
      throw new ServiceException(e);
    }
  }

  /**
   * 处理节点心跳请求，转换协议格式并调用实际业务逻辑。
   */
  @Override
  public NodeHeartbeatResponseProto nodeHeartbeat(RpcController controller,
      NodeHeartbeatRequestProto proto) throws ServiceException {
    NodeHeartbeatRequestPBImpl request = new NodeHeartbeatRequestPBImpl(proto);
    try {
      NodeHeartbeatResponse response = real.nodeHeartbeat(request);
      return ((NodeHeartbeatResponsePBImpl)response).getProto();
    } catch (YarnException | IOException e) {
      throw new ServiceException(e);
    }
  }

  /**
   * 处理节点注销请求，转换协议格式并调用实际业务逻辑。
   */
  @Override
  public UnRegisterNodeManagerResponseProto unRegisterNodeManager(
      RpcController controller, UnRegisterNodeManagerRequestProto proto)
      throws ServiceException {
    UnRegisterNodeManagerRequestPBImpl request =
        new UnRegisterNodeManagerRequestPBImpl(proto);
    try {
      UnRegisterNodeManagerResponse response = real
          .unRegisterNodeManager(request);
      return ((UnRegisterNodeManagerResponsePBImpl) response).getProto();
    } catch (YarnException | IOException e) {
      throw new ServiceException(e);
    }
  }
}
