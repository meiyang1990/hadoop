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

import org.apache.hadoop.thirdparty.protobuf.ServiceException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.ProtobufRpcEngine2;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos;
import org.apache.hadoop.yarn.server.api.DistributedSchedulingAMProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.DistributedSchedulingAllocateRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.DistributedSchedulingAllocateResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterDistributedSchedulingAMResponse;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.AllocateRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.AllocateResponsePBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.DistributedSchedulingAllocateRequestPBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.DistributedSchedulingAllocateResponsePBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.RegisterDistributedSchedulingAMResponsePBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.FinishApplicationMasterRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.FinishApplicationMasterResponsePBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.RegisterApplicationMasterRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.RegisterApplicationMasterResponsePBImpl;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.ipc.RPCUtil;
import org.apache.hadoop.yarn.proto.YarnServiceProtos;
import org.apache.hadoop.yarn.server.api.DistributedSchedulingAMProtocolPB;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * Implementation of {@link DistributedSchedulingAMProtocol}, used when
 * distributed scheduling is enabled.
 */
// PB 客户端实现，兼容分布式调度开启时 AM 的所有 RPC 接口
public class DistributedSchedulingAMProtocolPBClientImpl implements
    DistributedSchedulingAMProtocol, Closeable {

  private DistributedSchedulingAMProtocolPB proxy;

  // 依据给定地址与版本构建阻塞式 PB 代理，复用 ProtobufRpcEngine2
  public DistributedSchedulingAMProtocolPBClientImpl(long clientVersion,
      InetSocketAddress addr, Configuration conf) throws IOException {
    RPC.setProtocolEngine(conf, DistributedSchedulingAMProtocolPB.class,
        ProtobufRpcEngine2.class);
    proxy = RPC.getProxy(DistributedSchedulingAMProtocolPB.class, clientVersion,
        addr, conf);
  }

  // 释放底层代理
  @Override
  public void close() {
    if (this.proxy != null) {
      RPC.stopProxy(this.proxy);
    }
  }

  // 注册 AM 以便启用分布式调度，转换请求为 proto 并解包异常
  @Override
  public RegisterDistributedSchedulingAMResponse
      registerApplicationMasterForDistributedScheduling(
      RegisterApplicationMasterRequest request)
      throws YarnException, IOException {
    YarnServiceProtos.RegisterApplicationMasterRequestProto requestProto =
        ((RegisterApplicationMasterRequestPBImpl) request).getProto();
    try {
      return new RegisterDistributedSchedulingAMResponsePBImpl(
          proxy.registerApplicationMasterForDistributedScheduling(
              null, requestProto));
    } catch (ServiceException e) {
      RPCUtil.unwrapAndThrowException(e);
      return null;
    }
  }

  // 分布式调度版的 allocate，直接调用对应 RPC 并返回 PB 封装响应
  @Override
  public DistributedSchedulingAllocateResponse allocateForDistributedScheduling(
      DistributedSchedulingAllocateRequest request)
      throws YarnException, IOException {
    YarnServerCommonServiceProtos.DistributedSchedulingAllocateRequestProto
        requestProto =
        ((DistributedSchedulingAllocateRequestPBImpl) request).getProto();
    try {
      return new DistributedSchedulingAllocateResponsePBImpl(
          proxy.allocateForDistributedScheduling(null, requestProto));
    } catch (ServiceException e) {
      RPCUtil.unwrapAndThrowException(e);
      return null;
    }
  }

  // 常规 AM 注册 RPC，沿用同样的 proto 转换与异常解包流程
  @Override
  public RegisterApplicationMasterResponse registerApplicationMaster(
      RegisterApplicationMasterRequest request)
      throws YarnException, IOException {
    YarnServiceProtos.RegisterApplicationMasterRequestProto requestProto =
        ((RegisterApplicationMasterRequestPBImpl) request).getProto();
    try {
      return new RegisterApplicationMasterResponsePBImpl(
          proxy.registerApplicationMaster(null, requestProto));
    } catch (ServiceException e) {
      RPCUtil.unwrapAndThrowException(e);
      return null;
    }
  }

  // 结束 AM 的 RPC，负责包装/解包 proto 请求响应
  @Override
  public FinishApplicationMasterResponse finishApplicationMaster(
      FinishApplicationMasterRequest request)
      throws YarnException, IOException {
    YarnServiceProtos.FinishApplicationMasterRequestProto requestProto =
        ((FinishApplicationMasterRequestPBImpl) request).getProto();
    try {
      return new FinishApplicationMasterResponsePBImpl(
          proxy.finishApplicationMaster(null, requestProto));
    } catch (ServiceException e) {
      RPCUtil.unwrapAndThrowException(e);
      return null;
    }
  }

  // 常规 allocate 接口，保持同一的异常处理模式
  @Override
  public AllocateResponse allocate(AllocateRequest request)
      throws YarnException, IOException {
    YarnServiceProtos.AllocateRequestProto requestProto =
        ((AllocateRequestPBImpl) request).getProto();
    try {
      return new AllocateResponsePBImpl(proxy.allocate(null, requestProto));
    } catch (ServiceException e) {
      RPCUtil.unwrapAndThrowException(e);
      return null;
    }
  }
}
