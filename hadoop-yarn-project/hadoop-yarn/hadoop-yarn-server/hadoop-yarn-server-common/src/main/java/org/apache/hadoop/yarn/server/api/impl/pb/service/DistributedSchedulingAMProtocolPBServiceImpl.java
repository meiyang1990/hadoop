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

import org.apache.hadoop.thirdparty.protobuf.RpcController;
import org.apache.hadoop.thirdparty.protobuf.ServiceException;
import org.apache.hadoop.yarn.server.api.DistributedSchedulingAMProtocol;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos;
import org.apache.hadoop.yarn.server.api.DistributedSchedulingAMProtocolPB;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.DistributedSchedulingAllocateResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterDistributedSchedulingAMResponse;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterResponse;
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
import org.apache.hadoop.yarn.proto.YarnServiceProtos;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.AllocateRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.RegisterApplicationMasterRequestProto;

import java.io.IOException;

/**
 * Implementation of {@link DistributedSchedulingAMProtocolPB}.
 */
// PB Service 端实现，负责将分布式调度 AM 的 proto 请求转换并委派给真实实现
public class DistributedSchedulingAMProtocolPBServiceImpl implements
    DistributedSchedulingAMProtocolPB {

  private DistributedSchedulingAMProtocol real;

  // 保存真实协议实现，后续方法统一转发
  public DistributedSchedulingAMProtocolPBServiceImpl(
      DistributedSchedulingAMProtocol impl) {
    this.real = impl;
  }

  // 处理 AM 分布式调度注册，转换请求并捕获异常为 ServiceException
  @Override
  public YarnServerCommonServiceProtos.
      RegisterDistributedSchedulingAMResponseProto
      registerApplicationMasterForDistributedScheduling(
      RpcController controller, RegisterApplicationMasterRequestProto proto)
      throws ServiceException {
    RegisterApplicationMasterRequestPBImpl request = new
        RegisterApplicationMasterRequestPBImpl(proto);
    try {
      RegisterDistributedSchedulingAMResponse response =
          real.registerApplicationMasterForDistributedScheduling(request);
      return ((RegisterDistributedSchedulingAMResponsePBImpl) response)
          .getProto();
    } catch (YarnException e) {
      throw new ServiceException(e);
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  // 分布式调度版 allocate，负责 proto 与内部对象互转并回传响应 proto
  @Override
  public YarnServerCommonServiceProtos.
      DistributedSchedulingAllocateResponseProto
      allocateForDistributedScheduling(RpcController controller,
      YarnServerCommonServiceProtos.
          DistributedSchedulingAllocateRequestProto proto)
      throws ServiceException {
    DistributedSchedulingAllocateRequestPBImpl request =
        new DistributedSchedulingAllocateRequestPBImpl(proto);
    try {
      DistributedSchedulingAllocateResponse response = real
          .allocateForDistributedScheduling(request);
      return ((DistributedSchedulingAllocateResponsePBImpl) response)
          .getProto();
    } catch (YarnException e) {
      throw new ServiceException(e);
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  // 常规 allocate RPC 的 PB 封装
  @Override
  public YarnServiceProtos.AllocateResponseProto allocate(RpcController arg0,
      AllocateRequestProto proto) throws ServiceException {
    AllocateRequestPBImpl request = new AllocateRequestPBImpl(proto);
    try {
      AllocateResponse response = real.allocate(request);
      return ((AllocateResponsePBImpl) response).getProto();
    } catch (YarnException e) {
      throw new ServiceException(e);
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  // 结束 AM 的 RPC，保持一致的转换和异常包装
  @Override
  public YarnServiceProtos.FinishApplicationMasterResponseProto
  finishApplicationMaster(
      RpcController arg0, YarnServiceProtos
      .FinishApplicationMasterRequestProto proto)
      throws ServiceException {
    FinishApplicationMasterRequestPBImpl request = new
        FinishApplicationMasterRequestPBImpl(proto);
    try {
      FinishApplicationMasterResponse response = real.finishApplicationMaster
          (request);
      return ((FinishApplicationMasterResponsePBImpl) response).getProto();
    } catch (YarnException e) {
      throw new ServiceException(e);
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  // AM 注册的常规 RPC，负责转换请求与响应 proto
  @Override
  public YarnServiceProtos.RegisterApplicationMasterResponseProto
  registerApplicationMaster(
      RpcController arg0, RegisterApplicationMasterRequestProto proto)
      throws ServiceException {
    RegisterApplicationMasterRequestPBImpl request = new
        RegisterApplicationMasterRequestPBImpl(proto);
    try {
      RegisterApplicationMasterResponse response = real
          .registerApplicationMaster(request);
      return ((RegisterApplicationMasterResponsePBImpl) response).getProto();
    } catch (YarnException e) {
      throw new ServiceException(e);
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }
}
