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
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.GetTimelineCollectorContextRequestProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.GetTimelineCollectorContextResponseProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.ReportNewCollectorInfoRequestProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.ReportNewCollectorInfoResponseProto;
import org.apache.hadoop.yarn.server.api.CollectorNodemanagerProtocol;
import org.apache.hadoop.yarn.server.api.CollectorNodemanagerProtocolPB;
import org.apache.hadoop.yarn.server.api.protocolrecords.GetTimelineCollectorContextResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.ReportNewCollectorInfoResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.GetTimelineCollectorContextRequestPBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.GetTimelineCollectorContextResponsePBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.ReportNewCollectorInfoRequestPBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.ReportNewCollectorInfoResponsePBImpl;

import org.apache.hadoop.thirdparty.protobuf.RpcController;
import org.apache.hadoop.thirdparty.protobuf.ServiceException;

// PB Service 端实现，桥接 NM Collector 协议的 proto 请求与真实实现
public class CollectorNodemanagerProtocolPBServiceImpl implements
    CollectorNodemanagerProtocolPB {

  private CollectorNodemanagerProtocol real;

  // 保存真实实现，后续直接委派
  public CollectorNodemanagerProtocolPBServiceImpl(
      CollectorNodemanagerProtocol impl) {
    this.real = impl;
  }

  // 处理 collector 注册上报，将 proto 包装成内部请求并传递给真实实现
  @Override
  public ReportNewCollectorInfoResponseProto reportNewCollectorInfo(
      RpcController arg0, ReportNewCollectorInfoRequestProto proto)
      throws ServiceException {
    ReportNewCollectorInfoRequestPBImpl request =
        new ReportNewCollectorInfoRequestPBImpl(proto);
    try {
      ReportNewCollectorInfoResponse response =
          real.reportNewCollectorInfo(request);
      return ((ReportNewCollectorInfoResponsePBImpl)response).getProto();
    } catch (YarnException e) {
      throw new ServiceException(e);
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  // 处理 collector 上下文查询，负责 proto 与内部对象的双向转换
  @Override
  public GetTimelineCollectorContextResponseProto getTimelineCollectorContext(
      RpcController controller,
      GetTimelineCollectorContextRequestProto proto) throws ServiceException {
    GetTimelineCollectorContextRequestPBImpl request =
        new GetTimelineCollectorContextRequestPBImpl(proto);
    try {
      GetTimelineCollectorContextResponse response =
          real.getTimelineCollectorContext(request);
      return ((GetTimelineCollectorContextResponsePBImpl)response).getProto();
    } catch (YarnException e) {
      throw new ServiceException(e);
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }
}
