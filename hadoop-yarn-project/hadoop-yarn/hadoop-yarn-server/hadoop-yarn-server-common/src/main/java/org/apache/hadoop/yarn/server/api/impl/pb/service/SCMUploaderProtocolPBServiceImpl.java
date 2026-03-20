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
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.SCMUploaderCanUploadRequestProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.SCMUploaderCanUploadResponseProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.SCMUploaderNotifyRequestProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.SCMUploaderNotifyResponseProto;
import org.apache.hadoop.yarn.server.api.SCMUploaderProtocol;
import org.apache.hadoop.yarn.server.api.SCMUploaderProtocolPB;
import org.apache.hadoop.yarn.server.api.protocolrecords.SCMUploaderCanUploadResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.SCMUploaderNotifyResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.SCMUploaderCanUploadRequestPBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.SCMUploaderCanUploadResponsePBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.SCMUploaderNotifyRequestPBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.SCMUploaderNotifyResponsePBImpl;

import org.apache.hadoop.thirdparty.protobuf.RpcController;
import org.apache.hadoop.thirdparty.protobuf.ServiceException;

// PB Service 端实现，负责将 SCM 上传协议的 proto 请求委派给真实实现
public class SCMUploaderProtocolPBServiceImpl implements
    SCMUploaderProtocolPB {

  private SCMUploaderProtocol real;

  // 保存真实实现，后续方法统一转发
  public SCMUploaderProtocolPBServiceImpl(SCMUploaderProtocol impl) {
    this.real = impl;
  }

  // 处理上传完成/失败通知，转换请求对象并统一包装异常
  @Override
  public SCMUploaderNotifyResponseProto notify(RpcController controller,
      SCMUploaderNotifyRequestProto proto) throws ServiceException {
    SCMUploaderNotifyRequestPBImpl request =
        new SCMUploaderNotifyRequestPBImpl(proto);
    try {
      SCMUploaderNotifyResponse response = real.notify(request);
      return ((SCMUploaderNotifyResponsePBImpl) response).getProto();
    } catch (YarnException e) {
      throw new ServiceException(e);
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  // 处理上传权限校验，同样负责 proto 与内部对象互转
  @Override
  public SCMUploaderCanUploadResponseProto canUpload(RpcController controller,
      SCMUploaderCanUploadRequestProto proto)
      throws ServiceException {
    SCMUploaderCanUploadRequestPBImpl request =
        new SCMUploaderCanUploadRequestPBImpl(proto);
    try {
      SCMUploaderCanUploadResponse response = real.canUpload(request);
      return ((SCMUploaderCanUploadResponsePBImpl)response).getProto();
    } catch (YarnException e) {
      throw new ServiceException(e);
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }
}
