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
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.SCMUploaderCanUploadRequestProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.SCMUploaderNotifyRequestProto;
import org.apache.hadoop.yarn.server.api.SCMUploaderProtocol;
import org.apache.hadoop.yarn.server.api.SCMUploaderProtocolPB;
import org.apache.hadoop.yarn.server.api.protocolrecords.SCMUploaderCanUploadRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.SCMUploaderCanUploadResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.SCMUploaderNotifyRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.SCMUploaderNotifyResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.SCMUploaderCanUploadRequestPBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.SCMUploaderCanUploadResponsePBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.SCMUploaderNotifyRequestPBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.SCMUploaderNotifyResponsePBImpl;

import org.apache.hadoop.thirdparty.protobuf.ServiceException;

// PB 客户端实现，封装与 SCM 上传服务的 RPC 调用
public class SCMUploaderProtocolPBClientImpl implements
    SCMUploaderProtocol, Closeable {

  private SCMUploaderProtocolPB proxy;

  // 构造上传协议的 PB 代理，配置 RPC 引擎并建立连接
  public SCMUploaderProtocolPBClientImpl(long clientVersion,
      InetSocketAddress addr, Configuration conf) throws IOException {
    RPC.setProtocolEngine(conf, SCMUploaderProtocolPB.class,
        ProtobufRpcEngine2.class);
    proxy =
        RPC.getProxy(SCMUploaderProtocolPB.class, clientVersion, addr, conf);
  }

  // 释放代理资源
  @Override
  public void close() {
    if (this.proxy != null) {
      RPC.stopProxy(this.proxy);
      this.proxy = null;
    }
  }

  // 上报上传完成/出错的通知，转换请求为 proto 并解包 ServiceException
  @Override
  public SCMUploaderNotifyResponse notify(SCMUploaderNotifyRequest request)
      throws YarnException, IOException {
    SCMUploaderNotifyRequestProto requestProto =
        ((SCMUploaderNotifyRequestPBImpl) request).getProto();
    try {
      return new SCMUploaderNotifyResponsePBImpl(proxy.notify(null,
          requestProto));
    } catch (ServiceException e) {
      RPCUtil.unwrapAndThrowException(e);
      return null;
    }
  }

  // 询问是否允许上传，保持统一的 proto 转换与异常解包流程
  @Override
  public SCMUploaderCanUploadResponse canUpload(
      SCMUploaderCanUploadRequest request) throws YarnException, IOException {
    SCMUploaderCanUploadRequestProto requestProto =
        ((SCMUploaderCanUploadRequestPBImpl)request).getProto();
    try {
      return new SCMUploaderCanUploadResponsePBImpl(proxy.canUpload(null,
          requestProto));
    } catch (ServiceException e) {
      RPCUtil.unwrapAndThrowException(e);
      return null;
    }
  }
}
