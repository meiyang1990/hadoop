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

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.ProtobufRpcEngine2;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.ipc.RPCUtil;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.GetTimelineCollectorContextRequestProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.ReportNewCollectorInfoRequestProto;
import org.apache.hadoop.yarn.server.api.CollectorNodemanagerProtocol;
import org.apache.hadoop.yarn.server.api.CollectorNodemanagerProtocolPB;
import org.apache.hadoop.yarn.server.api.protocolrecords.GetTimelineCollectorContextRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.GetTimelineCollectorContextResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.ReportNewCollectorInfoRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.ReportNewCollectorInfoResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.GetTimelineCollectorContextRequestPBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.GetTimelineCollectorContextResponsePBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.ReportNewCollectorInfoRequestPBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.ReportNewCollectorInfoResponsePBImpl;

import org.apache.hadoop.thirdparty.protobuf.ServiceException;

// PB 客户端实现，封装与 NM Collector 服务的 RPC 交互
public class CollectorNodemanagerProtocolPBClientImpl implements
    CollectorNodemanagerProtocol, Closeable {

  // 未公开的配置键，仅内部测试使用
  static final String NM_COMMAND_TIMEOUT = YarnConfiguration.YARN_PREFIX
      + "rpc.nm-command-timeout";

  /**
   * Maximum of 1 minute timeout for a Node to react to the command.
   */
  static final int DEFAULT_COMMAND_TIMEOUT = 60000;

  private CollectorNodemanagerProtocolPB proxy;

  // 初始化 PB 代理客户端，按配置超时创建与目标 NM 的阻塞式 RPC 连接
  @Private
  public CollectorNodemanagerProtocolPBClientImpl(long clientVersion,
      InetSocketAddress addr, Configuration conf) throws IOException {
    RPC.setProtocolEngine(conf, CollectorNodemanagerProtocolPB.class,
        ProtobufRpcEngine2.class);
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();

    int expireIntvl = conf.getInt(NM_COMMAND_TIMEOUT, DEFAULT_COMMAND_TIMEOUT);
    proxy =
        (CollectorNodemanagerProtocolPB) RPC.getProxy(
            CollectorNodemanagerProtocolPB.class,
            clientVersion, addr, ugi, conf,
            NetUtils.getDefaultSocketFactory(conf), expireIntvl);
  }

  // 向 NM 上报新的 collector 注册信息，失败时解包 ServiceException 为 YarnException/IOException
  @Override
  public ReportNewCollectorInfoResponse reportNewCollectorInfo(
      ReportNewCollectorInfoRequest request) throws YarnException, IOException {

    ReportNewCollectorInfoRequestProto requestProto =
        ((ReportNewCollectorInfoRequestPBImpl) request).getProto();
    try {
      return new ReportNewCollectorInfoResponsePBImpl(
          proxy.reportNewCollectorInfo(null, requestProto));
    } catch (ServiceException e) {
      RPCUtil.unwrapAndThrowException(e);
      return null;
    }
  }

  // 查询指定应用的 collector 运行上下文，同样做 proto 转换与异常解包
  @Override
  public GetTimelineCollectorContextResponse getTimelineCollectorContext(
      GetTimelineCollectorContextRequest request)
      throws YarnException, IOException {
    GetTimelineCollectorContextRequestProto requestProto =
        ((GetTimelineCollectorContextRequestPBImpl) request).getProto();
    try {
      return new GetTimelineCollectorContextResponsePBImpl(
          proxy.getTimelineCollectorContext(null, requestProto));
    } catch (ServiceException e) {
      RPCUtil.unwrapAndThrowException(e);
      return null;
    }
  }

  // 关闭底层 RPC 代理，释放网络资源
  @Override
  public void close() {
    if (this.proxy != null) {
      RPC.stopProxy(this.proxy);
    }
  }

}
