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
package org.apache.hadoop.mapreduce.v2.api;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.ipc.ProtocolInfo;
import org.apache.hadoop.mapreduce.v2.hs.proto.HSAdminRefreshProtocolProtos.HSAdminRefreshProtocolService;
import org.apache.hadoop.security.KerberosInfo;

/**
 * 文件说明：MapReduce历史服务器管理刷新协议的Protobuf RPC实现接口
 * 所属模块：hadoop-mapreduce-client-common，提供历史服务器管理员刷新操作的RPC协议定义
 */
@KerberosInfo(serverPrincipal = CommonConfigurationKeys.HADOOP_SECURITY_SERVICE_USER_NAME_KEY)
@ProtocolInfo(protocolName = "org.apache.hadoop.mapreduce.v2.api.HSAdminRefreshProtocol", protocolVersion = 1)
@Private
@InterfaceStability.Evolving
/**
 * HSAdminRefreshProtocolPB 是历史服务器管理刷新协议的Protobuf阻塞版接口
 * 扩展自Protobuf生成的BlockingInterface，用于客户端和历史服务器之间
 * 完成刷新配置等管理员操作的RPC通信
 */
public interface HSAdminRefreshProtocolPB extends
    HSAdminRefreshProtocolService.BlockingInterface {
}