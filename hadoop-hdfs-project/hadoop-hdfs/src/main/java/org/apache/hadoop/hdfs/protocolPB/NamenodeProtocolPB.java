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

package org.apache.hadoop.hdfs.protocolPB;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.proto.NamenodeProtocolProtos.NamenodeProtocolService;
import org.apache.hadoop.ipc.ProtocolInfo;
import org.apache.hadoop.security.KerberosInfo;

/**
 * 备用NameNode（SecondaryNameNode）与主NameNode通信的Protobuf协议接口
 * 用于从主NameNode获取部分元数据状态，支持检查点操作
 * 
 * 该接口基于Protobuf生成的服务接口扩展，添加安全认证所需的注解
 */
/**
 * Kerberos认证配置：指定服务端和客户端的Principal配置键
 */
@KerberosInfo(
    serverPrincipal = DFSConfigKeys.DFS_NAMENODE_KERBEROS_PRINCIPAL_KEY,
    clientPrincipal = DFSConfigKeys.DFS_NAMENODE_KERBEROS_PRINCIPAL_KEY)
/**
 * RPC协议信息配置：指定协议名称和版本号
 */
@ProtocolInfo(protocolName = 
    "org.apache.hadoop.hdfs.server.protocol.NamenodeProtocol",
    protocolVersion = 1)
@InterfaceAudience.Private
public interface NamenodeProtocolPB extends
    NamenodeProtocolService.BlockingInterface {
}