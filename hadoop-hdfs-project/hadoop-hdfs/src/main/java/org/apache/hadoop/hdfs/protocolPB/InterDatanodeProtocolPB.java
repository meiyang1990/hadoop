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
/**
 * 数据节点间通信协议的Protobuf序列化RPC接口定义
 * 属于HDFS内部私有协议，用于DataNode之间的交互通信
 */
package org.apache.hadoop.hdfs.protocolPB;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.proto.InterDatanodeProtocolProtos.InterDatanodeProtocolService;
import org.apache.hadoop.ipc.ProtocolInfo;
import org.apache.hadoop.security.KerberosInfo;

/**
 * Kerberos认证配置：通信双方均使用DataNode的服务主体
 */
@KerberosInfo(
    serverPrincipal = DFSConfigKeys.DFS_DATANODE_KERBEROS_PRINCIPAL_KEY,
    clientPrincipal = DFSConfigKeys.DFS_DATANODE_KERBEROS_PRINCIPAL_KEY)
/**
 * RPC协议信息配置：指定协议名称与版本
 */
@ProtocolInfo(protocolName = 
    "org.apache.hadoop.hdfs.server.protocol.InterDatanodeProtocol",
    protocolVersion = 1)
@InterfaceAudience.Private
/**
 * 数据节点间协议的Protobuf阻塞接口定义
 * 扩展Protobuf生成的BlockingInterface，为Hadoop RPC提供协议标识
 * 用于DataNode之间执行块恢复、块复制等内部交互
 */
public interface InterDatanodeProtocolPB extends
    InterDatanodeProtocolService.BlockingInterface {
}