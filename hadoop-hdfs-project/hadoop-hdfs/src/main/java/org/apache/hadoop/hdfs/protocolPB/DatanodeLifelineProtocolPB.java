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
import org.apache.hadoop.hdfs.protocol.proto.DatanodeLifelineProtocolProtos.DatanodeLifelineProtocolService;
import org.apache.hadoop.ipc.ProtocolInfo;
import org.apache.hadoop.security.KerberosInfo;

/**
 * 文件级注释：DataNode生命线协议Protobuf版本定义，用于DataNode向NameNode发送保活心跳
 * 协议作用：当常规心跳请求超时时，DataNode通过该轻量级协议发送生命线消息，告知NameNode自身存活状态
 * Protocol used by a DataNode to send lifeline messages to a NameNode.
 */
/**
 * 定义DataNode向NameNode发送存活消息的Protobuf阻塞接口，基于Hadoop RPC框架实现
 */
@KerberosInfo(
    serverPrincipal = DFSConfigKeys.DFS_NAMENODE_KERBEROS_PRINCIPAL_KEY,
    clientPrincipal = DFSConfigKeys.DFS_DATANODE_KERBEROS_PRINCIPAL_KEY)
@ProtocolInfo(
    protocolName =
        "org.apache.hadoop.hdfs.server.protocol.DatanodeLifelineProtocol",
    protocolVersion = 1)
@InterfaceAudience.Private
public interface DatanodeLifelineProtocolPB extends
    DatanodeLifelineProtocolService.BlockingInterface {
}