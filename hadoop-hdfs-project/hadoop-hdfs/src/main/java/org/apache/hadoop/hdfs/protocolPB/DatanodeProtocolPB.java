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
 * @file DatanodeProtocolPB.java
 * @brief HDFS DataNode与NameNode之间PB序列化RPC协议接口定义
 * 
 * 该文件属于HDFS协议模块，定义了DataNode节点向NameNode节点发起RPC调用的
 * Protobuf序列化版本协议接口，用于DataNode与NameNode之间的通信。
 */
package org.apache.hadoop.hdfs.protocolPB;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.DatanodeProtocolService;
import org.apache.hadoop.ipc.ProtocolInfo;
import org.apache.hadoop.security.KerberosInfo;

/**
 * @brief DataNode协议Protobuf版本扩展接口
 * 
 * 继承自Protobuf生成的BlockingInterface，为DataNode和NameNode之间的RPC通信
 * 提供Hadoop RPC框架所需的协议标注信息，指定了Kerberos认证主体、协议名称和版本。
 * 该接口仅用于RPC框架层面的协议定义，实际业务逻辑由NameNode服务端实现。
 */
@KerberosInfo(
    serverPrincipal = DFSConfigKeys.DFS_NAMENODE_KERBEROS_PRINCIPAL_KEY, 
    clientPrincipal = DFSConfigKeys.DFS_DATANODE_KERBEROS_PRINCIPAL_KEY)
@ProtocolInfo(
    protocolName = "org.apache.hadoop.hdfs.server.protocol.DatanodeProtocol", 
    protocolVersion = 1)
@InterfaceAudience.Private
public interface DatanodeProtocolPB extends
    DatanodeProtocolService.BlockingInterface {
}