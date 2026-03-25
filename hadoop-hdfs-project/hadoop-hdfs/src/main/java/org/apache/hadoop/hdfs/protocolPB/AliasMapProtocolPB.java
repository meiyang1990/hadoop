// 这个文件已经全部加上中文注释
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.protocolPB;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.proto.AliasMapProtocolProtos;
import org.apache.hadoop.ipc.ProtocolInfo;
import org.apache.hadoop.security.KerberosInfo;

/**
 * 文件级注释：HDFS Provided存储别名映射协议Protobuf RPC接口定义
 * 本文件定义了NameNode与DataNode之间针对Provided存储别名映射查询的RPC协议PB接口，
 * 用于DataNode从NameNode获取文件路径与实际存储位置的映射关系，支持外部提供存储访问。
 * 
 * Protocol between the Namenode and the Datanode to read the AliasMap
 * used for Provided storage.
 */
/**
 * 别名映射协议PB接口，为NameNode和DataNode之间的别名映射查询提供基于Protobuf的阻塞RPC接口
 * 核心职责：为Provided存储路径别名查询提供RPC协议定义，实现跨节点的映射信息访问
 */
@ProtocolInfo(
    protocolName =
        "org.apache.hadoop.hdfs.server.aliasmap.AliasMapProtocol",
    protocolVersion = 1)
@KerberosInfo(
    serverPrincipal = DFSConfigKeys.DFS_NAMENODE_KERBEROS_PRINCIPAL_KEY)
@InterfaceAudience.Private
@InterfaceStability.Unstable
public interface AliasMapProtocolPB extends
    AliasMapProtocolProtos.AliasMapProtocolService.BlockingInterface {
}