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
 * @file InterQJournalProtocolPB.java
 * HDFS QJournal节点间通信协议PB实现接口，定义JournalNode之间日志同步的RPC协议规范
 */
package org.apache.hadoop.hdfs.qjournal.protocolPB;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.qjournal.protocol.InterQJournalProtocolProtos.InterQJournalProtocolService;
import org.apache.hadoop.ipc.ProtocolInfo;
import org.apache.hadoop.security.KerberosInfo;

/**
 * JournalNode之间用于日志同步的PB协议接口，扩展Protobuf生成的服务接口
 * 添加Hadoop RPC安全认证所需的注解，供Hadoop RPC框架使用
 * 在HDFS QJM（Quorum Journal Manager）高可用方案中，用于Journal节点间同步 edits日志
 */
/**
 * Protocol used to communicate between journal nodes for journal sync.
 * Note: This extends the protocolbuffer service based interface to
 * add annotations required for security.
 */
@KerberosInfo(
    serverPrincipal = DFSConfigKeys.DFS_JOURNALNODE_KERBEROS_PRINCIPAL_KEY,
    clientPrincipal = DFSConfigKeys.DFS_JOURNALNODE_KERBEROS_PRINCIPAL_KEY)
@ProtocolInfo(protocolName =
    "org.apache.hadoop.hdfs.qjournal.protocol.InterQJournalProtocol",
    protocolVersion = 1)
@InterfaceAudience.Private
public interface InterQJournalProtocolPB extends
    InterQJournalProtocolService.BlockingInterface {
}