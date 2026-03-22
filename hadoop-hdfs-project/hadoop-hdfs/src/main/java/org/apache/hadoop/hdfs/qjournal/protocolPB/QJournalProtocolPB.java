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
 * @file QJournalProtocolPB.java
 * QJournal协议PB实现接口，定义NameNode与JournalNode之间基于Protobuf的通信协议
 */
package org.apache.hadoop.hdfs.qjournal.protocolPB;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.QJournalProtocolService;
import org.apache.hadoop.ipc.ProtocolInfo;
import org.apache.hadoop.security.KerberosInfo;

/**
 * 仲裁日志节点的Protobuf通信协议接口
 * <p>
 * 核心职责：扩展基于Protobuf生成的服务接口，添加Hadoop RPC框架所需的安全注解和协议信息，
 * 用于NameNode向集群中的JournalNode同步编辑日志（edits），支持HDFS高可用场景下的元数据共享。
 * </p>
 */
@KerberosInfo(
    serverPrincipal = DFSConfigKeys.DFS_JOURNALNODE_KERBEROS_PRINCIPAL_KEY,
    clientPrincipal = DFSConfigKeys.DFS_NAMENODE_KERBEROS_PRINCIPAL_KEY)
@ProtocolInfo(protocolName = 
    "org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocol",
    protocolVersion = 1)
@InterfaceAudience.Private
public interface QJournalProtocolPB extends
    QJournalProtocolService.BlockingInterface {
}