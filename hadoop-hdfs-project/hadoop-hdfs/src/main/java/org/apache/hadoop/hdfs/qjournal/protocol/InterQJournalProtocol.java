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

package org.apache.hadoop.hdfs.qjournal.protocol;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.proto.HdfsServerProtos.StorageInfoProto;
import org.apache.hadoop.hdfs.qjournal.server.JournalNode;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.GetEditLogManifestResponseProto;
import org.apache.hadoop.security.KerberosInfo;

import java.io.IOException;

/**
 * 文件说明：QJournal Journal节点间同步协议接口定义
 * 
 * 该接口定义了HA架构下多个JournalNode之间进行日志同步时使用的RPC协议，
 * 负责在Journal节点之间交换编辑日志元数据和存储信息，保证多Journal节点间的数据一致性。
 */

@KerberosInfo(
    serverPrincipal = DFSConfigKeys.DFS_JOURNALNODE_KERBEROS_PRINCIPAL_KEY,
    clientPrincipal = DFSConfigKeys.DFS_JOURNALNODE_KERBEROS_PRINCIPAL_KEY)
@InterfaceAudience.Private
public interface InterQJournalProtocol {

  /** 协议版本ID */
  long versionID = 1L;

  /**
   * 从指定Journal节点获取编辑日志分段清单
   * 
   * @param jid 日志节点ID，指定要查询的日志
   * @param nameServiceId 名称服务ID，区分不同的命名空间
   * @param sinceTxId 起始事务ID，只返回该事务ID之后的日志分段
   * @param inProgressOk 是否包含未完成的正在写入的编辑日志分段
   * @return 指定事务ID之后的所有编辑日志分段信息响应
   * @throws IOException RPC调用或IO操作异常
   */
  GetEditLogManifestResponseProto getEditLogManifestFromJournal(
      String jid, String nameServiceId, long sinceTxId, boolean inProgressOk)
      throws IOException;

  /**
   * 获取指定日志的存储信息
   * 
   * @param jid 日志节点ID
   * @param nameServiceId 名称服务ID
   * @return 存储信息 proto 对象，包含存储版本、布局等信息
   * @throws IOException RPC调用或IO操作异常
   */
  StorageInfoProto getStorageInfo(String jid, String nameServiceId)
      throws IOException;

}