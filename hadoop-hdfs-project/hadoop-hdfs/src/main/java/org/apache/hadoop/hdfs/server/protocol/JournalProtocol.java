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
package org.apache.hadoop.hdfs.server.protocol;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.security.KerberosInfo;

/**
 * 文件级注释：HDFS日志同步RPC协议，定义了NameNode向远程节点同步编辑日志的接口
 * 
 * Protocol used to journal edits to a remote node. Currently,
 * this is used to publish edits from the NameNode to a BackupNode.
 * 核心职责：为Active NameNode提供向BackupNode同步编辑日志的RPC接口，实现元数据冗余备份
 */
@KerberosInfo(
    serverPrincipal = DFSConfigKeys.DFS_NAMENODE_KERBEROS_PRINCIPAL_KEY,
    clientPrincipal = DFSConfigKeys.DFS_NAMENODE_KERBEROS_PRINCIPAL_KEY)
@InterfaceAudience.Private
public interface JournalProtocol {
  /**
   * 协议版本标识，用于序列化兼容性校验
   * 
   * This class is used by both the Namenode (client) and BackupNode (server) 
   * to insulate from the protocol serialization.
   * 
   * If you are adding/changing DN's interface then you need to 
   * change both this class and ALSO related protocol buffer
   * wire protocol definition in JournalProtocol.proto.
   * 
   * For more details on protocol buffer wire protocol, please see 
   * .../org/apache/hadoop/hdfs/protocolPB/overview.html
   */
  public static final long versionID = 1L;

  /**
   * 向远程日志节点写入一批编辑日志记录，用于Active NameNode同步元数据变更到BackupNode
   * 
   * Journal edit records.
   * This message is sent by the active name-node to the backup node
   * via {@code EditLogBackupOutputStream} in order to synchronize meta-data
   * changes with the backup namespace image.
   * 
   * @param journalInfo 日志节点信息
   * @param epoch 日志写入者的纪元标识，用于隔离不同写入者
   * @param firstTxnId 本次批次中第一个事务ID
   * @param numTxns 本次批次包含的事务总数
   * @param records 序列化后的日志记录字节数组
   * @throws FencedException if the resource has been fenced
   * @throws IOException 网络或IO异常
   */
  public void journal(JournalInfo journalInfo,
                      long epoch,
                      long firstTxnId,
                      int numTxns,
                      byte[] records) throws IOException;

  /**
   * 通知BackupNode，NameNode已滚动编辑日志，开始写入新的日志分段
   * 
   * Notify the BackupNode that the NameNode has rolled its edit logs
   * and is now writing a new log segment.
   * @param journalInfo 日志节点信息
   * @param epoch 日志写入者的纪元标识
   * @param txid 新日志分段的第一个事务ID
   * @throws FencedException if the resource has been fenced
   * @throws IOException 网络或IO异常
   */
  public void startLogSegment(JournalInfo journalInfo, long epoch,
      long txid) throws IOException;
  
  /**
   * 执行fencing操作，隔离旧纪元的日志写入者，保证同一时间只有一个活跃写入者，解决脑裂问题
   * 
   * Request to fence any other journal writers.
   * Older writers with at previous epoch will be fenced and can no longer
   * perform journal operations.
   * 
   * @param journalInfo 日志节点信息
   * @param epoch 新日志写入者的纪元标识
   * @param fencerInfo 用于调试的fencer信息
   * @return fencing操作响应结果
   * @throws FencedException if the resource has been fenced
   * @throws IOException 网络或IO异常
   */
  public FenceResponse fence(JournalInfo journalInfo, long epoch,
      String fencerInfo) throws IOException;
}