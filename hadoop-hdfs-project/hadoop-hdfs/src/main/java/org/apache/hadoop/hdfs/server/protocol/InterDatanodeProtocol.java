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
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.server.protocol.BlockRecoveryCommand.RecoveringBlock;
import org.apache.hadoop.security.KerberosInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 数据节点之间的内部通信协议，用于块恢复过程中更新块的生成戳
 * 在数据块恢复流程中，不同数据节点之间通过该协议交互副本信息，完成块恢复
 */
@KerberosInfo(
    serverPrincipal = DFSConfigKeys.DFS_DATANODE_KERBEROS_PRINCIPAL_KEY,
    clientPrincipal = DFSConfigKeys.DFS_DATANODE_KERBEROS_PRINCIPAL_KEY)
@InterfaceAudience.Private
public interface InterDatanodeProtocol {
  Logger LOG = LoggerFactory.getLogger(InterDatanodeProtocol.class.getName());

  /**
   * Until version 9, this class InterDatanodeProtocol served as both
   * the interface to the DN AND the RPC protocol used to communicate with the 
   * DN.
   * 
   * This class is used by both the DN to insulate from the protocol 
   * serialization.
   * 
   * If you are adding/changing DN's interface then you need to 
   * change both this class and ALSO related protocol buffer
   * wire protocol definition in InterDatanodeProtocol.proto.
   * 
   * For more details on protocol buffer wire protocol, please see 
   * .../org/apache/hadoop/hdfs/protocolPB/overview.html
   */
  public static final long versionID = 6L;

  /**
   * 初始化副本恢复流程，获取本节点上对应块的副本状态信息
   * @param rBlock 待恢复的块信息
   * @return 本节点上该副本的实际状态，如果本节点没有该副本则返回null
   * @throws IOException 通信或IO异常
   */
  ReplicaRecoveryInfo initReplicaRecovery(RecoveringBlock rBlock)
  throws IOException;

  /**
   * 在恢复过程中更新副本信息，修改为新的生成戳和块长度
   * @param oldBlock 原块信息
   * @param recoveryId 恢复ID
   * @param newBlockId 新的块ID
   * @param newLength 新的块长度
   * @return 存储路径
   * @throws IOException 通信或IO异常
   */
  String updateReplicaUnderRecovery(ExtendedBlock oldBlock, long recoveryId,
                                    long newBlockId, long newLength)
      throws IOException;
}