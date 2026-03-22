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
import org.apache.hadoop.io.retry.Idempotent;
import org.apache.hadoop.security.KerberosInfo;

/**
 * 文件所属模块：HDFS服务端核心协议
 * 核心职责：定义DataNode向NameNode发送生命维持（lifeline）心跳消息的RPC协议
 * 功能说明：当DataNode通过本协议定期向NameNode发送存活状态报告，
 *          让NameNode确认DataNode是否正常运行，区别于常规心跳，
 *          主要用于节点存活探测场景
 * 认证信息：用于DataNode -> NameNode的反向生命探测通信
 */
/**
 * Protocol used by a DataNode to send lifeline messages to a NameNode.
 */
@KerberosInfo(
    serverPrincipal = DFSConfigKeys.DFS_NAMENODE_KERBEROS_PRINCIPAL_KEY,
    clientPrincipal = DFSConfigKeys.DFS_DATANODE_KERBEROS_PRINCIPAL_KEY)
@InterfaceAudience.Private
public interface DatanodeLifelineProtocol {

  /**
   * DataNode向NameNode发送生命维持消息，上报节点当前状态信息
   * @param registration DataNode注册信息，包含节点标识
   * @param reports 存储块报告，列出各存储块状态
   * @param dnCacheCapacity DataNode缓存总容量
   * @param dnCacheUsed DataNode已用缓存容量
   * @param xmitsInProgress 当前正在进行的块传输数量
   * @param xceiverCount 当前运行的数据传输线程数
   * @param failedVolumes 失败的卷数量
   * @param volumeFailureSummary 卷失败详细信息
   * @throws IOException RPC调用异常
   */
  @Idempotent
  void sendLifeline(DatanodeRegistration registration, StorageReport[] reports,
      long dnCacheCapacity, long dnCacheUsed, int xmitsInProgress,
      int xceiverCount, int failedVolumes,
      VolumeFailureSummary volumeFailureSummary) throws IOException;
}