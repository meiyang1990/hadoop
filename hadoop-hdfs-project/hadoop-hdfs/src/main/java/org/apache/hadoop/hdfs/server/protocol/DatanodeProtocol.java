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

import java.io.*;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.io.retry.Idempotent;
import org.apache.hadoop.security.KerberosInfo;

import javax.annotation.Nonnull;

/**
 * 数据节点与名称节点之间的RPC通信协议接口
 * 定义了数据节点主动向名称节点上报状态、块信息，以及接收名称节点命令的所有方法
 * 名称节点仅通过方法返回值向数据节点返回指令，不主动发起调用
 */
/**********************************************************************
 * Protocol that a DFS datanode uses to communicate with the NameNode.
 * It's used to upload current load information and block reports.
 *
 * The only way a NameNode can communicate with a DataNode is by
 * returning values from these functions.
 *
 **********************************************************************/
@KerberosInfo(
    serverPrincipal = DFSConfigKeys.DFS_NAMENODE_KERBEROS_PRINCIPAL_KEY, 
    clientPrincipal = DFSConfigKeys.DFS_DATANODE_KERBEROS_PRINCIPAL_KEY)
@InterfaceAudience.Private
public interface DatanodeProtocol {
  /**
   * 协议版本标识，用于兼容不同版本的数据节点和名称节点
   * 修改该协议接口时需要同步更新此版本号，同时需要同步修改DatanodeProtocol.proto中的PB协议定义
   */
  public static final long versionID = 28L;
  
  // 错误码定义
  final static int NOTIFY = 0;
  /** 磁盘错误，节点仍有可用卷 */
  final static int DISK_ERROR = 1;
  final static int INVALID_BLOCK = 2;
  /** 致命磁盘错误，节点无可用卷 */
  final static int FATAL_DISK_ERROR = 3;

  /**
   * 数据节点接收到名称节点命令后，需要执行的动作类型枚举常量
   */
  final static int DNA_UNKNOWN = 0;    // 未知动作
  final static int DNA_TRANSFER = 1;   // 将块传输到其他数据节点
  final static int DNA_INVALIDATE = 2; // 作废块
  final static int DNA_SHUTDOWN = 3;   // 关闭节点
  final static int DNA_REGISTER = 4;   // 重新注册
  final static int DNA_FINALIZE = 5;   // 完成之前的升级
  final static int DNA_RECOVERBLOCK = 6;  // 请求块恢复
  final static int DNA_ACCESSKEYUPDATE = 7;  // 更新访问密钥
  final static int DNA_BALANCERBANDWIDTHUPDATE = 8; // 更新均衡器带宽
  final static int DNA_CACHE = 9;      // 缓存块
  final static int DNA_UNCACHE = 10;   // 移除块缓存
  final static int DNA_ERASURE_CODING_RECONSTRUCTION = 11; // 纠删码重构命令
  int DNA_BLOCK_STORAGE_MOVEMENT = 12; // 块存储迁移命令
  int DNA_DROP_SPS_WORK_COMMAND = 13; // 丢弃SPS工作命令

  /**
   * 数据节点向名称节点注册
   * @param registration 数据节点注册信息
   * @return 更新后的注册信息
   * @throws IOException 通信异常时抛出
   */
  @Idempotent
  public DatanodeRegistration registerDatanode(DatanodeRegistration registration
      ) throws IOException;
  
  /**
   * 数据节点向名称节点发送心跳，上报节点状态，并接收名称节点返回的命令
   * @param registration 数据节点注册信息
   * @param reports 每个存储卷的利用率报告
   * @param dnCacheCapacity 数据节点总缓存容量（字节）
   * @param dnCacheUsed 数据节点已使用缓存大小（字节）
   * @param xmitsInProgress 当前正在进行的数据传输数量
   * @param xceiverCount 活跃传输线程数量
   * @param failedVolumes 故障卷数量
   * @param volumeFailureSummary 卷故障信息摘要
   * @param requestFullBlockReportLease 是否请求全量块报告租约
   * @param slowPeers 检测到的响应缓慢的对等数据节点报告，无则为空
   * @param slowDisks 检测到的慢速磁盘报告，无则为空
   * @return 心跳响应，包含名称Node返回给数据节点的命令列表
   * @throws IOException 通信异常时抛出
   */
  @Idempotent
  public HeartbeatResponse sendHeartbeat(DatanodeRegistration registration,
                                       StorageReport[] reports,
                                       long dnCacheCapacity,
                                       long dnCacheUsed,
                                       int xmitsInProgress,
                                       int xceiverCount,
                                       int failedVolumes,
                                       VolumeFailureSummary volumeFailureSummary,
                                       boolean requestFullBlockReportLease,
                                       @Nonnull SlowPeerReports slowPeers,
                                       @Nonnull SlowDiskReports slowDisks)
      throws IOException;

  /**
   * 数据节点向名称节点上报本节点存储的全量块信息
   * @param registration 数据节点注册信息
   * @param poolId 块池ID
   * @param reports 每个存储卷的块报告，通过压缩格式减少内存占用
   * @param context 块报告上下文信息
   * @return 名称Node返回给数据节点的下一个待执行命令
   * @throws IOException 通信异常时抛出
   */
  @Idempotent
  public DatanodeCommand blockReport(DatanodeRegistration registration,
            String poolId, StorageBlockReport[] reports,
            BlockReportContext context) throws IOException;
    

  /**
   * 数据节点向名称节点上报本节点缓存的所有块ID
   * @param registration 数据节点注册信息
   * @param poolId 块池ID
   * @param blockIds 缓存块ID列表
   * @return 名称Node返回给数据节点的待执行命令
   * @throws IOException 通信异常时抛出
   */
  @Idempotent
  public DatanodeCommand cacheReport(DatanodeRegistration registration,
      String poolId, List<Long> blockIds) throws IOException;

  /**
   * 数据节点向名称节点上报最近收到和删除的块信息
   * @param registration 数据节点注册信息
   * @param poolId 块池ID
   * @param rcvdAndDeletedBlocks 各存储卷的接收和删除块信息
   * @throws IOException 通信异常时抛出
   */
  @Idempotent
  public void blockReceivedAndDeleted(DatanodeRegistration registration,
                            String poolId,
                            StorageReceivedDeletedBlocks[] rcvdAndDeletedBlocks)
                            throws IOException;

  /**
   * 数据节点向名称节点上报错误信息，用于调试
   * @param registration 数据节点注册信息
   * @param errorCode 错误码
   * @param msg 错误描述信息
   * @throws IOException 通信异常时抛出
   */
  @Idempotent
  public void errorReport(DatanodeRegistration registration,
                          int errorCode, 
                          String msg) throws IOException;

  /**
   * 数据节点请求获取名称节点的命名空间信息
   * @return 命名空间信息，包含集群ID、块池ID等
   * @throws IOException 通信异常时抛出
   */
  @Idempotent
  public NamespaceInfo versionRequest() throws IOException;

  /**
   * 数据节点向名称节点上报损坏块
   * @param blocks 损坏块数组
   * @throws IOException 通信异常时抛出
   */
  @Idempotent
  public void reportBadBlocks(LocatedBlock[] blocks) throws IOException;
  
  /**
   * 租约恢复过程中提交块同步，确认块的最终状态
   * @param block 需要同步的块
   * @param newgenerationstamp 新的生成时间戳
   * @param newlength 块的最终长度
   * @param closeFile 是否关闭文件
   * @param deleteblock 是否删除该块
   * @param newtargets 块的新目标数据节点列表
   * @param newtargetstorages 块在目标节点上的存储ID列表
   * @throws IOException 通信异常时抛出
   */
  @Idempotent
  public void commitBlockSynchronization(ExtendedBlock block,
      long newgenerationstamp, long newlength,
      boolean closeFile, boolean deleteblock, DatanodeID[] newtargets,
      String[] newtargetstorages) throws IOException;
}