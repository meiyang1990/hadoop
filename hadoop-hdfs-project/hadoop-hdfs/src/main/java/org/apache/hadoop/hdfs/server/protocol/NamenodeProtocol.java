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
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.security.token.block.ExportedBlockKeys;
import org.apache.hadoop.hdfs.server.namenode.CheckpointSignature;
import org.apache.hadoop.hdfs.server.namenode.NNStorage;
import org.apache.hadoop.hdfs.server.namenode.ha.ReadOnly;
import org.apache.hadoop.io.retry.AtMostOnce;
import org.apache.hadoop.io.retry.Idempotent;
import org.apache.hadoop.security.KerberosInfo;

/**
 * 文件级注释：Namenode节点间通信RPC协议接口，用于备用节点、 SecondaryNameNode与活动NameNode通信
 * 核心功能：支持辅助节点获取NameNode状态、执行检查点、同步元数据等操作，同时也被外部存储策略满足器使用
 */
/*****************************************************************************
 * Protocol that a secondary NameNode uses to communicate with the NameNode.
 * Also used by external storage policy satisfier. It's used to get part of the
 * name node state
 *****************************************************************************/
@KerberosInfo(
    serverPrincipal = DFSConfigKeys.DFS_NAMENODE_KERBEROS_PRINCIPAL_KEY)
@InterfaceAudience.Private
/**
 * 类级注释：NameNode协议接口，定义了从属节点（SecondaryNameNode、备份节点）与活动NameNode之间的RPC交互规范
 * 核心职责：为元数据检查点、日志同步、集群平衡提供通信接口，支撑HDFS元数据的高可用与容错能力
 */
public interface NamenodeProtocol {
  /**
   * 协议版本ID，版本6开始使用基于事务ID的镜像和编辑日志命名方式
   * Until version 6L, this class served as both
   * the client interface to the NN AND the RPC protocol used to 
   * communicate with the NN.
   * 
   * This class is used by both the DFSClient and the 
   * NN server side to insulate from the protocol serialization.
   * 
   * If you are adding/changing NN's interface then you need to 
   * change both this class and ALSO related protocol buffer
   * wire protocol definition in NamenodeProtocol.proto.
   * 
   * For more details on protocol buffer wire protocol, please see 
   * .../org/apache/hadoop/hdfs/protocolPB/overview.html
   * 
   * 6: Switch to txid-based file naming for image and edits
   */
  public static final long versionID = 6L;

  // Error codes passed by errorReport().
  final static int NOTIFY = 0;
  final static int FATAL = 1;

  public final static int ACT_UNKNOWN = 0;    // unknown action   
  public final static int ACT_SHUTDOWN = 50;   // shutdown node
  public final static int ACT_CHECKPOINT = 51;   // do checkpoint

  /**
   * 函数级注释：为HDFS平衡器获取指定DataNode上指定存储类型的块信息，总大小达到要求值
   * @param datanode 目标DataNode信息
   * @param size 需要获取的块总大小
   * @param minBlockSize 块的最小大小过滤条件
   * @param hotBlockTimeInterval 热块时间间隔，优先选择冷文件中的块
   * @param storageType 目标存储类型
   * @return 包含块信息和位置的对象
   * @throws IOException 参数非法或DataNode不存在时抛出异常
   */
  @Idempotent
  @ReadOnly
  BlocksWithLocations getBlocks(DatanodeInfo datanode, long size, long
      minBlockSize, long hotBlockTimeInterval, StorageType storageType) throws IOException;

  /**
   * 函数级注释：获取当前NameNode的数据块密钥信息，用于数据块访问认证
   * @return 导出的块密钥对象
   * @throws IOException IO异常
   */
  @Idempotent
  public ExportedBlockKeys getBlockKeys() throws IOException;

  /**
   * 函数级注释：获取已同步到持久化存储的最新事务ID
   * @return 最新同步事务ID
   * @throws IOException IO异常
   */
  @Idempotent
  public long getTransactionID() throws IOException;

  /**
   * 函数级注释：获取最近一次检查点的事务ID
   * @return 最近检查点事务ID
   * @throws IOException IO异常
   */
  @Idempotent
  public long getMostRecentCheckpointTxId() throws IOException;

  /**
   * 函数级注释：获取指定类型NameNode文件最近一次检查点的事务ID
   * @param nnf NameNode文件类型
   * @return 对应文件的最近检查点事务ID
   * @throws IOException IO异常
   */
  @Idempotent
  long getMostRecentNameNodeFileTxId(NNStorage.NameNodeFile nnf) throws IOException;

  /**
   * 函数级注释：关闭当前编辑日志并打开新日志，生成检查点签名，安全模式下会失败
   * @return 唯一标识本次检查点的签名
   * @throws IOException 安全模式或IO异常
   */
  @Idempotent
  public CheckpointSignature rollEditLog() throws IOException;

  /**
   * 函数级注释：请求获取NameNode版本和存储信息
   * @return 命名空间信息对象，包含版本和存储信息
   * @throws IOException IO异常
   */
  @Idempotent
  public NamespaceInfo versionRequest() throws IOException;

  /**
   * 函数级注释：向活动NameNode上报从属节点发生的错误，NameNode会根据错误码决定是否注销该节点
   * @param registration 上报节点的注册信息
   * @param errorCode 错误码（NOTIFY/FATAL）
   * @param msg 错误描述信息
   * @throws IOException IO异常
   */
  @Idempotent
  public void errorReport(NamenodeRegistration registration,
                          int errorCode, 
                          String msg) throws IOException;

  /**
   * 函数级注释：注册从属NameNode（如备份节点）到活动NameNode
   * @param registration 从属节点的注册信息
   * @return 注册完成后返回活动NameNode的注册信息
   * @throws IOException IO异常
   */
  @Idempotent
  public NamenodeRegistration registerSubordinateNamenode(
      NamenodeRegistration registration) throws IOException;

  /**
   * 函数级注释：从属节点请求活动NameNode开始一次检查点，NameNode决定是否允许
   * @param registration 请求节点的注册信息
   * @return 检查点命令，包含检查点执行要求；若不允许则返回关闭命令
   * @throws IOException IO异常
   */
  @AtMostOnce
  public NamenodeCommand startCheckpoint(NamenodeRegistration registration)
  throws IOException;

  /**
   * 函数级注释：从属节点请求活动NameNode完成之前开始的检查点
   * @param registration 请求节点的注册信息
   * @param sig 本次检查点的签名标识
   * @throws IOException IO异常
   */
  @AtMostOnce
  public void endCheckpoint(NamenodeRegistration registration,
                            CheckpointSignature sig) throws IOException;
  
  
  /**
   * 函数级注释：获取NameNode可用编辑日志的清单，用于从NameNode同步日志
   * @param sinceTxId 只返回包含事务ID大于等于该值的日志
   * @return 远程编辑日志清单
   * @throws IOException IO异常
   */
  @Idempotent
  public RemoteEditLogManifest getEditLogManifest(long sinceTxId)
    throws IOException;

  /**
   * 函数级注释：查询HDFS升级是否已经完成
   * @return true表示升级已完成，false表示升级中
   * @throws IOException IO异常
   */
  @Idempotent
  public boolean isUpgradeFinalized() throws IOException;

  /**
   * 函数级注释：查询是否正在进行滚动升级
   * @return true表示滚动升级进行中，false表示未进行滚动升级
   * @throws IOException IO异常
   */
  @Idempotent
  boolean isRollingUpgrade() throws IOException;

  /**
   * 函数级注释：获取下一个待处理的存储策略满足器(SPS)路径，供外部SPS服务使用
   * @return 下一个路径ID，没有待处理路径则返回null
   * @throws IOException IO异常
   */
  @AtMostOnce
  Long getNextSPSPath() throws IOException;
}