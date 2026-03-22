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
package org.apache.hadoop.hdfs.server.namenode;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.hadoop.hdfs.server.blockmanagement.BlockUnderConstructionFeature;
import org.apache.hadoop.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.hdfs.BlockReader;
import org.apache.hadoop.hdfs.client.impl.BlockReaderFactory;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.hdfs.RemotePeerFactory;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.net.Peer;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.DatanodeInfoWithStorage;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.SnapshottableDirectoryStatus;
import org.apache.hadoop.hdfs.protocol.datatransfer.sasl.DataEncryptionKeyFactory;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.security.token.block.DataEncryptionKey;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoStriped;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoStriped.StorageAndBlockIndex;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockPlacementPolicies;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockPlacementStatus;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStorageInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.NumberReplicas;
import org.apache.hadoop.hdfs.server.datanode.CachingStrategy;
import org.apache.hadoop.hdfs.util.RwLockMode;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.NodeBase;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.tracing.TraceUtils;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.tracing.Tracer;

import org.apache.hadoop.classification.VisibleForTesting;

/**
 * 文件系统健康检查工具，对HDFS目录树从指定起始路径开始进行完整性扫描，检测各类异常状态并生成统计报告。
 * 可检测的异常包括：完全缺失所有副本的 corrupt 文件、副本数不足/过多的块、不符合块放置策略的块等。
 * 支持对损坏文件执行自动删除或移动到/lost+found目录的修复操作，还支持触发不平衡块的重新复制。
 * <p>核心功能：</p>
 * <ul>
 * <li>检测完全丢失所有块副本的文件<br>
 * 针对损坏文件支持两种处理动作：
 *  <ul>
 *      <li>将剩余可 salvag 块移动到/lost+found目录 ({@link #doMove})</li>
 *      <li>直接删除整个损坏文件 ({@link #doDelete})</li>
 *  </ul>
 *  </li>
 *  <li>检测副本不足、副本过多、放置不符合策略的块</li>
 *  </ul>
 *  工具会收集整个文件系统的全局统计信息，也支持输出每个文件块的位置、副本状态等详细信息。
 */
@InterfaceAudience.Private
public class NamenodeFsck implements DataEncryptionKeyFactory {
  public static final Logger LOG =
      LoggerFactory.getLogger(NameNode.class.getName());

  // FSCK状态标记常量
  public static final String CORRUPT_STATUS = "is CORRUPT";
  public static final String HEALTHY_STATUS = "is HEALTHY";
  public static final String DECOMMISSIONING_STATUS = "is DECOMMISSIONING";
  public static final String DECOMMISSIONED_STATUS = "is DECOMMISSIONED";
  public static final String ENTERING_MAINTENANCE_STATUS =
      "is ENTERING MAINTENANCE";
  public static final String IN_MAINTENANCE_STATUS = "is IN MAINTENANCE";
  public static final String STALE_STATUS = "is STALE";
  public static final String EXCESS_STATUS = "is EXCESS";
  public static final String NONEXISTENT_STATUS = "does not exist";
  public static final String FAILURE_STATUS = "FAILED";
  public static final String UNDEFINED = "undefined";

  private final NameNode namenode;
  private final BlockManager blockManager;
  private final NetworkTopology networktopology;
  private final int totalDatanodes;
  private final InetAddress remoteAddress;

  private long totalDirs = 0L;
  private long totalSymlinks = 0L;

  private String lostFound = null;
  private boolean lfInited = false;
  private boolean lfInitedOk = false;
  private boolean showFiles = false;
  private boolean showOpenFiles = false;
  private boolean showBlocks = false;
  private boolean showLocations = false;
  private boolean showRacks = false;
  private boolean showStoragePolcies = false;
  private boolean showCorruptFileBlocks = false;

  private boolean showReplicaDetails = false;
  private boolean showUpgradeDomains = false;
  private boolean showMaintenanceState = false;
  private long staleInterval;
  private Tracer tracer;
  private String auditSource;

  /**
   * FSCK执行过程中是否发生内部错误（如删除损坏文件失败）
   */
  private boolean internalError = false;

  /**
   * 是否开启move模式：开启后将损坏文件剩余块复制到/lost+found目录
   */
  private boolean doMove = false;

  /**
   * 是否开启delete模式：开启后直接删除所有检测到的损坏文件
   */
  private boolean doDelete = false;

  /**
   * 是否开启replicate模式：开启后对不符合放置策略的块触发重新复制，让块布局符合策略要求
   */
  private boolean doReplicate = false;

  String path = "/";

  private String[] blockIds = null;

  // 用于列出损坏块的分页cookie，支持断点续查，保存上一次查询最后返回的块ID
  private final String[] currentCookie = new String[] { null };

  private final Configuration conf;
  private final PrintWriter out;
  private List<String> snapshottableDirs = null;

  private final BlockPlacementPolicies bpPolicies;
  private StoragePolicySummary storageTypeSummary = null;

  /**
   * 构造NamenodeFsck实例，解析URL参数初始化检查配置
   * @param conf NameNode配置对象
   * @param namenode 本次检查所属的NameNode实例
   * @param networktopology 集群网络拓扑信息
   * @param pmap HTTP请求参数map，保存fsck命令行参数
   * @param out 输出检查结果的打印流
   * @param totalDatanodes 当前集群存活DataNode总数
   * @param remoteAddress 发起fsck请求的客户端地址
   */
  NamenodeFsck(Configuration conf, NameNode namenode,
      NetworkTopology networktopology,
      Map<String, String[]> pmap, PrintWriter out,
      int totalDatanodes, InetAddress remoteAddress) {
    this.conf = conf;
    this.namenode = namenode;
    this.blockManager = namenode.getNamesystem().getBlockManager();
    this.networktopology = networktopology;
    this.out = out;
    this.totalDatanodes = totalDatanodes;
    this.remoteAddress = remoteAddress;
    this.bpPolicies = new BlockPlacementPolicies(conf, null,
        networktopology,
        namenode.getNamesystem().getBlockManager().getDatanodeManager()
        .getHost2DatanodeMap());
    this.staleInterval =
        conf.getLong(DFSConfigKeys.DFS_NAMENODE_STALE_DATANODE_INTERVAL_KEY,
          DFSConfigKeys.DFS_NAMENODE_STALE_DATANODE_INTERVAL_DEFAULT);
    this.tracer = new Tracer.Builder("NamenodeFsck").
        conf(TraceUtils.wrapHadoopConf("namenode.fsck.htrace.", conf)).
        build();

    // 遍历解析所有请求参数
    for (Iterator<String> it = pmap.keySet().iterator(); it.hasNext();) {
      String key = it.next();
      if (key.equals("path")) { this.path = pmap.get("path")[0]; }
      else if (key.equals("move")) { this.doMove = true; }
      else if (key.equals("delete")) { this.doDelete = true; }
      else if (key.equals("files")) { this.showFiles = true; }
      else if (key.equals("blocks")) { this.showBlocks = true; }
      else if (key.equals("locations")) { this.showLocations = true; }
      else if (key.equals("racks")) { this.showRacks = true; }
      else if (key.equals("replicadetails")) {
        this.showReplicaDetails = true;
      } else if (key.equals("upgradedomains")) {
        this.showUpgradeDomains = true;
      } else if (key.equals("maintenance")) {
        this.showMaintenanceState = true;
      } else if (key.equals("storagepolicies")) {
        this.showStoragePolcies = true; }
      else if (key.equals("showprogress")) {
        out.println("The fsck switch -showprogress is deprecated and no " +
                "longer has any effect. Progress is now shown by default.");
        LOG.warn("The fsck switch -showprogress is deprecated and no longer " +
            "has any effect. Progress is now shown by default.");
      } else if (key.equals("openforwrite")) {
        this.showOpenFiles = true;
      } else if (key.equals("listcorruptfileblocks")) {
        this.showCorruptFileBlocks = true;
      } else if (key.equals("startblockafter")) {
        this.currentCookie[0] = pmap.get("startblockafter")[0];
      } else if (key.equals("includeSnapshots")) {
        this.snapshottableDirs = new ArrayList<String>();
      } else if (key.equals("blockId")) {
        this.blockIds = pmap.get("blockId")[0].split(" ");
      } else if (key.equals("replicate")) {
        this.doReplicate = true;
      }
    }
    // 生成审计日志源标识
    this.auditSource = (blockIds != null)
        ? "blocksIds=" + Arrays.asList(blockIds) : path;
  }

  /**
   * 获取本次fsck请求的审计源信息
   * @return 审计源字符串，包含检查路径或待检查块ID列表
   */
  public String getAuditSource() {
    return auditSource;
  }

  /**
   * 根据输入块ID，检查单个块的状态与副本分布信息
   * @param blockId 待检查块ID字符串
   */
  public void blockIdCK(String blockId) {

    if(blockId == null) {
      out.println("Please provide valid blockId!");
      return;
    }

    // 获取NameNode全局读锁
    namenode.getNamesystem().readLock(RwLockMode.GLOBAL);
    try {
      // 根据输入字符串构造Block对象
      Block block = new Block(Block.getBlockId(blockId));
      // 从BlockManager获取块元数据
      BlockInfo blockInfo = blockManager.getStoredBlock(block);
      if (blockInfo == null || blockInfo.isDeleted()) {
        out.println("Block "+ blockId +" " + NONEXISTENT_STATUS);
        LOG.warn("Block "+ blockId + " " + NONEXISTENT_STATUS);
        return;
      }
      // 获取块所属文件INode
      final INodeFile iNode = namenode.getNamesystem().getBlockCollection(blockInfo);
      // 统计各类状态的副本数量
      NumberReplicas numberReplicas= blockManager.countNodes(blockInfo);
      // 输出块基本信息
      out.println("Block Id: " + blockId);
      out.println("Block belongs to: "+iNode.getFullPathName());
      out.println("No. of Expected Replica: " +
          blockManager.getExpectedRedundancyNum(blockInfo));
      out.println("No. of live Replica: " + numberReplicas.liveReplicas());
      out.println("No. of excess Replica: " + numberReplicas.excessReplicas());
      out.println("No. of stale Replica: " +
          numberReplicas.replicasOnStaleNodes());
      out.println("No. of decommissioned Replica: "
          + numberReplicas.decommissioned());
      out.println("No. of decommissioning Replica: "
          + numberReplicas.decommissioning());
      if (this.showMaintenanceState) {
        out.println("No. of entering maintenance Replica: "
            + numberReplicas.liveEnteringMaintenanceReplicas());
        out.println("No. of in maintenance Replica: "
            + numberReplicas.maintenanceNotForReadReplicas());
      }
      out.println("No. of corrupted Replica: " +
          numberReplicas.corruptReplicas());
      // 纠删码块额外输出冗余内部块统计
      if (blockInfo.isStriped()) {
        out.println("No. of redundant Replica: " + numberReplicas.redundantInternalBlocks());
      }
      // 获取损坏副本记录
      Collection<DatanodeDescriptor> corruptionRecord = null;
      if (blockManager.getCorruptReplicas(block) != null) {
        corruptionRecord = blockManager.getCorruptReplicas(block);
      }
      // 遍历每个存储该块的DataNode，输出每个副本状态
      if (blockInfo.isStriped()) {
        for (int idx = (blockInfo.getCapacity() - 1); idx >= 0; idx--) {
          DatanodeDescriptor dn = blockInfo.getDatanode(idx);
          if (dn == null) {
            continue;
          }
          printDatanodeReplicaStatus(block, corruptionRecord, dn);
        }
      } else {
        for (int idx = (blockInfo.numNodes() - 1); idx >= 0; idx--) {
          DatanodeDescriptor dn = blockInfo.getDatanode(idx);
          printDatanodeReplicaStatus(block, corruptionRecord, dn);
        }
      }
    } catch (Exception e) {
      String errMsg = "Fsck on blockId '" + blockId;
      LOG.warn(errMsg, e);
      out.println(e.getMessage());
      out.print("\n\n" + errMsg);
      LOG.warn("Error in looking up block", e);
    } finally {
      // 释放NameNode读锁
      namenode.getNamesystem().readUnlock(RwLockMode.GLOBAL, "fsck");
    }
  }

  /**
   * 打印单个DataNode上块副本的状态信息
   * @param block 待检查块
   * @param corruptionRecord 损坏副本列表
   * @param dn 目标DataNode描述信息
   */
  private void printDatanodeReplicaStatus(Block block,
      Collection<DatanodeDescriptor> corruptionRecord, DatanodeDescriptor dn) {
    out.print("Block replica on datanode/rack: " + dn.getHostName() +
        dn.getNetworkLocation() + " ");
    // 根据DataNode和副本状态输出对应标记
    if (corruptionRecord != null && corruptionRecord.contains(dn)) {
      out.print(CORRUPT_STATUS + "\t ReasonCode: "