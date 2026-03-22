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
package org.apache.hadoop.hdfs.server.blockmanagement;

import static org.apache.hadoop.hdfs.server.protocol.DatanodeProtocol.DNA_ERASURE_CODING_RECONSTRUCTION;
import static org.apache.hadoop.util.Time.monotonicNow;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.util.Preconditions;

import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableList;
import org.apache.hadoop.thirdparty.com.google.common.net.InetAddresses;

import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.net.DFSNetworkTopology;
import org.apache.hadoop.hdfs.protocol.*;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo.DatanodeInfoBuilder;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor.BlockTargetPair;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor.CachedBlocksList;
import org.apache.hadoop.hdfs.server.common.Util;
import org.apache.hadoop.hdfs.server.namenode.CachedBlock;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.Namesystem;
import org.apache.hadoop.hdfs.server.protocol.*;
import org.apache.hadoop.hdfs.server.protocol.BlockECReconstructionCommand.BlockECReconstructionInfo;
import org.apache.hadoop.hdfs.server.protocol.BlockRecoveryCommand.RecoveringBlock;
import org.apache.hadoop.hdfs.server.protocol.BlockRecoveryCommand.RecoveringStripedBlock;
import org.apache.hadoop.hdfs.util.RwLockMode;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.net.*;
import org.apache.hadoop.net.NetworkTopology.InvalidTopologyException;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Sets;
import org.apache.hadoop.util.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * HDFS NameNode端数据节点管理器，负责管理集群中所有Datanode的生命周期、状态维护、心跳处理、拓扑解析等核心逻辑。
 * 核心职责包括：Datanode注册注销管理、节点状态维护（退役/维护/ stale节点处理）、块位置排序、慢速节点追踪、拓扑位置解析。
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class DatanodeManager {
  static final Logger LOG = LoggerFactory.getLogger(DatanodeManager.class);

  private final Namesystem namesystem;
  private final BlockManager blockManager;
  private final DatanodeAdminManager datanodeAdminManager;
  private final HeartbeatManager heartbeatManager;
  private final FSClusterStats fsClusterStats;

  private volatile long heartbeatIntervalSeconds;
  private volatile int heartbeatRecheckInterval;
  /**
   * Stores the datanode -> block map.  
   * <p>
   * Done by storing a set of {@link DatanodeDescriptor} objects, sorted by 
   * storage id. In order to keep the storage map consistent it tracks 
   * all storages ever registered with the namenode.
   * A descriptor corresponding to a specific storage id can be
   * <ul> 
   * <li>added to the map if it is a new storage id;</li>
   * <li>updated with a new datanode started as a replacement for the old one 
   * with the same storage id; and </li>
   * <li>removed if and only if an existing datanode is restarted to serve a
   * different storage id.</li>
   * </ul> <br> 
   * <p>
   * Mapping: StorageID -> DatanodeDescriptor
   */
  private final Map<String, DatanodeDescriptor> datanodeMap
      = new HashMap<>();

  /** Cluster network topology. */
  private final NetworkTopology networktopology;

  /** Host names to datanode descriptors mapping. */
  private final Host2NodesMap host2DatanodeMap = new Host2NodesMap();

  private final DNSToSwitchMapping dnsToSwitchMapping;
  private final boolean rejectUnresolvedTopologyDN;

  private final int defaultXferPort;
  
  private final int defaultInfoPort;

  private final int defaultInfoSecurePort;

  private final int defaultIpcPort;

  /** Read include/exclude files. */
  private HostConfigManager hostConfigManager;

  /** The period to wait for datanode heartbeat.*/
  private long heartbeatExpireInterval;
  /** Ask Datanode only up to this many blocks to delete. */
  private volatile int blockInvalidateLimit;

  /** The interval for judging stale DataNodes for read/write */
  private final long staleInterval;
  
  /** Whether or not to avoid using stale DataNodes for reading */
  private final boolean avoidStaleDataNodesForRead;

  /** Whether or not to avoid using slow DataNodes for reading. */
  private volatile boolean avoidSlowDataNodesForRead;

  /** Whether or not to consider lad for reading. */
  private final boolean readConsiderLoad;

  /** Whether or not to consider storageType for reading. */
  private final boolean readConsiderStorageType;

  /**
   * Whether or not to avoid using stale DataNodes for writing.
   * Note that, even if this is configured, the policy may be
   * temporarily disabled when a high percentage of the nodes
   * are marked as stale.
   */
  private final boolean avoidStaleDataNodesForWrite;

  /**
   * When the ratio of stale datanodes reaches this number, stop avoiding 
   * writing to stale datanodes, i.e., continue using stale nodes for writing.
   */
  private final float ratioUseStaleDataNodesForWrite;

  /** The number of stale DataNodes */
  private volatile int numStaleNodes;

  /** The number of stale storages */
  private volatile int numStaleStorages;

  /**
   * Number of blocks to check for each postponedMisreplicatedBlocks iteration
   */
  private final long blocksPerPostponedMisreplicatedBlocksRescan;

  /**
   * Whether or not this cluster has ever consisted of more than 1 rack,
   * according to the NetworkTopology.
   */
  private boolean hasClusterEverBeenMultiRack = false;

  private final boolean checkIpHostnameInRegistration;
  /**
   * Whether we should tell datanodes what to cache in replies to
   * heartbeat messages.
   */
  private boolean shouldSendCachingCommands = false;

  /**
   * The number of datanodes for each software version. This list should change
   * during rolling upgrades.
   * Software version -> Number of datanodes with this version
   */
  private final HashMap<String, Integer> datanodesSoftwareVersions =
    new HashMap<>(4, 0.75f);

  /**
   *  True if we should process latency metrics from individual DN disks.
   */
  private final boolean dataNodeDiskStatsEnabled;

  /**
   * If we use DfsNetworkTopology to choose nodes for placing replicas.
   */
  private final boolean useDfsNetworkTopology;

  private static final String IP_PORT_SEPARATOR = ":";

  @Nullable
  private SlowPeerTracker slowPeerTracker;
  private static Set<String> slowNodesUuidSet = Sets.newConcurrentHashSet();
  private Daemon slowPeerCollectorDaemon;
  private volatile long slowPeerCollectionInterval;
  private volatile int maxSlowPeerReportNodes;

  @Nullable
  private final SlowDiskTracker slowDiskTracker;
  
  /**
   * The minimum time between resending caching directives to Datanodes,
   * in milliseconds.
   *
   * Note that when a rescan happens, we will send the new directives
   * as soon as possible.  This timeout only applies to resending 
   * directives that we've already sent.
   */
  private final long timeBetweenResendingCachingDirectivesMs;

  private final boolean randomNodeOrderEnabled;

  /**
   * 构造DatanodeManager，初始化集群拓扑、心跳管理器、管理员管理器等核心组件，加载配置参数。
   * @param blockManager 块管理器
   * @param namesystem NameNode命名空间
   * @param conf Hadoop配置
   * @throws IOException 初始化IO异常
   */
  DatanodeManager(final BlockManager blockManager, final Namesystem namesystem,
      final Configuration conf) throws IOException {
    this.namesystem = namesystem;
    this.blockManager = blockManager;

    this.useDfsNetworkTopology = conf.getBoolean(
        DFSConfigKeys.DFS_USE_DFS_NETWORK_TOPOLOGY_KEY,
        DFSConfigKeys.DFS_USE_DFS_NETWORK_TOPOLOGY_DEFAULT);
    // 根据配置选择网络拓扑实现类
    if (useDfsNetworkTopology) {
      networktopology = DFSNetworkTopology.getInstance(conf);
    } else {
      networktopology = NetworkTopology.getInstance(conf);
    }
    this.heartbeatManager = new HeartbeatManager(namesystem,
        blockManager, conf);
    this.datanodeAdminManager = new DatanodeAdminManager(namesystem,
        blockManager, heartbeatManager);
    this.fsClusterStats = newFSClusterStats();
    this.dataNodeDiskStatsEnabled = Util.isDiskStatsEnabled(conf.getInt(
        DFSConfigKeys.DFS_DATANODE_FILEIO_PROFILING_SAMPLING_PERCENTAGE_KEY,
        DFSConfigKeys.
            DFS_DATANODE_FILEIO_PROFILING_SAMPLING_PERCENTAGE_DEFAULT));
    final Timer timer = new Timer();
    final boolean dataNodePeerStatsEnabledVal =
        conf.getBoolean(DFSConfigKeys.DFS_DATANODE_PEER_STATS_ENABLED_KEY,
            DFSConfigKeys.DFS_DATANODE_PEER_STATS_ENABLED_DEFAULT);
    this.maxSlowPeerReportNodes = conf.getInt(
        DFSConfigKeys.DFS_NAMENODE_MAX_SLOWPEER_COLLECT_NODES_KEY,
        DFSConfigKeys.DFS_NAMENODE_MAX_SLOWPEER_COLLECT_NODES_DEFAULT);
    this.slowPeerCollectionInterval = conf.getTimeDuration(
        DFSConfigKeys.DFS_NAMENODE_SLOWPEER_COLLECT_INTERVAL_KEY,
        DFSConfigKeys.DFS_NAMENODE_SLOWPEER_COLLECT_INTERVAL_DEFAULT,
        TimeUnit.MILLISECONDS);
    // 初始化慢速对等节点追踪器
    initSlowPeerTracker(conf, timer, dataNodePeerStatsEnabledVal);
    // 初始化慢速磁盘追踪器
    this.slowDiskTracker = dataNodeDiskStatsEnabled ?
        new SlowDiskTracker(conf, timer) : null;
    // 读取默认端口配置
    this.defaultXferPort = NetUtils.createSocketAddr(
          conf.getTrimmed(DFSConfigKeys.DFS_DATANODE_ADDRESS_KEY,
              DFSConfigKeys.DFS_DATANODE_ADDRESS_DEFAULT)).getPort();
    this.defaultInfoPort = NetUtils.createSocketAddr(
          conf.getTrimmed(DFSConfigKeys.DFS_DATANODE_HTTP_ADDRESS_KEY,
              DFSConfigKeys.DFS_DATANODE_HTTP_ADDRESS_DEFAULT)).getPort();
    this.defaultInfoSecurePort = NetUtils.createSocketAddr(
        conf.getTrimmed(DFSConfigKeys.DFS_DATANODE_HTTPS_ADDRESS_KEY,
            DFSConfigKeys.DFS_DATANODE_HTTPS_ADDRESS_DEFAULT)).getPort();
    this.defaultIpcPort = NetUtils.createSocketAddr(
          conf.getTrimmed(DFSConfigKeys.DFS_DATANODE_IPC_ADDRESS_KEY,
              DFSConfigKeys.DFS_DATANODE_IPC_ADDRESS_DEFAULT)).getPort();
    // 初始化主机配置管理器，加载include/exclude列表
    this.hostConfigManager = ReflectionUtils.newInstance(
        conf.getClass(DFSConfigKeys.DFS_NAMENODE_HOSTS_PROVIDER_CLASSNAME_KEY,
            HostFileManager.class, HostConfigManager.class), conf);
    try {
      this.hostConfigManager.refresh();
    } catch (IOException e) {
      LOG.error("error reading hosts files: ", e);
    }
    // 初始化机架解析映射
    this.dnsToSwitchMapping = ReflectionUtils.newInstance(
        conf.getClass(DFSConfigKeys.NET_TOPOLOGY_NODE_SWITCH_MAPPING_IMPL_KEY, 
            ScriptBasedMapping.class, DNSToSwitchMapping.class), conf);
    this.rejectUnresolvedTopologyDN = conf.getBoolean(
        DFSConfigKeys.DFS_REJECT_UNRESOLVED_DN_TOPOLOGY_MAPPING_KEY,
        DFSConfigKeys.DFS_REJECT_UNRESOLVED_DN_TOPOLOGY_MAPPING_DEFAULT);
    
    // 如果机架映射支持缓存，预解析include列表中的主机并缓存
    if (dnsToSwitchMapping instanceof CachedDNSToSwitchMapping) {
      final ArrayList<String> locations = new ArrayList<>();
      for (InetSocketAddress addr : hostConfigManager.getIncludes()) {
        locations.add(addr.getAddress().getHostAddress());
      }
      dnsToSwitchMapping.resolve(locations);
    }
    // 读取心跳相关配置
    heartbeatIntervalSeconds = conf.getTimeDuration(
        DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY,
        DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_DEFAULT, TimeUnit.SECONDS);
    heartbeatRecheckInterval = conf.getInt(
        DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY, 
        DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_DEFAULT); // 5 minutes
    this.heartbeatExpireInterval = 2 * heartbeatRecheckInterval
        + 10 * 1000 * heartbeatIntervalSeconds;
    final int configuredBlockInvalidateLimit = conf.getInt(
        DFSConfigKeys.DFS_BLOCK_INVALIDATE_LIMIT_KEY,
        DFSConfigKeys.DFS_BLOCK_INVALIDATE_LIMIT_DEFAULT);
    // Block invalidate limit also has some dependency on heartbeat interval.
    // Check setBlockInvalidateLimit().
    setBlockInvalidateLimit(configuredBlockInvalidateLimit);
    this.checkIpHostnameInRegistration = conf.getBoolean(
        DFSConfigKeys.DFS_NAMENODE_DATANODE_REGISTRATION_IP_HOSTNAME_CHECK_KEY,
        DFSConfigKeys.DFS_NAMENODE_DATANODE_REGISTRATION_IP_HOSTNAME_CHECK_DEFAULT);
    LOG.info(DFSConfigKeys.DFS_NAMENODE_DATANODE_REGISTRATION_IP_HOSTNAME_CHECK_KEY
        + "=" + checkIpHostnameInRegistration);
    // 读取stale节点相关配置
    this.avoidStaleDataNodesForRead = conf.getBoolean(
        DFSConfigKeys.DFS_NAMENODE_AVOID_STALE_DATANODE_FOR_READ_KEY,
        DFSConfigKeys.DFS_NAMENODE_AVOID_STALE_DATANODE_FOR_READ_DEFAULT);
    this.avoidSlowDataNodesForRead = conf.getBoolean(
        DFSConfigKeys.DFS_NAMENODE_AVOID_SLOW_DATANODE_FOR_READ_KEY,
        DFSConfigKeys.DFS_NAMENODE_AVOID_SLOW_DATANODE_FOR_READ_DEFAULT);
    this.readConsiderLoad = conf.getBoolean(
        DFSConfigKeys.DFS_NAMENODE_READ_CONSIDERLOAD_KEY,
        DFSConfigKeys.DFS_NAMENODE_READ_CONSIDERLOAD_DEFAULT);
    this.readConsiderStorageType = conf.getBoolean(
        DFSConfigKeys.DFS_NAMENODE_READ_CONSIDERSTORAGETYPE_KEY,
        DFSConfigKeys.DFS_NAMENODE_READ_CONSIDERSTORAGETYPE_DEFAULT);
    // 负载感知和存储类型感知不兼容，同时开启时给出警告
    if (readConsiderLoad && readConsiderStorageType) {
      LOG.warn(
          "{} and {} are incompatible and only one can be enabled. "
              + "Both are currently enabled. {} will be ignored.",
          DFSConfigKeys.DFS_NAMENODE_READ_CONSIDERLOAD_KEY,
          DFSConfigKeys.DFS_NAMENODE_READ_CONSIDERSTORAGETYPE_KEY,
          DFSConfigKeys.DFS_NAMENODE_READ_CONSIDERSTORAGETYPE_KEY);
    }
    this.avoidStaleDataNodesForWrite = conf.getBoolean(
        DFSConfigKeys.DFS_NAMENODE_AVOID_STALE_DATANODE_FOR_WRITE_KEY,
        D