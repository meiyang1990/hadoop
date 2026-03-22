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
package org.apache.hadoop.hdfs.server.balancer;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.util.Preconditions;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.RateLimiter;
import org.apache.hadoop.ha.HAServiceProtocol;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HAUtil;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsServerDefaults;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StreamCapabilities.StreamCapability;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.NameNodeProxies;
import org.apache.hadoop.hdfs.protocol.AlreadyBeingCreatedException;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.protocol.RollingUpgradeInfo;
import org.apache.hadoop.hdfs.server.protocol.BalancerProtocols;
import org.apache.hadoop.hdfs.server.protocol.BlocksWithLocations;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorageReport;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.ipc.RemoteException;

import org.apache.hadoop.classification.VisibleForTesting;

/**
 * 文件: NameNodeConnector.java
 * 所属模块: HDFS 数据均衡器核心模块
 * 核心职责: 封装平衡器对NameNode的访问逻辑，管理与NameNode的连接、实例互斥锁和负载均衡统计信息
 * 为平衡器提供统一的NameNode访问入口，支持HA集群从Standby节点获取块信息降低Active节点压力
 */
/**
 * The class provides utilities for accessing a NameNode.
 */
@InterfaceAudience.Private
public class NameNodeConnector implements Closeable {
  private static final Logger LOG =
      LoggerFactory.getLogger(NameNodeConnector.class);

  public static final int DEFAULT_MAX_IDLE_ITERATIONS = 5;
  private static boolean write2IdFile = true;
  private static boolean checkOtherInstanceRunning = true;

  /** 创建多个NameNode的连接器集合，用于联邦场景 */
  /** Create {@link NameNodeConnector} for the given namenodes. */
  public static List<NameNodeConnector> newNameNodeConnectors(
      Collection<URI> namenodes, String name, Path idPath, Configuration conf,
      int maxIdleIterations) throws IOException {
    final List<NameNodeConnector> connectors = new ArrayList<NameNodeConnector>(
        namenodes.size());
    for (URI uri : namenodes) {
      NameNodeConnector nnc = new NameNodeConnector(name, uri, idPath,
          null, conf, maxIdleIterations);
      nnc.getKeyManager().startBlockKeyUpdater();
      connectors.add(nnc);
    }
    return connectors;
  }

  /** 创建带指定目标路径的多个NameNode连接器 */
  public static List<NameNodeConnector> newNameNodeConnectors(
      Map<URI, List<Path>> namenodes, String name, Path idPath,
      Configuration conf, int maxIdleIterations) throws IOException {
    final List<NameNodeConnector> connectors = new ArrayList<NameNodeConnector>(
        namenodes.size());
    for (Map.Entry<URI, List<Path>> entry : namenodes.entrySet()) {
      NameNodeConnector nnc = new NameNodeConnector(name, entry.getKey(),
          idPath, entry.getValue(), conf, maxIdleIterations);
      nnc.getKeyManager().startBlockKeyUpdater();
      connectors.add(nnc);
    }
    return connectors;
  }

  /** 创建带命名空间ID的多个NameNode连接器，用于HA联邦场景 */
  public static List<NameNodeConnector> newNameNodeConnectors(
      Collection<URI> namenodes, Collection<String> nsIds, String name,
      Path idPath, Configuration conf, int maxIdleIterations)
      throws IOException {
    final List<NameNodeConnector> connectors = new ArrayList<NameNodeConnector>(
        namenodes.size());
    Map<URI, String> uriToNsId = new HashMap<>();
    if (nsIds != null) {
      for (URI uri : namenodes) {
        for (String nsId : nsIds) {
          if (uri.getAuthority().equals(nsId)) {
            uriToNsId.put(uri, nsId);
          }
        }
      }
    }
    for (URI uri : namenodes) {
      String nsId = uriToNsId.get(uri);
      NameNodeConnector nnc = new NameNodeConnector(name, uri, nsId, idPath,
          null, conf, maxIdleIterations);
      nnc.getKeyManager().startBlockKeyUpdater();
      connectors.add(nnc);
    }
    return connectors;
  }

  @VisibleForTesting
  /** 设置是否将主机名写入ID锁文件，仅用于测试 */
  public static void setWrite2IdFile(boolean write2IdFile) {
    NameNodeConnector.write2IdFile = write2IdFile;
  }

  @VisibleForTesting
  /** 设置是否检查其他实例正在运行，仅用于测试 */
  public static void checkOtherInstanceRunning(boolean toCheck) {
    NameNodeConnector.checkOtherInstanceRunning = toCheck;
  }

  private final URI nameNodeUri;
  private final String blockpoolID;

  private final BalancerProtocols namenode;
  /**
   * If set getBlocksToStandby true, Balancer will getBlocks from
   * Standby NameNode only and it can reduce the performance impact of Active
   * NameNode, especially in a busy HA mode cluster.
   */
  private boolean getBlocksToStandby;
  private String nsId;
  private Configuration config;
  private final KeyManager keyManager;
  final AtomicBoolean fallbackToSimpleAuth = new AtomicBoolean(false);

  private final DistributedFileSystem fs;
  private final Path idPath;
  private OutputStream out;
  private final List<Path> targetPaths;
  private final AtomicLong bytesMoved = new AtomicLong();
  private final AtomicLong blocksMoved = new AtomicLong();
  private final AtomicLong blocksFailed = new AtomicLong();

  private final int maxNotChangedIterations;
  private int notChangedIterations = 0;
  private final RateLimiter getBlocksRateLimiter;

  /**
   * 构造单个NameNode连接器，初始化连接和互斥检查
   * @param name 均衡器进程名称
   * @param nameNodeUri NameNode地址
   * @param idPath 互斥锁文件路径
   * @param targetPaths 需要均衡的目标路径列表
   * @param conf Hadoop配置
   * @param maxNotChangedIterations 最大无数据移动迭代次数，达到后退出均衡
   * @throws IOException 初始化或连接失败时抛出
   */
  public NameNodeConnector(String name, URI nameNodeUri, Path idPath,
                           List<Path> targetPaths, Configuration conf,
                           int maxNotChangedIterations)
      throws IOException {
    this.nameNodeUri = nameNodeUri;
    this.idPath = idPath;
    // 未指定目标路径默认均衡根目录下所有数据
    this.targetPaths = targetPaths == null || targetPaths.isEmpty() ? Arrays
        .asList(new Path("/")) : targetPaths;
    this.maxNotChangedIterations = maxNotChangedIterations;
    // 从配置读取getBlocks请求最大QPS限制
    int getBlocksMaxQps = conf.getInt(
        DFSConfigKeys.DFS_NAMENODE_GETBLOCKS_MAX_QPS_KEY,
        DFSConfigKeys.DFS_NAMENODE_GETBLOCKS_MAX_QPS_DEFAULT);
    if (getBlocksMaxQps > 0) {
      LOG.info("getBlocks calls for {} will be rate-limited to {} per second",
          nameNodeUri, getBlocksMaxQps);
      // 创建限流器
      this.getBlocksRateLimiter = RateLimiter.create(getBlocksMaxQps);
    } else {
      // 不限制QPS
      this.getBlocksRateLimiter = null;
    }

    // 创建NameNode代理连接
    this.namenode = NameNodeProxies.createProxy(conf, nameNodeUri,
        BalancerProtocols.class, fallbackToSimpleAuth).getProxy();
    // 读取配置是否允许从Standby获取块信息
    this.getBlocksToStandby = !conf.getBoolean(
        DFSConfigKeys.DFS_NAMENODE_GETBLOCKS_CHECK_OPERATION_KEY,
        DFSConfigKeys.DFS_NAMENODE_GETBLOCKS_CHECK_OPERATION_DEFAULT);
    this.config = conf;

    // 获取DistributedFileSystem实例
    this.fs = (DistributedFileSystem)FileSystem.get(nameNodeUri, conf);

    // 获取NameNode版本信息，提取块池ID
    final NamespaceInfo namespaceinfo = namenode.versionRequest();
    this.blockpoolID = namespaceinfo.getBlockPoolID();

    // 获取服务器默认配置，初始化密钥管理器
    final FsServerDefaults defaults = fs.getServerDefaults(new Path("/"));
    this.keyManager = new KeyManager(blockpoolID, namenode,
        defaults.getEncryptDataTransfer(), conf);
    // 检查是否已有其他均衡器实例运行，并创建锁文件标记当前实例运行
    if (checkOtherInstanceRunning) {
      out = checkAndMarkRunning();
      if (out == null) {
        // 已有实例运行，抛出异常终止当前进程
        throw new IOException("Another " + name + " is running.");
      }
    }
  }

  /**
   * 带命名空间ID的构造函数，用于HA场景
   * @param name 均衡器进程名称
   * @param nameNodeUri NameNode地址
   * @param nsId 命名空间ID
   * @param idPath 互斥锁文件路径
   * @param targetPaths 需要均衡的目标路径列表
   * @param conf Hadoop配置
   * @param maxNotChangedIterations 最大无数据移动迭代次数
   * @throws IOException 初始化失败时抛出
   */
  public NameNodeConnector(String name, URI nameNodeUri, String nsId,
                           Path idPath, List<Path> targetPaths,
                           Configuration conf, int maxNotChangedIterations)
      throws IOException {
    this(name, nameNodeUri, idPath, targetPaths, conf, maxNotChangedIterations);
    this.nsId = nsId;
  }

  /** 获取当前连接对应的DistributedFileSystem实例 */
  public DistributedFileSystem getDistributedFileSystem() {
    return fs;
  }

  /** @return the block pool ID */
  public String getBlockpoolID() {
    return blockpoolID;
  }

  /** 获取已移动字节数统计 */
  public AtomicLong getBytesMoved() {
    return bytesMoved;
  }

  /** 获取已移动块数统计 */
  public AtomicLong getBlocksMoved() {
    return blocksMoved;
  }

  /** 获取移动失败块数统计 */
  public AtomicLong getBlocksFailed() {
    return blocksFailed;
  }

  /** 累加已移动数据量和块数统计 */
  public void addBytesMoved(long numBytes) {
    bytesMoved.addAndGet(numBytes);
    blocksMoved.incrementAndGet();
  }

  /** 获取当前连接的NameNode地址 */
  public URI getNameNodeUri() {
    return nameNodeUri;
  }

  /**
   * 从NameNode获取指定DataNode上符合条件的块信息
   * @param datanode 目标DataNode
   * @param size 需要获取的总大小
   * @param minBlockSize 最小块大小过滤
   * @param timeInterval 时间间隔过滤
   * @param storageType 存储类型过滤
   * @return 带位置信息的块列表
   * @throws IOException 获取块信息失败时抛出
   */
  /** @return blocks with locations. */
  public BlocksWithLocations getBlocks(DatanodeInfo datanode, long size, long
      minBlockSize, long timeInterval, StorageType storageType) throws IOException {
    // 如果开启了QPS限制，获取令牌
    if (getBlocksRateLimiter != null) {
      getBlocksRateLimiter.acquire();
    }
    boolean isRequestStandby = false;
    NamenodeProtocol nnProxy = null;
    InetSocketAddress standbyAddress = null;
    try {
      // 获取合适的NameNode代理（优先Standby）
      ProxyPair proxyPair = getProxy();
      isRequestStandby = proxyPair.isRequestStandby;
      ClientProtocol proxy = proxyPair.clientProtocol;
      if (isRequestStandby) {
        // 连接Standby节点获取NamenodeProtocol代理
        standbyAddress = RPC.getServerAddress(proxy);
        nnProxy = NameNodeProxies.createNonHAProxy(
            config, standbyAddress, NamenodeProtocol.class,
            UserGroupInformation.getCurrentUser(), false).getProxy();
      } else {
        // 使用默认的Active节点代理
        nnProxy = namenode;
      }
      // 请求获取块信息
      return nnProxy.getBlocks(datanode, size, minBlockSize, timeInterval, storageType);
    } finally {
      if (isRequestStandby) {
        // 记录成功请求Standby节点的日志
        LOG.info("Request #getBlocks to Standby NameNode success. " +
            "remoteAddress: {}", standbyAddress.getHostString());
      }
    }
  }

  /**
   * 检查当前集群是否正在进行升级
   * @return true 正在升级，false 升级已完成
   * @throws IOException 检查失败时抛出
   */
  /**
   * @return true if an upgrade is in progress, false if not.
   * @throws IOException
   */
  public boolean isUpgrading() throws IOException {
    // 检查fsimage升级是否未完成
    final boolean isUpgrade = !namenode.isUpgradeFinalized();
    // 检查滚动升级是否未完成
    RollingUpgradeInfo info = fs.rollingUpgrade(
        HdfsConstants.RollingUpgradeAction.QUERY);
    final boolean isRollingUpgrade = (info != null && !info.isFinalized());
    return (isUpgrade || isRollingUpgrade);
  }

  /**
   * 获取所有在线DataNode的存储报告
   * @return 在线DataNode存储报告数组
   * @throws IOException 获取失败时抛出
   */
  /** @return live datanode storage reports. */
  public DatanodeStorageReport[] getLiveDatanodeStorageReport()
      throws IOException {
    boolean isRequestStandby = false;
    InetSocketAddress standbyAddress = null;
    try {
      // 获取合适的NameNode代理（优先Standby）
      ProxyPair proxyPair = getProxy();
      isRequestStandby = proxyPair.isRequestStandby;
      ClientProtocol proxy = proxyPair.clientProtocol;
      if (isRequestStandby) {
        standbyAddress = RPC.getServerAddress(proxy);
      }
      // 请求获取在线DataNode存储报告
      return proxy.getDatanodeStorageReport(DatanodeReportType.LIVE);
    } finally {
      if (isRequestStandby) {
        // 记录成功请求Standby节点的日志
        LOG.info("Request #getLiveDatanodeStorageReport to Standby " +
            "NameNode success. remoteAddress: {}", standbyAddress.getHostString());
      }
    }
  }

  /**
   * 获取合适的NameNode代理，HA开启时优先返回Standby节点代理降低Active负载
   * @return 包含代理和是否为Standby标识的ProxyPair对象
   * @throws IOException 获取代理失败时抛出
   */
  /**
   * get the proxy.
   * @return ProxyPair(clientProtocol and isRequestStandby)
   * @throws IOException
   */
  private ProxyPair getProxy() throws IOException {
    boolean isRequestStandby = false;
    ClientProtocol clientProtocol = null;
    // 如果配置允许且HA已启用，尝试从Standby获取数据
    if (getBlocksToStandby && nsId != null
        && HAUtil.isHAEnabled(config, nsId)) {
      // 获取当前命名空间所有NameNode的代理
      List<ClientProtocol> namenodes =