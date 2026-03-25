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
package org.apache.hadoop.hdfs.server.mover;

import org.apache.commons.cli.*;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.*;
import org.apache.hadoop.hdfs.server.balancer.Dispatcher;
import org.apache.hadoop.hdfs.server.balancer.Dispatcher.*;
import org.apache.hadoop.hdfs.server.balancer.Dispatcher.DDatanode.StorageGroup;
import org.apache.hadoop.hdfs.server.balancer.ExitStatus;
import org.apache.hadoop.hdfs.server.balancer.Matcher;
import org.apache.hadoop.hdfs.server.balancer.NameNodeConnector;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockStoragePolicySuite;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.namenode.ErasureCodingPolicyManager;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorageReport;
import org.apache.hadoop.hdfs.server.protocol.StorageReport;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.source.JvmMetrics;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Lists;
import org.apache.hadoop.util.Preconditions;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.collect.Maps;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.text.DateFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * HDFS数据迁移工具核心类，根据文件存储策略将块副本迁移到对应存储类型，
 * 实现冷热数据分层存储，优化存储资源利用率。
 * 支持指定路径批量迁移，支持联邦HDFS多NameNode场景。
 */
@InterfaceAudience.Private
public class Mover {
  static final Logger LOG = LoggerFactory.getLogger(Mover.class);

  /**
   * 存储分组映射容器，管理源存储和目标存储分组信息，按存储类型分类索引。
   */
  private static class StorageMap {
    private final StorageGroupMap<Source> sources
        = new StorageGroupMap<Source>();
    private final StorageGroupMap<StorageGroup> targets
        = new StorageGroupMap<StorageGroup>();
    private final EnumMap<StorageType, List<StorageGroup>> targetStorageTypeMap
        = new EnumMap<StorageType, List<StorageGroup>>(StorageType.class);
    
    private StorageMap() {
      // 为所有可迁移存储类型初始化空列表
      for(StorageType t : StorageType.getMovableTypes()) {
        targetStorageTypeMap.put(t, new LinkedList<StorageGroup>());
      }
    }
    
    private void add(Source source, StorageGroup target) {
      sources.put(source);
      if (target != null) {
        targets.put(target);
        // 将目标存储加入对应存储类型索引
        getTargetStorages(target.getStorageType()).add(target);
      }
    }
    
    private Source getSource(MLocation ml) {
      return get(sources, ml);
    }

    private StorageGroup getTarget(String uuid, StorageType storageType) {
      return targets.get(uuid, storageType);
    }

    private static <G extends StorageGroup> G get(StorageGroupMap<G> map, MLocation ml) {
      return map.get(ml.datanode.getDatanodeUuid(), ml.storageType);
    }
    
    private List<StorageGroup> getTargetStorages(StorageType t) {
      return targetStorageTypeMap.get(t);
    }
  }

  private final Dispatcher dispatcher;
  private final StorageMap storages;
  private final List<Path> targetPaths;
  private final int retryMaxAttempts;
  private final AtomicInteger retryCount;
  private final Map<Long, Set<DatanodeInfo>> excludedPinnedBlocks;
  private final MoverMetrics metrics;
  private final NameNodeConnector nnc;

  private final BlockStoragePolicy[] blockStoragePolicies;

  /**
   * 构造Mover实例，初始化调度器、存储映射和配置参数。
   * @param nnc NameNode连接器，用于和NameNode交互
   * @param conf Hadoop配置对象
   * @param retryCount 当前重试次数计数器
   * @param excludedPinnedBlocks 固定无法迁移的块集合
   */
  Mover(NameNodeConnector nnc, Configuration conf, AtomicInteger retryCount,
      Map<Long, Set<DatanodeInfo>> excludedPinnedBlocks) {
    final long movedWinWidth = conf.getLong(
        DFSConfigKeys.DFS_MOVER_MOVEDWINWIDTH_KEY,
        DFSConfigKeys.DFS_MOVER_MOVEDWINWIDTH_DEFAULT);
    final int moverThreads = conf.getInt(
        DFSConfigKeys.DFS_MOVER_MOVERTHREADS_KEY,
        DFSConfigKeys.DFS_MOVER_MOVERTHREADS_DEFAULT);
    final int maxConcurrentMovesPerNode = conf.getInt(
        DFSConfigKeys.DFS_DATANODE_BALANCE_MAX_NUM_CONCURRENT_MOVES_KEY,
        DFSConfigKeys.DFS_DATANODE_BALANCE_MAX_NUM_CONCURRENT_MOVES_DEFAULT);
    final int maxNoMoveInterval = conf.getInt(
        DFSConfigKeys.DFS_MOVER_MAX_NO_MOVE_INTERVAL_KEY,
        DFSConfigKeys.DFS_MOVER_MAX_NO_MOVE_INTERVAL_DEFAULT);
    final int maxAttempts = conf.getInt(
        DFSConfigKeys.DFS_MOVER_RETRY_MAX_ATTEMPTS_KEY,
        DFSConfigKeys.DFS_MOVER_RETRY_MAX_ATTEMPTS_DEFAULT);
    // 检查最大重试次数配置合法性
    if (maxAttempts >= 0) {
      this.retryMaxAttempts = maxAttempts;
    } else {
      LOG.warn(DFSConfigKeys.DFS_MOVER_RETRY_MAX_ATTEMPTS_KEY + " is "
          + "configured with a negative value, using default value of "
          + DFSConfigKeys.DFS_MOVER_RETRY_MAX_ATTEMPTS_DEFAULT);
      this.retryMaxAttempts = DFSConfigKeys.DFS_MOVER_RETRY_MAX_ATTEMPTS_DEFAULT;
    }
    this.retryCount = retryCount;
    // 初始化块移动调度器，复用Balancer的Dispatcher实现
    this.dispatcher = new Dispatcher(nnc, Collections.<String> emptySet(),
        Collections.<String> emptySet(), movedWinWidth, moverThreads, 0,
        maxConcurrentMovesPerNode, maxNoMoveInterval, conf);
    this.storages = new StorageMap();
    this.targetPaths = nnc.getTargetPaths();
    // 按ID索引存储策略，ID长度固定不超过ID_BIT_LENGTH
    this.blockStoragePolicies = new BlockStoragePolicy[1 <<
        BlockStoragePolicySuite.ID_BIT_LENGTH];
    this.excludedPinnedBlocks = excludedPinnedBlocks;
    this.nnc = nnc;
    this.metrics = MoverMetrics.create(this);
  }

  /**
   * 初始化Mover，加载存储策略并收集DataNode存储信息。
   * @throws IOException 初始化过程IO异常
   */
  void init() throws IOException {
    initStoragePolicies();
    final List<DatanodeStorageReport> reports = dispatcher.init();
    // 遍历所有DataNode存储报告，初始化源和目标存储
    for(DatanodeStorageReport r : reports) {
      final DDatanode dn = dispatcher.newDatanode(r.getDatanodeInfo());
      for(StorageType t : StorageType.getMovableTypes()) {
        final Source source = dn.addSource(t, Long.MAX_VALUE, dispatcher);
        final long maxRemaining = getMaxRemaining(r, t);
        // 剩余空间大于0则作为目标存储
        final StorageGroup target = maxRemaining > 0L ? dn.addTarget(t,
            maxRemaining) : null;
        storages.add(source, target);
      }
    }
  }

  /**
   * 从NameNode拉取所有存储策略，缓存到本地数组。
   * @throws IOException 拉取过程IO异常
   */
  private void initStoragePolicies() throws IOException {
    Collection<BlockStoragePolicy> policies =
        dispatcher.getDistributedFileSystem().getAllStoragePolicies();
    for (BlockStoragePolicy policy : policies) {
      this.blockStoragePolicies[policy.getId()] = policy;
    }
  }

  /**
   * 启动迁移流程，执行块迁移任务。
   * @return 迁移退出状态
   */
  private ExitStatus run() {
    try {
      init();
      return new Processor().processNamespace().getExitStatus();
    } catch (IllegalArgumentException e) {
      System.out.println(e + ".  Exiting ...");
      return ExitStatus.ILLEGAL_ARGUMENTS;
    } catch (IOException e) {
      System.out.println(e + ".  Exiting ...");
      LOG.error(e + ".  Exiting ...");
      return ExitStatus.IO_EXCEPTION;
    } finally {
      // 无论成功失败都关闭调度器释放资源
      dispatcher.shutdownNow();
    }
  }

  public NameNodeConnector getNnc() {
    return nnc;
  }

  /**
   * 根据LocatedBlock构造内部块对象，支持副本块和纠删码块。
   * @param lb 位置块信息，来自NameNode
   * @param locations 块位置列表
   * @param ecPolicy 纠删码策略，非纠删码块为null
   * @return 构造完成的内部块对象
   */
  DBlock newDBlock(LocatedBlock lb, List<MLocation> locations,
                   ErasureCodingPolicy ecPolicy) {
    Block blk = lb.getBlock().getLocalBlock();
    DBlock db;
    if (lb.isStriped()) {
      // 构造纠删码条带化块
      LocatedStripedBlock lsb = (LocatedStripedBlock) lb;
      byte[] indices = new byte[lsb.getBlockIndices().length];
      for (int i = 0; i < indices.length; i++) {
        indices[i] = (byte) lsb.getBlockIndices()[i];
      }
      db = new DBlockStriped(blk, indices, (short) ecPolicy.getNumDataUnits(),
          ecPolicy.getCellSize());
    } else {
      // 构造普通副本块
      db = new DBlock(blk);
    }

    List<Integer> adjustList = new ArrayList<>();
    // 遍历所有位置，只保留当前需要迁移存储对应的源位置
    for (int i = 0; i < locations.size(); i++) {
      MLocation ml = locations.get(i);
      StorageGroup source = storages.getSource(ml);
      if (source != null) {
        db.addLocation(source);
      } else if (lb.isStriped()) {
        // 纠删码块中部分DataNode不可用，需要后续调整索引
        adjustList.add(i);
      }
    }

    if (!adjustList.isEmpty()) {
      // 调整纠删码块索引，匹配当前可用位置
      ((DBlockStriped) db).adjustIndices(adjustList);
      Preconditions.checkArgument(((DBlockStriped) db).getIndices().length
          == db.getLocations().size());
    }
    return db;
  }

  /**
   * 获取指定DataNode上指定存储类型的最大剩余空间。
   * @param report DataNode存储报告
   * @param t 目标存储类型
   * @return 最大剩余空间值
   */
  private static long getMaxRemaining(DatanodeStorageReport report, StorageType t) {
    long max = 0L;
    for(StorageReport r : report.getStorageReports()) {
      if (r.getStorage().getStorageType() == t) {
        if (r.getRemaining() > max) {
          max = r.getRemaining();
        }
      }
    }
    return max;
  }

  /**
   * 将快照路径转换为非快照路径，例如 /foo/.snapshot/s1/bar --> /foo/bar。
   * @param pathComponents 路径分段数组
   * @return 转换后的非快照路径
   */
  private static String convertSnapshotPath(String[] pathComponents) {
    StringBuilder sb = new StringBuilder(Path.SEPARATOR);
    for (int i = 0; i < pathComponents.length; i++) {
      if (pathComponents[i].equals(HdfsConstants.DOT_SNAPSHOT_DIR)) {
        // 跳过.snapshot段和快照名称段
        i++;
      } else {
        sb.append(pathComponents[i]);
      }
    }
    return sb.toString();
  }

  /**
   * 命名空间处理器，递归遍历指定路径下所有文件，处理块迁移。
   */
  class Processor {
    private final DFSClient dfs;
    private final List<String> snapshottableDirs = new ArrayList<String>();

    Processor() {
      dfs = dispatcher.getDistributedFileSystem().getClient();
    }

    /**
     * 从NameNode获取所有支持快照的目录，后续需要遍历快照目录。
     */
    private void getSnapshottableDirs() {
      SnapshottableDirectoryStatus[] dirs = null;
      try {
        dirs = dfs.getSnapshottableDirListing();
      } catch (IOException e) {
        LOG.warn("Failed to get snapshottable directories."
            + " Ignore and continue.", e);
      }
      if (dirs != null) {
        for (SnapshottableDirectoryStatus dir : dirs) {
          snapshottableDirs.add(dir.getFullPath().toString());
        }
      }
    }

    /**
     * 检查快照路径对应的原文件是否仍然存在于当前目录树。
     * @param path 输入快照路径
     * @return true如果原文件存在，不需要重复处理
     * @throws IOException 查询文件信息IO异常
     */
    private boolean isSnapshotPathInCurrent(String path) throws IOException {
      // if the parent path contains "/.snapshot/", this is a snapshot path
      if (path.contains(HdfsConstants.SEPARATOR_DOT_SNAPSHOT_DIR_SEPARATOR)) {
        String[] pathComponents = INode.getPathNames(path);
        if (HdfsConstants.DOT_SNAPSHOT_DIR
            .equals(pathComponents[pathComponents.length - 2])) {
          // this is a path for a specific snapshot (e.g., /foo/.snapshot/s1)
          return false;
        }
        String nonSnapshotPath = convertSnapshotPath(pathComponents);
        return dfs.getFileInfo(nonSnapshotPath) != null;
      } else {
        return false;
      }
    }

    /**
     * 遍历所有目标路径，处理文件块迁移，检查是否还有未完成任务。
     * @return 处理结果对象，包含是否需要下一轮处理
     * @throws IOException 遍历过程IO异常
     */
    private Result processNamespace() throws IOException {
      metrics.setProcessingNamespace(true);
      getSnapshottableDirs();
      Result result = new Result();
      // 遍历所有待迁移路径
      for (Path target : targetPaths) {
        processPath(target.toUri().getPath(), result);
      }
      // 等待所有已调度块移动完成，检查是否有失败
      boolean hasFailed = Dispatcher.waitForMoveCompletion(storages.targets
          .values());
      Dispatcher.checkForBlockPinningFailures(excludedPinnedBlocks,
          storages.targets.values());
      boolean hasSuccess = Dispatcher.checkForSuccess(storages.targets
          .values());
      if (hasFailed && !hasSuccess) {
        // 全部失败，检查是否达到最大重试次数
        if (retryCount.get() == retryMaxAttempts) {
          result.setRetryFailed();
          LOG.error("Failed to move some block's after "
              + retryMaxAttempts + " retries.");