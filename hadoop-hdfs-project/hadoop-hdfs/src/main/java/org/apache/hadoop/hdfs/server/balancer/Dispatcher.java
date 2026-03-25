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

import static org.apache.hadoop.hdfs.util.StripedBlockUtil.getInternalBlockLength;
import static org.apache.hadoop.hdfs.protocolPB.PBHelperClient.vintPrefixed;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.datatransfer.BlockPinningException;
import org.apache.hadoop.hdfs.protocol.datatransfer.DataTransferProtoUtil;
import org.apache.hadoop.hdfs.protocol.datatransfer.IOStreamPair;
import org.apache.hadoop.hdfs.protocol.datatransfer.Sender;
import org.apache.hadoop.hdfs.protocol.datatransfer.TrustedChannelResolver;
import org.apache.hadoop.hdfs.protocol.datatransfer.sasl.DataTransferSaslUtil;
import org.apache.hadoop.hdfs.protocol.datatransfer.sasl.SaslDataTransferClient;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.BlockOpResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.Status;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.server.balancer.Dispatcher.DDatanode.StorageGroup;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockPlacementPolicies;
import org.apache.hadoop.hdfs.protocol.BlockType;
import org.apache.hadoop.hdfs.server.protocol.BlocksWithLocations;
import org.apache.hadoop.hdfs.server.protocol.BlocksWithLocations.BlockWithLocations;
import org.apache.hadoop.hdfs.server.protocol.BlocksWithLocations.StripedBlockWithLocations;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorageReport;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Time;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.util.Preconditions;

/**
 * 文件说明: HDFS数据平衡器核心调度模块，负责在数据节点之间调度和执行块副本迁移任务
 * 核心职责: 维护源/目标存储组信息，选择合适的块进行迁移，管理并发迁移任务，执行网络数据传输
 */
/** Dispatching block replica moves between datanodes. */
@InterfaceAudience.Private
public class Dispatcher {
  static final Logger LOG = LoggerFactory.getLogger(Dispatcher.class);

  /**
   * 数据节点发生迁移错误后，延迟重试的时间
   */
  private static long delayAfterErrors = 10 * 1000;

  /** NameNode连接器，用于和NameNode交互获取块信息和上报迁移进度 */
  private final NameNodeConnector nnc;
  /** SASL认证客户端，用于数据传输安全认证 */
  private final SaslDataTransferClient saslClient;

  /** Set of datanodes to be excluded. */
  private final Set<String> excludedNodes;
  /** Restrict to the following nodes. */
  private final Set<String> includedNodes;

  /** 所有待迁移块的源存储组集合 */
  private final Collection<Source> sources = new HashSet<Source>();
  /** 所有接收迁移块的目标存储组集合 */
  private final Collection<StorageGroup> targets = new HashSet<StorageGroup>();

  /** 全局块信息缓存，维护所有待迁移块的位置信息 */
  private final GlobalBlockMap globalBlocks = new GlobalBlockMap();
  /** 已迁移块记录，用于去重和清理过期记录 */
  private final MovedBlocks<StorageGroup> movedBlocks;

  /** Map (datanodeUuid,storageType -> StorageGroup) */
  private final StorageGroupMap<StorageGroup> storageGroupMap
      = new StorageGroupMap<StorageGroup>();

  /** 集群网络拓扑，用于块放置策略校验 */
  private NetworkTopology cluster;

  /** 块调度线程池，每个源存储使用一个线程调度块 */
  private final ExecutorService dispatchExecutor;

  /** 迁移线程分配器，负责为目标节点分配迁移线程 */
  private final Allocator moverThreadAllocator;

  /** The maximum number of concurrent blocks moves at a datanode */
  private final int maxConcurrentMovesPerNode;
  /** 最大迁移线程总数 */
  private final int maxMoverThreads;

  /** 每次从NameNode获取块的最大大小 */
  private final long getBlocksSize;
  /** 参与平衡的最小块大小，小于该大小的块跳过不迁移 */
  private final long getBlocksMinBlockSize;
  /** 单个块迁移超时时间 */
  private final long blockMoveTimeout;
  /** 热块排除时间区间，最近被修改过时间小于该值的块不迁移 */
  private final long hotBlockTimeInterval;
  /**
   * If no block can be moved out of a {@link Source} after this configured
   * amount of time, the Source should give up choosing the next possible move.
   */
  private final int maxNoMoveInterval;

  /** IO缓冲区大小 */
  private final int ioFileBufferSize;

  /** 是否通过主机名连接数据节点 */
  private final boolean connectToDnViaHostname;
  /** 块放置策略实例，用于校验迁移后是否符合放置策略 */
  private BlockPlacementPolicies placementPolicies;

  /** 单次迭代最大运行时间，超时后终止当前迭代 */
  private long maxIterationTime;

  /**
   * 线程分配器，负责按配额分配迁移线程资源
   */
  static class Allocator {
    private final int max;
    private int count = 0;
    private int lotSize = 1;

    Allocator(int max) {
      this.max = max;
    }

    /** Allocate specified number of items */
    synchronized int allocate(int n) {
      final int remaining = max - count;
      if (remaining <= 0) {
        return 0;
      } else {
        final int allocated = remaining < n? remaining: n;
        count += allocated;
        return allocated;
      }
    }

    /** Allocate a single lot of items. */
    int allocate() {
      return allocate(lotSize);
    }

    synchronized void reset() {
      count = 0;
    }

    /** Set the lot size */
    synchronized void setLotSize(int lotSize) {
      this.lotSize = lotSize;
    }
  }

  /**
   * 全局块信息存储容器，维护所有块元信息，保证全局唯一一份块对象
   */
  private static class GlobalBlockMap {
    private final Map<Block, DBlock> map = new HashMap<Block, DBlock>();

    /**
     * 如果块不存在则放入全局映射，否则返回已有对象
     * @return 放入的新块或已存在的块对象
     */
    private DBlock putIfAbsent(Block blk, DBlock dblk) {
      if (!map.containsKey(blk)) {
        map.put(blk, dblk);
        return dblk;
      }
      return map.get(blk);
    }

    /** Remove all blocks except for the moved blocks. */
    private void removeAllButRetain(MovedBlocks<StorageGroup> movedBlocks) {
      for (Iterator<Block> i = map.keySet().iterator(); i.hasNext();) {
        if (!movedBlocks.contains(i.next())) {
          i.remove();
        }
      }
    }
  }

  /**
   * 存储组索引映射，按数据节点UUID和存储类型查找存储组
   * @param <G> 存储组类型
   */
  public static class StorageGroupMap<G extends StorageGroup> {
    private static String toKey(String datanodeUuid, StorageType storageType) {
      return datanodeUuid + ":" + storageType;
    }

    private final Map<String, G> map = new HashMap<String, G>();

    public G get(String datanodeUuid, StorageType storageType) {
      return map.get(toKey(datanodeUuid, storageType));
    }

    public void put(G g) {
      final String key = toKey(g.getDatanodeInfo().getDatanodeUuid(), g.storageType);
      final StorageGroup existing = map.put(key, g);
      Preconditions.checkState(existing == null);
    }

    int size() {
      return map.size();
    }

    void clear() {
      map.clear();
    }

    public Collection<G> values() {
      return map.values();
    }
  }

  /**
   * 待迁移块任务，维护块迁移的源、目标和代理信息，负责执行块迁移网络请求
   */
  /** This class keeps track of a scheduled reportedBlock move */
  public class PendingMove {
    private DBlock reportedBlock;
    private Source source;
    private DDatanode proxySource;
    private StorageGroup target;

    @VisibleForTesting
    PendingMove(Source source, StorageGroup target) {
      this.source = source;
      this.target = target;
    }

    public DatanodeInfo getSource() {
      return source.getDatanodeInfo();
    }

    @Override
    public String toString() {
      final Block b = reportedBlock != null ? reportedBlock.getBlock() : null;
      String bStr = b != null ? (b + " with size=" + b.getNumBytes() + " ")
          : " ";
      return bStr + "from " + source.getDisplayName() + " to " + target
          .getDisplayName() + " through " + (proxySource != null ? proxySource
          .datanode : "");
    }

    /**
     * 从源选择合适块，并为该块选择代理源节点
     * @return 成功选择到块和代理返回true，否则false
     */
    private boolean chooseBlockAndProxy() {
      // source and target must have the same storage type
      final StorageType t = source.getStorageType();
      // iterate all source's blocks until find a good one
      for (Iterator<DBlock> i = source.getBlockIterator(); i.hasNext();) {
        if (markMovedIfGoodBlock(i.next(), t)) {
          i.remove();
          return true;
        }
      }
      return false;
    }

    /**
     * 检查块是否符合迁移条件，符合则标记为已迁移
     * @return 符合迁移条件返回true
     */
    @VisibleForTesting
    boolean markMovedIfGoodBlock(DBlock block, StorageType targetStorageType) {
      synchronized (block) {
        synchronized (movedBlocks) {
          if (isGoodBlockCandidate(source, target, targetStorageType, block)) {
            if (block instanceof DBlockStriped) {
              reportedBlock = ((DBlockStriped) block).getInternalBlock(source);
              if (reportedBlock == null) {
                LOG.info(
                    "No striped internal block on source {}, block {}. Skipping.",
                    source, block);
                return false;
              }
            } else {
              reportedBlock = block;
            }
            if (chooseProxySource()) {
              movedBlocks.put(block);
              if (LOG.isDebugEnabled()) {
                LOG.debug("Decided to move " + this);
              }
              return true;
            }
          }
        }
      }
      return false;
    }

    /**
     * 为当前块选择合适的代理源节点
     * @return 找到代理节点返回true
     */
    private boolean chooseProxySource() {
      final DatanodeInfo targetDN = target.getDatanodeInfo();
      // if source and target are same nodes then no need of proxy
      if (source.getDatanodeInfo().equals(targetDN) && addTo(source.getDDatanode())) {
        return true;
      }
      // if node group is supported, first try add nodes in the same node group
      if (cluster.isNodeGroupAware()) {
        for (StorageGroup loc : reportedBlock.getLocations()) {
          if (cluster.isOnSameNodeGroup(loc.getDatanodeInfo(), targetDN)
              && addTo(loc.getDDatanode())) {
            return true;
          }
        }
      }
      // check if there is replica which is on the same rack with the target
      for (StorageGroup loc : reportedBlock.getLocations()) {
        if (cluster.isOnSameRack(loc.getDatanodeInfo(), targetDN) && addTo(loc.getDDatanode())) {
          return true;
        }
      }
      // find out a non-busy replica
      for (StorageGroup loc : reportedBlock.getLocations()) {
        if (addTo(loc.getDDatanode())) {
          return true;
        }
      }
      return false;
    }

    /** add to a proxy source for specific reportedBlock movement */
    private boolean addTo(StorageGroup g) {
      final DDatanode dn = g.getDDatanode();
      if (dn.addPendingBlock(this)) {
        proxySource = dn;
        return true;
      }
      return false;
    }

    /** Dispatch the move to the proxy source & wait for the response. */
    private void dispatch() {
      Socket sock = new Socket();
      DataOutputStream out = null;
      DataInputStream in = null;
      try {
        if (source.isIterationOver()){
          LOG.info("Cancel moving " + this +
              " as iteration is already cancelled due to" +
              " dfs.balancer.max-iteration-time is passed.");
          throw new IOException("Block move cancelled.");
        }
        LOG.info("Start moving " + this);
        assert !(reportedBlock instanceof DBlockStriped);

        // 连接目标数据节点
        sock.connect(
            NetUtils.createSocketAddr(target.getDatanodeInfo().
                getXferAddr(Dispatcher.this.connectToDnViaHostname)),
                HdfsConstants.READ_TIMEOUT);

        // 设置SO超时时间，避免永久挂起
        sock.setSoTimeout(HdfsConstants.READ_TIMEOUT * 5);
        sock.setKeepAlive(true);

        OutputStream unbufOut = sock.getOutputStream();
        InputStream unbufIn = sock.getInputStream();
        ExtendedBlock eb = new ExtendedBlock(nnc.getBlockpoolID(),
            reportedBlock.getBlock());
        final KeyManager km = nnc.getKeyManager(); 
        // 获取块访问令牌
        Token<BlockTokenIdentifier> accessToken = km.getAccessToken(eb,
            new StorageType[]{target.storageType}, new String[0]);
        // 完成SASL认证握手，获取加密流
        IOStreamPair saslStreams = saslClient.socketSend(sock, unbufOut,
            unbufIn, km, accessToken, target.getDatanodeInfo());
        unbufOut = saslStreams.out;
        unbufIn = saslStreams.in;
        // 包装缓冲流提升IO性能
        out = new DataOutputStream(new BufferedOutputStream(unbufOut,
            ioFileBufferSize));
        in = new DataInputStream(new BufferedInputStream(unbufIn,
            ioFileBufferSize));

        // 发送块替换请求
        sendRequest(out, eb, accessToken);
        // 接收并处理响应
        receiveResponse(in);
        // 更新总迁移字节数
        nnc.addBytesMoved(reportedBlock.getNumBytes());
        target.getDDatanode().setHasSuccess();
        LOG.info("Successfully moved " + this);
      } catch (IOException e) {
        LOG.warn("Failed to move " + this, e);
        nnc.getBlocksFailed().incrementAndGet();
        target.getDDatanode().setHasFailure();
        // 处理块固定错误，记录失败块后续排除