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

import static org.apache.hadoop.util.Time.monotonicNow;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BatchedRemoteIterator.BatchedListEntries;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.OpenFileEntry;
import org.apache.hadoop.hdfs.protocol.OpenFilesIterator;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.util.RwLockMode;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.Lists;
import org.apache.hadoop.util.Time;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 文件写入租约管理器，负责HDFS文件写入的租约生命周期管理和过期租约恢复。
 * 核心功能包括：租约创建、续约、删除、定期检查过期租约并触发租约恢复，保证文件写入异常后的完整性。
 * <p>
 * 租约恢复算法：
 * 1) Namenode 获取租约信息
 * 2) 对于租约中的每个文件 f，处理文件的最后一个块 b
 * 2.1) 获取存储块 b 的所有数据节点
 * 2.2) 选择一个数据节点作为主节点 p
 *
 * 2.3) p 从 namenode 获取新的世代戳
 * 2.4) p 从每个数据节点获取块信息
 * 2.5) p 计算最小块长度
 * 2.6) p 更新所有有效世代戳的数据节点，设置新世代戳和最小块长度
 * 2.7) p 向 namenode 确认更新结果
 *
 * 2.8) Namenode 更新 BlockInfo 元数据
 * 2.9) Namenode 从租约中移除文件 f，当租约中所有文件都移除后删除租约
 * 2.10) Namenode 将变更提交到 edit log
 */
@InterfaceAudience.Private
public class LeaseManager {
  public static final Logger LOG = LoggerFactory.getLogger(LeaseManager.class
      .getName());
  // 关联的FSNamesystem对象，NameNode核心文件系统管理
  private final FSNamesystem fsnamesystem;
  // 租约软限制时间，超过软限制后可被其他客户端申请恢复
  private long softLimit = HdfsConstants.LEASE_SOFTLIMIT_PERIOD;
  // 租约硬限制时间，超过硬限制后强制触发租约恢复
  private long hardLimit;
  // INode过滤最大工作线程数
  static final int INODE_FILTER_WORKER_COUNT_MAX = 4;
  // 每个工作线程最小处理任务数阈值
  static final int INODE_FILTER_WORKER_TASK_MIN = 512;
  // 内部租约持有者上次更新时间
  private long lastHolderUpdateTime;
  // 当前内部租约持有者名称
  private String internalLeaseHolder;

  // 映射：租约持有者 -> 租约对象
  private final HashMap<String, Lease> leases = new HashMap<>();
  // 映射：INode ID -> 租约对象
  private final TreeMap<Long, Lease> leasesById = new TreeMap<>();

  // 租约监控后台线程
  private Daemon lmthread;
  // 监控线程运行标志，volatile保证多线程可见性
  private volatile boolean shouldRunMonitor;

  /**
   * 构造租约管理器，关联FSNamesystem，加载配置参数
   * @param fsnamesystem 关联的FSNamesystem
   */
  LeaseManager(FSNamesystem fsnamesystem) {
    Configuration conf = new Configuration();
    this.fsnamesystem = fsnamesystem;
    this.hardLimit = conf.getLong(DFSConfigKeys.DFS_LEASE_HARDLIMIT_KEY,
        DFSConfigKeys.DFS_LEASE_HARDLIMIT_DEFAULT) * 1000;
    updateInternalLeaseHolder();
  }

  /**
   * 使用当前时间戳更新内部租约持有者
   */
  private void updateInternalLeaseHolder() {
    this.lastHolderUpdateTime = Time.monotonicNow();
    this.internalLeaseHolder = HdfsServerConstants.NAMENODE_LEASE_HOLDER +
        "-" + Time.formatTime(Time.now());
  }

  /**
   * 获取当前内部租约持有者名称，过期则自动更新
   * @return 当前有效的内部租约持有者名称
   */
  String getInternalLeaseHolder() {
    long elapsed = Time.monotonicNow() - lastHolderUpdateTime;
    if (elapsed > hardLimit) {
      updateInternalLeaseHolder();
    }
    return internalLeaseHolder;
  }

  /**
   * 根据租约持有者名称获取租约对象
   * @param holder 租约持有者名称
   * @return 对应租约对象，不存在则返回null
   */
  Lease getLease(String holder) {
    return leases.get(holder);
  }

  /**
   * 遍历所有租约，统计处于未完成状态的块数量。调用前必须持有FSNamesystem读锁
   * @return 未完成块的总数量
   */
  synchronized long getNumUnderConstructionBlocks() {
    assert this.fsnamesystem.hasReadLock(RwLockMode.GLOBAL) :
        "The FSNamesystem read lock wasn't acquired before counting under construction blocks";
    long numUCBlocks = 0;
    for (Long id : getINodeIdWithLeases()) {
      INode inode = fsnamesystem.getFSDirectory().getInode(id);
      if (inode == null) {
        // INode可能在获取ID列表后被删除，直接忽略
        LOG.warn("Failed to find inode {} in getNumUnderConstructionBlocks().",
            id);
        continue;
      }
      final INodeFile cons = inode.asFile();
      if (!cons.isUnderConstruction()) {
        LOG.warn("The file {} is not under construction but has lease.",
            cons.getFullPathName());
        continue;
      }
      BlockInfo[] blocks = cons.getBlocks();
      if(blocks == null) {
        continue;
      }
      for(BlockInfo b : blocks) {
        if(!b.isComplete()) {
          numUCBlocks++;
        }
      }
    }
    LOG.info("Number of blocks under construction: {}", numUCBlocks);
    return numUCBlocks;
  }

  Collection<Long> getINodeIdWithLeases() {return leasesById.keySet();}

  /**
   * 获取所有持有有效租约的INode对应的INodesInPath集合
   * @return 所有有效租约文件的INodesInPath集合
   * @throws IOException 获取过程中IO异常
   */
  @VisibleForTesting
  Set<INodesInPath> getINodeWithLeases() throws IOException {
    return getINodeWithLeases(null);
  }

  /**
   * 获取所有当前持有租约的INode数组，过滤已删除文件
   * @return 有效租约对应的INode数组
   */
  private synchronized INode[] getINodesWithLease() {
    List<INode> inodes = new ArrayList<>(leasesById.size());
    INode currentINode;
    for (long inodeId : leasesById.keySet()) {
      currentINode = fsnamesystem.getFSDirectory().getInode(inodeId);
      // 持有租约的文件可能已经被删除，或者父目录被递归删除
      if (currentINode != null &&
          currentINode.isFile() &&
          !fsnamesystem.isFileDeleted(currentINode.asFile())) {
        inodes.add(currentINode);
      }
    }
    return inodes.toArray(new INode[0]);
  }

  /**
   * 获取指定祖先目录下所有持有有效租约的文件INodesInPath，若祖先目录为null则返回所有有效租约文件。
   * 调用者必须持有FSNamesystem读锁或写锁。
   * @param ancestorDir 祖先目录，为null表示获取所有
   * @return 符合条件的有效租约文件INodesInPath集合
   * @throws IOException 获取过程中IO异常
   */
  public Set<INodesInPath> getINodeWithLeases(final INodeDirectory
      ancestorDir) throws IOException {
    assert fsnamesystem.hasReadLock(RwLockMode.FS);
    // 记录开始时间，用于性能统计
    final long startTimeMs = Time.monotonicNow();
    Set<INodesInPath> iipSet = new HashSet<>();
    final INode[] inodes = getINodesWithLease();
    int inodeCount = inodes.length;
    if (inodeCount == 0) {
      return iipSet;
    }

    List<Future<List<INodesInPath>>> futureList = Lists.newArrayList();
    // 根据总数量计算需要的工作线程数，不超过最大限制
    final int workerCount = Math.min(INODE_FILTER_WORKER_COUNT_MAX,
        (((inodeCount - 1) / INODE_FILTER_WORKER_TASK_MIN) + 1));
    // 创建固定线程池
    ExecutorService inodeFilterService =
        Executors.newFixedThreadPool(workerCount);
    for (int workerIdx = 0; workerIdx < workerCount; workerIdx++) {
      final int startIdx = workerIdx;
      Callable<List<INodesInPath>> c = new Callable<List<INodesInPath>>() {
        @Override
        public List<INodesInPath> call() {
          List<INodesInPath> iNodesInPaths = Lists.newArrayList();
          // 按workerCount步长遍历，每个线程处理间隔workerCount个元素
          for (int idx = startIdx; idx < inodeCount; idx += workerCount) {
            INode inode = inodes[idx];
            if (!inode.isFile()) {
              continue;
            }
            // 构造文件完整路径
            INodesInPath inodesInPath = INodesInPath.fromINode(
                fsnamesystem.getFSDirectory().getRoot(), inode.asFile());
            // 过滤掉不在指定祖先目录下的文件
            if (ancestorDir != null &&
                !inodesInPath.isDescendant(ancestorDir)) {
              continue;
            }
            iNodesInPaths.add(inodesInPath);
          }
          return iNodesInPaths;
        }
      };

      // 提交过滤任务到线程池
      futureList.add(inodeFilterService.submit(c));
    }
    // 关闭线程池，不再接受新任务
    inodeFilterService.shutdown();

    // 收集所有任务结果
    for (Future<List<INodesInPath>> f : futureList) {
      try {
        iipSet.addAll(f.get());
      } catch (Exception e) {
        throw new IOException("Failed to get files with active leases", e);
      }
    }
    final long endTimeMs = Time.monotonicNow();
    // 耗时超过1秒则打印日志
    if ((endTimeMs - startTimeMs) > 1000) {
      LOG.info("Took {} ms to collect {} open files with leases {}",
          (endTimeMs - startTimeMs), iipSet.size(), ((ancestorDir != null) ?
              " under " + ancestorDir.getFullPathName() : "."));
    }
    return iipSet;
  }

  /**
   * 获取一批处于构建中的未完成文件，使用默认路径过滤
   * @param prevId INodeID游标，返回比该ID大的结果
   * @return 分批结果包含当前批条目和是否还有更多结果
   * @throws IOException 获取过程中IO异常
   */
  public BatchedListEntries<OpenFileEntry> getUnderConstructionFiles(
      final long prevId) throws IOException {
    return getUnderConstructionFiles(prevId,
        OpenFilesIterator.FILTER_PATH_DEFAULT);
  }

  /**
   * 从当前活跃租约中获取一批处于构建中的未完成文件。
   * 使用INodeID作为游标获取下一批结果，批大小可配置。
   * 分批获取不保证所有打开文件的一致性视图。
   * @param prevId INodeID游标，返回比该ID大的结果
   * @param path 路径过滤前缀，只返回该路径下的文件
   * @return 分批结果包含当前批条目和是否还有更多结果
   * @throws IOException 获取过程中IO异常
   * @see org.apache.hadoop.hdfs.DFSConfigKeys#DFS_NAMENODE_LIST_OPENFILES_NUM_RESPONSES
   */
  public BatchedListEntries<OpenFileEntry> getUnderConstructionFiles(
      final long prevId, final String path) throws IOException {
    assert fsnamesystem.hasReadLock(RwLockMode.FS);
    SortedMap<Long, Lease> remainingLeases;
    synchronized (this) {
      // 获取比prevId大的剩余租约
      remainingLeases = leasesById.tailMap(prevId, false);
    }
    Collection<Long> inodeIds = remainingLeases.keySet();
    // 计算本次返回数量，不超过配置的最大批大小
    final int numResponses = Math.min(
        this.fsnamesystem.getMaxListOpenFilesResponses(), inodeIds.size());
    final List<OpenFileEntry> openFileEntries =
        Lists.newArrayListWithExpectedSize(numResponses);

    int count = 0;
    String fullPathName = null;
    Iterator<Long> inodeIdIterator = inodeIds.iterator();
    while (inodeIdIterator.hasNext()) {
      Long inodeId = inodeIdIterator.next();
      INode ucFile = fsnamesystem.getFSDirectory().getInode(inodeId);
      if (ucFile == null) {
        // INode可能已经被删除，直接跳过
        continue;
      }

      final INodeFile inodeFile = ucFile.asFile();
      if (!inodeFile.isUnderConstruction()) {
        LOG.warn("The file {} is not under construction but has lease.",
            inodeFile.getFullPathName());
        continue;
      }

      fullPathName = inodeFile.getFullPathName();
      // 路径匹配才加入结果
      if (StringUtils.isEmpty(path) ||
          DFSUtil.isParentEntry(fullPathName, path)) {
        openFileEntries.add(new OpenFileEntry(inodeFile.getId(), fullPathName,
            inodeFile.getFileUnderConstructionFeature().getClientName(),
            inodeFile.getFileUnderConstructionFeature().getClientMachine()));
        count++;
      }

      // 达到批大小则停止
      if (count >= numResponses) {
        break;
      }
    }
    // 是否还有更多结果，避免下次全量扫描
    boolean hasMore = inodeIdIterator.hasNext();
    return new BatchedListEntries<>(openFileEntries, hasMore);
  }

  /**
   * 根据文件INode获取对应租约
   * @param src 文件INode
   * @return 对应租约对象，不存在则返回null
   */
  /** @return the lease containing src */
  public synchronized Lease getLease(INodeFile src) {return leasesById.get(src.getId());}

  /**
   * 获取当前系统中租约总数
   * @return 租约总数
   */
  /** @return the number of leases currently in the system */
  @VisibleForTesting
  public synchronized int countLease() {
    return leases.size();
  }

  /**
   * 获取所有租约包含的文件路径总数
   * @return 总文件路径数
   */
  /** @return the number of paths contained in all leases */
  synchronized long countPath() {
    return leasesById.size();
  }

  /**
   * 添加或续约指定文件的租约
   * @param holder 租约持有者名称
   * @param inodeId 文件INode ID
   * @return 新增或更新后的租约对象
   */
  synchronized Lease addLease(String holder, long inodeId) {
    Lease lease = getLease(holder);
    if (lease == null) {
      lease = new Lease(holder);