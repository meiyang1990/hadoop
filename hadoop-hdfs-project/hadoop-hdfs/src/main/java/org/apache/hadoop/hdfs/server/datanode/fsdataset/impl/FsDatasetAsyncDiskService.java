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

package org.apache.hadoop.hdfs.server.datanode.fsdataset.impl;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.datanode.DataNodeFaultInjector;
import org.apache.hadoop.util.Preconditions;
import org.apache.hadoop.util.concurrent.SubjectInheritingThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.DatanodeUtil;
import org.apache.hadoop.hdfs.server.datanode.ReplicaInfo;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeReference;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.ReplicaOutputStreams;
import org.apache.hadoop.hdfs.server.protocol.BlockCommand;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.nativeio.NativeIOException;

/**
 * 文件级注释：HDFS DataNode 异步磁盘操作服务，为每个存储卷维护独立线程池，支持异步磁盘任务调度
 * 
 * 该类为每个DataNode存储卷单独维护一个线程池，用于执行异步磁盘操作，
 * 例如块文件删除、sync_file_range 刷新请求等，避免阻塞核心心跳线程同时提高多卷并发效率。
 * 对比全局单线程池，该设计可以更好利用多磁盘IO并行能力，提升异步操作效率。
 */
class FsDatasetAsyncDiskService {
  public static final Logger LOG =
      LoggerFactory.getLogger(FsDatasetAsyncDiskService.class);
  
  // 线程池核心线程数，每个卷保持1个核心线程
  private static final int CORE_THREADS_PER_VOLUME = 1;
  // 每个卷允许的最大线程数
  private final int maxNumThreadsPerVolume;
  // 超过核心数的线程空闲存活时间，单位秒
  private static final long THREADS_KEEP_ALIVE_SECONDS = 60; 
  
  private final DataNode datanode;
  private final FsDatasetImpl fsdatasetImpl;
  // 存储卷ID -> 对应异步任务线程池的映射
  private Map<String, ThreadPoolExecutor> executors
      = new HashMap<String, ThreadPoolExecutor>();
  // 块池ID -> 已删除块ID集合的缓存，用于批量通知NameNode删除结果
  private Map<String, Set<Long>> deletedBlockIds 
      = new HashMap<String, Set<Long>>();
  // 批量通知NameNode删除结果的最大块数阈值
  private static final int MAX_DELETED_BLOCKS = 64;
  // 当前已缓存待批量通知的删除块总数
  private int numDeletedBlocks = 0;
  
  /**
   * 构造异步磁盘服务，基于DataNode配置初始化每个卷的线程池参数
   * @param datanode DataNode服务实例
   * @param fsdatasetImpl DataNode文件数据集实现实例
   */
  FsDatasetAsyncDiskService(DataNode datanode, FsDatasetImpl fsdatasetImpl) {
    this.datanode = datanode;
    this.fsdatasetImpl = fsdatasetImpl;
    // 从配置读取每个卷最大线程数，使用默认值兜底
    maxNumThreadsPerVolume = datanode.getConf().getInt(
      DFSConfigKeys.DFS_DATANODE_FSDATASETASYNCDISK_MAX_THREADS_PER_VOLUME_KEY,
          DFSConfigKeys.DFS_DATANODE_FSDATASETASYNCDISK_MAX_THREADS_PER_VOLUME_DEFAULT);
    Preconditions.checkArgument(maxNumThreadsPerVolume > 0,
        DFSConfigKeys.DFS_DATANODE_FSDATASETASYNCDISK_MAX_THREADS_PER_VOLUME_KEY +
          " must be a positive integer.");
  }

  /**
   * 为新增存储卷创建并注册异步任务线程池
   * @param volume 新增的存储卷实例
   */
  private void addExecutorForVolume(final FsVolumeImpl volume) {
    ThreadFactory threadFactory = new ThreadFactory() {
      int counter = 0;

      @Override
      public Thread newThread(Runnable r) {
        int thisIndex;
        synchronized (this) {
          thisIndex = counter++;
        }
        // 创建继承访问主体的线程，保持安全上下文
        Thread t = new SubjectInheritingThread(r);
        t.setName("Async disk worker #" + thisIndex +
            " for volume " + volume);
        return t;
      }
    };

    // 创建线程池，使用无界任务队列
    ThreadPoolExecutor executor = new ThreadPoolExecutor(
        CORE_THREADS_PER_VOLUME, maxNumThreadsPerVolume,
        THREADS_KEEP_ALIVE_SECONDS, TimeUnit.SECONDS,
        new LinkedBlockingQueue<Runnable>(), threadFactory);

    // 允许核心线程超时退出，减少空闲资源占用
    executor.allowCoreThreadTimeOut(true);
    executors.put(volume.getStorageID(), executor);
  }

  /**
   * 添加新存储卷到异步服务，为其创建对应线程池
   * @param volume 待添加的新存储卷实例
   */
  synchronized void addVolume(FsVolumeImpl volume) {
    if (executors == null) {
      throw new RuntimeException("AsyncDiskService is already shutdown");
    }
    if (volume == null) {
      throw new RuntimeException("Attempt to add a null volume");
    }
    ThreadPoolExecutor executor = executors.get(volume.getStorageID());
    if (executor != null) {
      throw new RuntimeException("Volume " + volume + " is already existed.");
    }
    addExecutorForVolume(volume);
  }

  /**
   * 从异步服务移除存储卷，关闭对应线程池
   * @param storageId 待移除存储卷的存储ID
   */
  synchronized void removeVolume(String storageId) {
    if (executors == null) {
      throw new RuntimeException("AsyncDiskService is already shutdown");
    }
    ThreadPoolExecutor executor = executors.get(storageId);
    if (executor == null) {
      throw new RuntimeException("Can not find volume with storageId "
          + storageId + " to remove.");
    } else {
      // 优雅关闭线程池，不再接受新任务
      executor.shutdown();
      executors.remove(storageId);
    }
  }

  /**
   * 统计所有卷当前待处理和正在处理的异步磁盘任务总数
   * @return 所有卷的未完成异步任务总数
   */
  synchronized long countPendingDeletions() {
    long count = 0;
    for (ThreadPoolExecutor exec : executors.values()) {
      // 已提交任务数减去已完成任务数 = 未完成任务数
      count += exec.getTaskCount() - exec.getCompletedTaskCount();
    }
    return count;
  }
  
  /**
   * 将异步任务提交到对应存储卷的线程池执行
   * @param volume 任务所属存储卷
   * @param task 待执行的异步任务
   */
  synchronized void execute(FsVolumeImpl volume, Runnable task) {
    try {
      if (executors == null) {
        throw new RuntimeException("AsyncDiskService is already shutdown");
      }
      if (volume == null) {
        throw new RuntimeException("A null volume does not have a executor");
      }
      ThreadPoolExecutor executor = executors.get(volume.getStorageID());
      if (executor == null) {
        throw new RuntimeException("Cannot find volume " + volume
            + " for execution of task " + task);
      } else {
        executor.execute(task);
      }
    } catch (RuntimeException re) {
      // 任务提交失败时释放卷引用，避免资源泄漏
      if (task instanceof ReplicaFileDeleteTask) {
        IOUtils.cleanupWithLogger(null,
            ((ReplicaFileDeleteTask) task).volumeRef);
      }
      throw re;
    }
  }
  
  /**
   * 优雅关闭所有存储卷的线程池，等待所有任务执行完成
   */
  synchronized void shutdown() {
    if (executors == null) {
      LOG.warn("AsyncDiskService has already shut down.");
    } else {
      LOG.info("Shutting down all async disk service threads");
      
      // 逐个关闭每个卷的线程池
      for (Map.Entry<String, ThreadPoolExecutor> e : executors.entrySet()) {
        e.getValue().shutdown();
      }
      // 清空映射表，让后续提交操作直接失败
      executors = null;
      
      LOG.info("All async disk service threads have been shut down");
    }
  }

  /**
   * 提交异步 sync_file_range 刷新请求，将指定文件范围刷新到磁盘
   * @param volume 请求所属存储卷
   * @param streams 待刷新的输出流集合
   * @param offset 刷新起始偏移量
   * @param nbytes 刷新字节长度
   * @param flags 同步标志位
   */
  public void submitSyncFileRangeRequest(FsVolumeImpl volume, final ReplicaOutputStreams streams,
      final long offset, final long nbytes, final int flags) {
    execute(volume, () -> {
      try {
        // 执行文件范围刷新
        streams.syncFileRangeIfPossible(offset, nbytes, flags);
      } catch (NativeIOException e) {
        try {
          LOG.warn("sync_file_range error. Volume: {}, Capacity: {}, Available space: {}, "
                  + "File range offset: {}, length: {}, flags: {}", volume, volume.getCapacity(),
              volume.getAvailable(), offset, nbytes, flags, e);
        } catch (IOException ioe) {
          LOG.warn("sync_file_range error. Volume: {}, Capacity: {}, "
                  + "File range offset: {}, length: {}, flags: {}", volume, volume.getCapacity(),
              offset, nbytes, flags, e);
        }
      }
    });
  }

  /**
   * 异步删除指定副本的块文件和元数据文件，完成后更新存储卷空间统计
   * @param volumeRef 待删除块所属存储卷引用
   * @param replicaToDelete 待删除副本信息
   * @param block 待删除扩展块信息
   * @param trashDirectory 回收站目录，不为null则移动到回收站而非直接删除
   */
  void deleteAsync(FsVolumeReference volumeRef, ReplicaInfo replicaToDelete,
      ExtendedBlock block, String trashDirectory) {
    LOG.info("Scheduling " + block.getLocalBlock()
        + " replica " + replicaToDelete + " on volume " +
        replicaToDelete.getVolume() + " for deletion");
    ReplicaFileDeleteTask deletionTask = new ReplicaFileDeleteTask(
        volumeRef, replicaToDelete, block, trashDirectory);
    execute(((FsVolumeImpl) volumeRef.getVolume()), deletionTask);
  }

  /**
   * 同步删除指定副本的块文件和元数据文件，完成后更新存储卷空间统计
   * @param volumeRef 待删除块所属存储卷引用
   * @param replicaToDelete 待删除副本信息
   * @param block 待删除扩展块信息
   * @param trashDirectory 回收站目录，不为null则移动到回收站而非直接删除
   */
  void deleteSync(FsVolumeReference volumeRef, ReplicaInfo replicaToDelete,
      ExtendedBlock block, String trashDirectory) {
    LOG.info("Deleting " + block.getLocalBlock() + " replica " + replicaToDelete);
    ReplicaFileDeleteTask deletionTask = new ReplicaFileDeleteTask(volumeRef,
        replicaToDelete, block, trashDirectory);
    deletionTask.run();
  }

  /**
   * 副本文件删除任务，负责删除/移动块文件和元数据，更新存储卷空间统计并通知NameNode
   * 若指定了回收站目录，则将文件移动到回收站；否则直接物理删除文件。
   */
  class ReplicaFileDeleteTask implements Runnable {
    private final FsVolumeReference volumeRef;
    private final FsVolumeImpl volume;
    private final ReplicaInfo replicaToDelete;
    private final ExtendedBlock block;
    private final String trashDirectory;

    ReplicaFileDeleteTask(FsVolumeReference volumeRef,
        ReplicaInfo replicaToDelete, ExtendedBlock block,
        String trashDirectory) {
      this.volumeRef = volumeRef;
      this.volume = (FsVolumeImpl) volumeRef.getVolume();
      this.replicaToDelete = replicaToDelete;
      this.block = block;
      this.trashDirectory = trashDirectory;
    }

    @Override
    public String toString() {
      // 异常展示时使用，描述当前删除任务的目标
      return "deletion of block " + block.getBlockPoolId() + " "
          + block.getLocalBlock() + " with block file "
          + replicaToDelete.getBlockURI() + " and meta file "
          + replicaToDelete.getMetadataURI() + " from volume " + volume;
    }

    /**
     * 直接物理删除块文件和元数据文件
     * @return 删除操作是否成功
     */
    private boolean deleteFiles() {
      return replicaToDelete.deleteBlockData() &&
        (replicaToDelete.deleteMetadata() || !replicaToDelete.metadataExists());
    }

    /**
     * 将块文件和元数据移动到回收站目录
     * @return 移动操作是否成功
     */
    private boolean moveFiles() {
      if (trashDirectory == null) {
        LOG.error("Trash dir for replica " + replicaToDelete + " is null");
        return false;
      }

      File trashDirFile = new File(trashDirectory);
      try {
        // 创建回收站目录，若不存在
        volume.getFileIoProvider().mkdirsWithExistsCheck(
            volume, trashDirFile);
      } catch (IOException e) {
        return false;
      }

      if (LOG.isDebugEnabled()) {
        LOG.debug("Moving files " + replicaToDelete.getBlockURI() + " and " +
            replicaToDelete.getMetadataURI() + " to trash.");
      }

      // 构造回收站中新文件路径
      final String blockName = replicaToDelete.getBlockName();
      final long genstamp = replicaToDelete.getGenerationStamp();
      File newBlockFile = new File(trashDirectory, blockName);
      File newMetaFile = new File(trashDirectory,
          DatanodeUtil.getMetaName(blockName, genstamp));
      try {
        // 重命名块文件和元数据到回收站
        return (replicaToDelete.renameData(newBlockFile.toURI()) &&
                replicaToDelete.renameMeta(newMetaFile.toURI()));
      } catch (IOException e) {
        LOG.error("Error moving files to trash: " + replicaToDelete, e);
      }
      return false;
    }

    @Override
    public void run() {
      try {
        // 故障注入：测试时模拟异步删除任务排队延迟
        DataNodeFaultInjector.get().delayDeleteReplica();
        // 先从内存中移除副本，若内存移除失败直接返回
        if (!fsdatasetImpl.removeReplicaFromMem(block, volume)) {
          return;
        }

        // 获取块和元数据长度，用于更新空间统计
        final long blockLength = replicaToDelete.getBlockDataLength();
        final long metaLength = replicaToDelete.getMetadataLength();
        boolean result;

        // 根据是否配置回收站选择删除还是移动
        result = (trashDirectory == null) ? deleteFiles() : moveFiles();

        if (!result) {
          LOG.warn("Unexpected error trying to "
              + (trashDirectory == null ? "delete" : "move")
              + " block " + block.getBlockPoolId() + " " + block.getLocalBlock()
              + " at file " + replicaToDelete.getBlockURI() + ". Ignored.");
        } else {
          // 需要通知NameNode块删除完成（NO_ACK表示不需要通知）
          if (block.getLocalBlock().getNumBytes() != BlockCommand.NO_ACK) {
            datanode.notifyNamenodeDeletedBlock(block, volume.getStorageID());
          }
          // 更新存储卷已用空间统计，扣除删除的块和元数据大小
          volume.onBlockFileDeletion(block.getBlockPoolId(), blockLength);
          volume.onMetaFileDeletion(block.getBlockPoolId(), metaLength);
          LOG.info("Deleted " + block.getBlockPoolId() + " " +
              block.getLocalBlock() + " URI " + replicaToDelete.getBlockURI());
        }
        // 更新已删除