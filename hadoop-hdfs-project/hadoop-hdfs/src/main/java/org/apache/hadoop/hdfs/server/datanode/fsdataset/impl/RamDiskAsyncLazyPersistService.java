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

import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.concurrent.SubjectInheritingThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.ReplicaInfo;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeReference;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * 文件：RamDisk异步延迟持久化服务
 * 核心职责：为每个非RamDisk存储卷维护一个单线程线程池，管理RamDisk上块数据异步落盘到磁盘的任务调度，
 *          支持存储卷动态上线/下线，统一处理RamDisk懒持久化异步任务。
 * 说明：本类与{@link org.apache.hadoop.util.AsyncDiskService}功能类似，未来可合并。
 */
class RamDiskAsyncLazyPersistService {
  public static final Logger LOG =
      LoggerFactory.getLogger(RamDiskAsyncLazyPersistService.class);

  // 每个卷对应的线程池核心线程数
  private static final int CORE_THREADS_PER_VOLUME = 1;
  // 每个卷对应的线程池最大线程数
  private static final int MAXIMUM_THREADS_PER_VOLUME = 1;
  // 超过核心数的线程空闲保活时间（单位：秒）
  private static final long THREADS_KEEP_ALIVE_SECONDS = 60;

  private final DataNode datanode;
  private final Configuration conf;

  private final ThreadGroup threadGroup;
  private Map<String, ThreadPoolExecutor> executors
      = new HashMap<String, ThreadPoolExecutor>();
  private final static HdfsConfiguration EMPTY_HDFS_CONF = new HdfsConfiguration();

  /**
   * 构造RamDisk异步延迟持久化服务实例
   * @param datanode 当前Datanode实例
   * @param conf Hadoop配置对象
   */
  RamDiskAsyncLazyPersistService(DataNode datanode, Configuration conf) {
    this.datanode = datanode;
    this.conf = conf;
    this.threadGroup = new ThreadGroup(getClass().getSimpleName());
  }

  /**
   * 为指定存储ID的卷创建对应的持久化线程池
   * @param storageId 存储卷ID
   */
  private void addExecutorForVolume(final String storageId) {
    ThreadFactory threadFactory = new ThreadFactory() {

      @Override
      public Thread newThread(Runnable r) {
        Thread t = new SubjectInheritingThread(threadGroup, r);
        t.setName("Async RamDisk lazy persist worker " +
            " for volume with id " + storageId);
        return t;
      }
    };

    ThreadPoolExecutor executor = new ThreadPoolExecutor(
        CORE_THREADS_PER_VOLUME, MAXIMUM_THREADS_PER_VOLUME,
        THREADS_KEEP_ALIVE_SECONDS, TimeUnit.SECONDS,
        new LinkedBlockingQueue<Runnable>(), threadFactory);

    // 允许核心线程超时退出，减少空闲资源占用
    executor.allowCoreThreadTimeOut(true);
    executors.put(storageId, executor);
  }

  /**
   * 添加新存储卷，为其创建对应的持久化线程池
   * @param volume 新增的数据存储卷
   */
  synchronized void addVolume(FsVolumeImpl volume) {
    String storageId = volume.getStorageID();
    if (executors == null) {
      throw new RuntimeException("AsyncLazyPersistService is already shutdown");
    }
    ThreadPoolExecutor executor = executors.get(storageId);
    if (executor != null) {
      throw new RuntimeException("Volume " + volume + " is already existed.");
    }
    addExecutorForVolume(storageId);
  }

  /**
   * 移除存储卷，关闭其对应的持久化线程池
   * @param volume 要移除的数据存储卷
   */
  synchronized void removeVolume(FsVolumeImpl volume) {
    String storageId = volume.getStorageID();
    if (executors == null) {
      throw new RuntimeException("AsyncDiskService is already shutdown");
    }
    ThreadPoolExecutor executor = executors.get(storageId);
    if (executor == null) {
      throw new RuntimeException("Can not find volume with storage id " +
          storageId + " to remove.");
    } else {
      executor.shutdown();
      executors.remove(storageId);
    }
  }

  /**
   * 查询指定存储卷是否已注册对应的线程池
   * @param volume 待查询的存储卷
   * @return true 存在对应线程池；false 不存在
   */
  synchronized boolean queryVolume(FsVolumeImpl volume) {
    String storageId = volume.getStorageID();
    if (executors == null) {
      throw new RuntimeException(
          "AsyncLazyPersistService is already shutdown");
    }
    ThreadPoolExecutor executor = executors.get(storageId);
    return (executor != null);
  }

  /**
   * 提交任务到对应存储卷的线程池异步执行
   * @param storageId 目标存储卷ID
   * @param task 待执行的持久化任务
   */
  synchronized void execute(String storageId, Runnable task) {
    try {
      if (executors == null) {
        throw new RuntimeException(
            "AsyncLazyPersistService is already shutdown");
      }
      ThreadPoolExecutor executor = executors.get(storageId);
      if (executor == null) {
        throw new RuntimeException("Cannot find root storage volume with id " +
            storageId + " for execution of task " + task);
      } else {
        executor.execute(task);
      }
    } catch (RuntimeException re) {
      // 任务提交失败时清理卷引用，避免资源泄漏
      if (task instanceof ReplicaLazyPersistTask) {
        IOUtils.cleanupWithLogger(null,
            ((ReplicaLazyPersistTask) task).targetVolume);
      }
      throw re;
    }
  }

  /**
   * 优雅关闭所有存储卷的线程池，等待所有持久化任务完成后退出
   */
  synchronized void shutdown() {
    if (executors == null) {
      LOG.warn("AsyncLazyPersistService has already shut down.");
    } else {
      LOG.info("Shutting down all async lazy persist service threads");

      // 关闭所有线程池
      for (Map.Entry<String, ThreadPoolExecutor> e : executors.entrySet()) {
        e.getValue().shutdown();
      }
      // 清空执行器映射，防止后续提交任务
      executors = null;
      LOG.info("All async lazy persist service threads have been shut down");
    }
  }

  /**
   * 提交RamDisk块数据异步落盘任务
   * @param bpId 块池ID
   * @param blockId 块ID
   * @param genStamp 块生成时间戳
   * @param creationTime 块创建时间
   * @param replica RamDisk上的副本信息
   * @param target 目标存储卷引用
   * @throws IOException 任务提交异常
   */
  void submitLazyPersistTask(String bpId, long blockId,
      long genStamp, long creationTime,
      ReplicaInfo replica, FsVolumeReference target) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("LazyWriter schedule async task to persist RamDisk block pool id: "
          + bpId + " block id: " + blockId);
    }

    ReplicaLazyPersistTask lazyPersistTask = new ReplicaLazyPersistTask(
        bpId, blockId, genStamp, creationTime, replica, target);

    FsVolumeImpl volume = (FsVolumeImpl)target.getVolume();
    execute(volume.getStorageID(), lazyPersistTask);
  }

  /**
   * RamDisk副本异步持久化任务，负责将RamDisk上的块数据拷贝到目标磁盘存储卷
   */
  class ReplicaLazyPersistTask implements Runnable {
    private final String bpId;
    private final long blockId;
    private final long genStamp;
    private final long creationTime;
    private final ReplicaInfo replicaInfo;
    private final FsVolumeReference targetVolume;

    /**
     * 构造RamDisk副本持久化任务
     * @param bpId 块池ID
     * @param blockId 块ID
     * @param genStamp 块生成时间戳
     * @param creationTime 块创建时间
     * @param replicaInfo RamDisk上的副本信息
     * @param targetVolume 目标存储卷引用
     */
    ReplicaLazyPersistTask(String bpId, long blockId,
        long genStamp, long creationTime,
        ReplicaInfo replicaInfo,
        FsVolumeReference targetVolume) {
      this.bpId = bpId;
      this.blockId = blockId;
      this.genStamp = genStamp;
      this.creationTime = creationTime;
      this.replicaInfo = replicaInfo;
      this.targetVolume = targetVolume;
    }

    @Override
    public String toString() {
      // 用于异常信息展示，打印任务基本信息
      return "LazyWriter async task of persist RamDisk block pool id:"
          + bpId + " block pool id: "
          + blockId + " with block file " + replicaInfo.getBlockURI()
          + " and meta file " + replicaInfo.getMetadataURI()
          + " to target volume " + targetVolume;
    }

    @Override
    public void run() {
      boolean succeeded = false;
      final FsDatasetImpl dataset = (FsDatasetImpl)datanode.getFSDataset();
      // 自动关闭存储卷引用，保证资源释放
      try (FsVolumeReference ref = this.targetVolume) {
        // 获取IO缓冲区大小配置
        int smallBufferSize = DFSUtilClient.getSmallBufferSize(EMPTY_HDFS_CONF);

        FsVolumeImpl volume = (FsVolumeImpl)ref.getVolume();
        // 将块数据从RamDisk拷贝到目标卷的持久化位置
        File[] targetFiles = volume.copyBlockToLazyPersistLocation(bpId,
            blockId, genStamp, replicaInfo, smallBufferSize, conf);

        // 持久化完成后通知数据集更新元数据，替换RamDisk副本为磁盘副本
        dataset.onCompleteLazyPersist(bpId, blockId,
                creationTime, targetFiles, volume);
        succeeded = true;
      } catch (Exception e){
        FsDatasetImpl.LOG.warn(
            "LazyWriter failed to async persist RamDisk block pool id: "
            + bpId + "block Id: " + blockId, e);
      } finally {
        // 持久化失败，通知数据集清理RamDisk上的失效副本
        if (!succeeded) {
          dataset.onFailLazyPersist(bpId, blockId);
        }
      }
    }
  }
}