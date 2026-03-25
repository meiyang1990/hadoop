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
package org.apache.hadoop.hdfs.server.datanode.erasurecode;

import org.apache.hadoop.util.Preconditions;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.protocol.BlockECReconstructionCommand.BlockECReconstructionInfo;
import org.apache.hadoop.hdfs.util.StripedBlockUtil.BlockReadStats;
import org.apache.hadoop.util.Daemon;
import org.slf4j.Logger;

import java.util.Collection;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 文件：hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/datanode/erasurecode/ErasureCodingWorker.java
 * 本文件属于HDFS数据节点的纠删码模块，负责处理来自NameNode的纠删码块重建任务
 */

/**
 * ErasureCodingWorker 处理纠删码重建工作任务。任务由NameNode在Datanode心跳响应中下发，
 * BPOfferService将EC任务委托给此类处理，负责任务解析、线程池调度和任务提交。
 */
@InterfaceAudience.Private
public final class ErasureCodingWorker {
  private static final Logger LOG = DataNode.LOG;

  private final DataNode datanode;
  private final Configuration conf;
  private final float xmitWeight;

  private ThreadPoolExecutor stripedReconstructionPool;
  private ThreadPoolExecutor stripedReadPool;

  /**
   * 构造ErasureCodingWorker实例，初始化配置和线程池
   * @param conf Hadoop配置对象
   * @param datanode 所属DataNode实例
   */
  public ErasureCodingWorker(Configuration conf, DataNode datanode) {
    this.datanode = datanode;
    this.conf = conf;
    this.xmitWeight = conf.getFloat(
        DFSConfigKeys.DFS_DN_EC_RECONSTRUCTION_XMITS_WEIGHT_KEY,
        DFSConfigKeys.DFS_DN_EC_RECONSTRUCTION_XMITS_WEIGHT_DEFAULT
    );
    // 检查传输权重配置合法性，不允许为负数
    Preconditions.checkArgument(this.xmitWeight >= 0,
        "Invalid value configured for " +
            DFSConfigKeys.DFS_DN_EC_RECONSTRUCTION_XMITS_WEIGHT_KEY +
            ", it can not be negative value (" + this.xmitWeight + ").");

    initializeStripedReadThreadPool();
    initializeStripedBlkReconstructionThreadPool(conf.getInt(
        DFSConfigKeys.DFS_DN_EC_RECONSTRUCTION_THREADS_KEY,
        DFSConfigKeys.DFS_DN_EC_RECONSTRUCTION_THREADS_DEFAULT));
  }

  /**
   * 初始化条带块读取线程池，用于并行读取纠删码块的多个源数据块
   * 实现类似CachedThreadPool，按需创建线程，空闲60秒回收线程
   */
  private void initializeStripedReadThreadPool() {
    LOG.debug("Using striped reads");

    // 本质是缓存线程池，无核心线程，按需创建，最大可创建无限线程
    stripedReadPool = new ThreadPoolExecutor(0, Integer.MAX_VALUE,
        60, TimeUnit.SECONDS,
        new SynchronousQueue<>(),
        // 自定义线程工厂，命名线程方便监控排查
        new Daemon.DaemonFactory() {
          private final AtomicInteger threadIndex = new AtomicInteger(0);

          @Override
          public Thread newThread(Runnable r) {
            Thread t = super.newThread(r);
            t.setName("stripedRead-" + threadIndex.getAndIncrement());
            return t;
          }
        },
        // 拒绝策略：提交被拒绝后在当前线程直接执行，避免任务丢失
        new ThreadPoolExecutor.CallerRunsPolicy() {
          @Override
          public void rejectedExecution(Runnable runnable,
                                        ThreadPoolExecutor e) {
            LOG.info("Execution for striped reading rejected, "
                + "Executing in current thread");
            // will run in the current thread
            super.rejectedExecution(runnable, e);
          }
        });

    stripedReadPool.allowCoreThreadTimeOut(true);
  }

  /**
   * 初始化条带块重建线程池，负责执行完整的纠删码重建任务
   * @param numThreads 线程池最大线程数，从配置读取
   */
  private void initializeStripedBlkReconstructionThreadPool(int numThreads) {
    LOG.debug("Using striped block reconstruction; pool threads={}",
        numThreads);
    stripedReconstructionPool = DFSUtilClient.getThreadPoolExecutor(numThreads,
        numThreads, 60, new LinkedBlockingQueue<>(),
        "StripedBlockReconstruction-", false);
    stripedReconstructionPool.allowCoreThreadTimeOut(true);
  }

  /**
   * 批量处理NameNode下发的纠删码重建任务，解析任务并提交到重建线程池
   * @param ecTasks NameNode下发的纠删码重建任务集合
   *
   */
  public void processErasureCodingTasks(
      Collection<BlockECReconstructionInfo> ecTasks) {
    for (BlockECReconstructionInfo reconInfo : ecTasks) {
      try {
        // 将NameNode下发的通用重建信息转换为条带重建所需信息格式
        StripedReconstructionInfo stripedReconInfo =
            new StripedReconstructionInfo(
            reconInfo.getExtendedBlock(), reconInfo.getErasureCodingPolicy(),
            reconInfo.getLiveBlockIndices(), reconInfo.getSourceDnInfos(),
            reconInfo.getTargetDnInfos(), reconInfo.getTargetStorageTypes(),
            reconInfo.getTargetStorageIDs(), reconInfo.getExcludeReconstructedIndices());
        // It may throw IllegalArgumentException from task#stripedReader
        // constructor.
        // 创建条带块重建任务实例
        final StripedBlockReconstructor task =
            new StripedBlockReconstructor(this, stripedReconInfo);
        // 任务存在有效重建目标节点才提交
        if (task.hasValidTargets()) {
          stripedReconstructionPool.submit(task);
          // See HDFS-12044. We increase xmitsInProgress even the task is only
          // enqueued, so that
          //   1) NN will not send more tasks than what DN can execute and
          //   2) DN will not throw away reconstruction tasks, and instead keeps
          //      an unbounded number of tasks in the executor's task queue.
          // 根据权重计算任务占用的传输配额，至少占用1个配额
          int xmitsSubmitted = Math.max((int)(task.getXmits() * xmitWeight), 1);
          // 增加DataNode正在处理的传输计数，限制NameNode任务下发量
          getDatanode().incrementXmitsInProcess(xmitsSubmitted);
        } else {
          LOG.warn("No missing internal block. Skip reconstruction for task:{}",
              reconInfo);
        }
      } catch (Throwable e) {
        LOG.warn("Failed to reconstruct striped block {}",
            reconInfo.getExtendedBlock().getLocalBlock(), e);
      }
    }
  }

  /**
   * 获取当前worker所属的DataNode实例
   * @return 所属DataNode实例
   */
  DataNode getDatanode() {
    return datanode;
  }

  /**
   * 获取Hadoop配置对象
   * @return 配置对象
   */
  Configuration getConf() {
    return conf;
  }

  /**
   * 创建基于条带读线程池的CompletionService，用于并行读取多个块并收集结果
   * @return 新建的CompletionService实例
   */
  CompletionService<BlockReadStats> createReadService() {
    return new ExecutorCompletionService<>(stripedReadPool);
  }

  /**
   * 关闭工作线程池，停止处理新任务，用于DataNode退出
   */
  public void shutDown() {
    stripedReconstructionPool.shutdown();
    stripedReadPool.shutdown();
  }

  /**
   * 获取重建任务的传输权重，用于计算任务占用的传输配额
   * @return 传输权重值
   */
  public float getXmitWeight() {
    return xmitWeight;
  }
}