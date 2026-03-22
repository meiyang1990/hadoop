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

package org.apache.hadoop.hdfs.server.sps;

import java.io.IOException;
import java.net.Socket;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.datatransfer.Sender;
import org.apache.hadoop.hdfs.protocol.datatransfer.TrustedChannelResolver;
import org.apache.hadoop.hdfs.protocol.datatransfer.sasl.DataTransferSaslUtil;
import org.apache.hadoop.hdfs.protocol.datatransfer.sasl.SaslDataTransferClient;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.server.balancer.KeyManager;
import org.apache.hadoop.hdfs.server.balancer.NameNodeConnector;
import org.apache.hadoop.hdfs.server.common.sps.BlockDispatcher;
import org.apache.hadoop.hdfs.server.common.sps.BlockMovementAttemptFinished;
import org.apache.hadoop.hdfs.server.common.sps.BlockMovementStatus;
import org.apache.hadoop.hdfs.server.common.sps.BlockStorageMovementTracker;
import org.apache.hadoop.hdfs.server.common.sps.BlocksMovementsStatusHandler;
import org.apache.hadoop.hdfs.server.namenode.sps.BlockMoveTaskHandler;
import org.apache.hadoop.hdfs.server.namenode.sps.SPSService;
import org.apache.hadoop.hdfs.server.protocol.BlockStorageMovementCommand.BlockMovingInfo;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.Daemon;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 文件级注释：存储策略满足器（SPS）的外部块移动任务处理器，在NameNode外部执行块移动操作
 * 该类负责处理块移动任务，通过直接建立到目标DataNode的Socket连接，
 * 调用{@link Sender#replaceBlock(ExtendedBlock, StorageType, Token, String, DatanodeInfo, String)}
 * 方法完成块迁移，满足存储策略对块位置的要求。
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class ExternalSPSBlockMoveTaskHandler implements BlockMoveTaskHandler {
  private static final Logger LOG = LoggerFactory
      .getLogger(ExternalSPSBlockMoveTaskHandler.class);

  // 块移动任务执行线程池
  private final ExecutorService moveExecutor;
  // 异步任务结果完成服务，用于获取已完成的块移动结果
  private final CompletionService<BlockMovementAttemptFinished> mCompletionServ;
  // NameNode连接器，用于获取NameNode连接和密钥信息
  private final NameNodeConnector nnc;
  // SASL数据传输客户端，用于安全认证
  private final SaslDataTransferClient saslClient;
  // 块移动状态跟踪器，跟踪正在进行的块移动任务
  private final BlockStorageMovementTracker blkMovementTracker;
  // 块移动跟踪后台线程
  private Daemon movementTrackerThread;
  // 存储策略满足器服务引用，用于通知任务完成结果
  private final SPSService service;
  // 块分发执行器，实际执行块移动逻辑
  private final BlockDispatcher blkDispatcher;
  // 块移动最大重试次数
  private final int maxRetry;

  /**
   * 构造外部SPS块移动任务处理器，初始化线程池、安全组件和后台跟踪线程
   * @param conf Hadoop配置对象
   * @param nnc NameNode连接器
   * @param spsService 存储策略满足器服务
   */
  public ExternalSPSBlockMoveTaskHandler(Configuration conf,
      NameNodeConnector nnc, SPSService spsService) {
    int moverThreads = conf.getInt(DFSConfigKeys.DFS_MOVER_MOVERTHREADS_KEY,
        DFSConfigKeys.DFS_MOVER_MOVERTHREADS_DEFAULT);
    maxRetry = conf.getInt(
        DFSConfigKeys.DFS_STORAGE_POLICY_SATISFIER_MOVE_TASK_MAX_RETRY_ATTEMPTS_KEY,
        DFSConfigKeys.DFS_STORAGE_POLICY_SATISFIER_MOVE_TASK_MAX_RETRY_ATTEMPTS_DEFAULT);
    moveExecutor = initializeBlockMoverThreadPool(moverThreads);
    mCompletionServ = new ExecutorCompletionService<>(moveExecutor);
    this.nnc = nnc;
    this.saslClient = new SaslDataTransferClient(conf,
        DataTransferSaslUtil.getSaslPropertiesResolver(conf),
        TrustedChannelResolver.getInstance(conf),
        nnc.getFallbackToSimpleAuth());
    this.blkMovementTracker = new BlockStorageMovementTracker(
        mCompletionServ, new ExternalBlocksMovementsStatusHandler());
    this.service = spsService;

    boolean connectToDnViaHostname = conf.getBoolean(
        HdfsClientConfigKeys.DFS_CLIENT_USE_DN_HOSTNAME,
        HdfsClientConfigKeys.DFS_CLIENT_USE_DN_HOSTNAME_DEFAULT);
    int ioFileBufferSize = DFSUtilClient.getIoFileBufferSize(conf);
    blkDispatcher = new BlockDispatcher(HdfsConstants.READ_TIMEOUT,
        ioFileBufferSize, connectToDnViaHostname);

    startMovementTracker();
  }

  /**
   * 初始化并启动块移动跟踪后台线程
   */
  private void startMovementTracker() {
    movementTrackerThread = new Daemon(this.blkMovementTracker);
    movementTrackerThread.setName("BlockStorageMovementTracker");
    movementTrackerThread.start();
  }

  /**
   * 初始化块移动任务线程池，配置线程工厂和拒绝策略
   * @param num 线程池最大线程数
   * @return 初始化完成的线程池实例
   */
  private ThreadPoolExecutor initializeBlockMoverThreadPool(int num) {
    LOG.debug("Block mover to satisfy storage policy; pool threads={}", num);

    ThreadPoolExecutor moverThreadPool = new ThreadPoolExecutor(1, num, 60,
        TimeUnit.SECONDS, new SynchronousQueue<Runnable>(),
        new Daemon.DaemonFactory() {
          private final AtomicInteger threadIndex = new AtomicInteger(0);

          @Override
          public Thread newThread(Runnable r) {
            Thread t = super.newThread(r);
            t.setName("BlockMoverTask-" + threadIndex.getAndIncrement());
            return t;
          }
        }, new ThreadPoolExecutor.CallerRunsPolicy() {
          @Override
          public void rejectedExecution(Runnable runnable,
              ThreadPoolExecutor e) {
            LOG.info("Execution for block movement to satisfy storage policy"
                + " got rejected, Executing in current thread");
            // 由调用线程直接执行任务
            super.rejectedExecution(runnable, e);
          }
        });

    moverThreadPool.allowCoreThreadTimeOut(true);
    return moverThreadPool;
  }

  @Override
  /**
   * 提交块移动任务到线程池异步执行
   * @param blkMovingInfo 待移动块的信息，包含源、目标、块信息和存储类型
   * @throws IOException 提交失败时抛出IO异常
   */
  public void submitMoveTask(BlockMovingInfo blkMovingInfo) throws IOException {
    // TODO: 需要在目标节点增加已调度块计数，该计数用于目标节点剩余空间计算
    // 内部移动时，剩余空间在DatanodeDescriptor中维护，请参考
    // IntraSPSNameNodeBlockMoveTaskHandler#submitMoveTask实现，通过
    // dn.incrementBlocksScheduled(blkMovingInfo.getTargetStorageType())更新计数
    LOG.debug("Received BlockMovingTask {}", blkMovingInfo);
    BlockMovingTask blockMovingTask = new BlockMovingTask(blkMovingInfo);
    mCompletionServ.submit(blockMovingTask);
  }

  /**
   * 外部块移动状态处理实现，处理块移动完成事件，通知SPS服务更新状态
   */
  private class ExternalBlocksMovementsStatusHandler
      implements BlocksMovementsStatusHandler {
    @Override
    /**
     * 处理块移动完成事件，通知SPSService任务完成结果
     * @param attemptedMove 块移动尝试完成结果
     */
    public void handle(BlockMovementAttemptFinished attemptedMove) {
      service.notifyStorageMovementAttemptFinishedBlk(
          attemptedMove.getTargetDatanode(), attemptedMove.getTargetType(),
          attemptedMove.getBlock());
    }
  }

  /**
   * 单个块移动任务封装，负责执行具体的块移动逻辑
   */
  private class BlockMovingTask
      implements Callable<BlockMovementAttemptFinished> {
    // 待移动块的信息
    private final BlockMovingInfo blkMovingInfo;

    /**
     * 构造块移动任务
     * @param blkMovingInfo 待移动块信息
     */
    BlockMovingTask(BlockMovingInfo blkMovingInfo) {
      this.blkMovingInfo = blkMovingInfo;
    }

    @Override
    /**
     * 执行块移动任务，返回移动结果
     * @return 块移动尝试完成结果
     */
    public BlockMovementAttemptFinished call() {
      BlockMovementStatus blkMovementStatus = moveBlock();
      return new BlockMovementAttemptFinished(blkMovingInfo.getBlock(),
          blkMovingInfo.getSource(), blkMovingInfo.getTarget(),
          blkMovingInfo.getTargetStorageType(),
          blkMovementStatus);
    }

    /**
     * 执行实际块移动逻辑，包含重试机制
     * @return 块移动状态（成功/失败）
     */
    private BlockMovementStatus moveBlock() {
      // 构造完整的块信息，添加块池ID
      ExtendedBlock eb = new ExtendedBlock(nnc.getBlockpoolID(),
          blkMovingInfo.getBlock());

      final KeyManager km = nnc.getKeyManager();
      Token<BlockTokenIdentifier> accessToken;
      int retry = 0;
      // 按最大重试次数循环尝试移动
      while (retry <= maxRetry) {
        try {
          // 故障注入：测试用模拟异常
          ExternalSPSFaultInjector.getInstance().mockAnException(retry);
          // 获取块访问令牌
          accessToken = km.getAccessToken(eb,
              new StorageType[]{blkMovingInfo.getTargetStorageType()},
              new String[0]);
          // 调用块分发器执行块移动
          return blkDispatcher.moveBlock(blkMovingInfo, saslClient, eb,
              new Socket(), km, accessToken);
        } catch (IOException e) {
          // 移动失败，记录日志并重试
          LOG.warn(
              "Failed to move block:{} from src:{} to dest:{} to satisfy "
                  + "storageType:{}, retry: {}",
              blkMovingInfo.getBlock(), blkMovingInfo.getSource(),
              blkMovingInfo.getTarget(), blkMovingInfo.getTargetStorageType(), retry, e);
          retry++;
        }
      }
      // 超过最大重试次数，返回失败
      return BlockMovementStatus.DN_BLK_STORAGE_MOVEMENT_FAILURE;
    }
  }

  /**
   * 清理资源，停止跟踪线程和线程池
   */
  void cleanUp() {
    blkMovementTracker.stopTracking();
    if (movementTrackerThread != null) {
      movementTrackerThread.interrupt();
    }
  }
}