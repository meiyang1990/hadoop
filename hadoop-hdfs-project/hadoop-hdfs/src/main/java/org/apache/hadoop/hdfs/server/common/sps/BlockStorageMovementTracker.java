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
package org.apache.hadoop.hdfs.server.common.sps;

import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * HDFS存储策略满足器（SPS）模块的块移动任务追踪器，负责跟踪异步块移动任务的完成状态，
 * 并将完成结果通知给状态处理器，实现块移动结果的异步处理。
 * 该类作为独立线程运行，持续从完成服务中获取已完成的块移动任务并处理结果。
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class BlockStorageMovementTracker implements Runnable {
  private static final Logger LOG = LoggerFactory
      .getLogger(BlockStorageMovementTracker.class);
  // 存储块移动任务的完成服务，用于获取已完成的任务结果
  private final CompletionService<BlockMovementAttemptFinished>
      moverCompletionService;
  // 块移动完成结果处理器，用于处理已完成的块移动任务状态
  private final BlocksMovementsStatusHandler blksMovementsStatusHandler;

  // 线程运行标志，volatile保证多线程可见性
  private volatile boolean running = true;

  /**
   * 构造块存储移动任务追踪器实例。
   *
   * @param moverCompletionService 用于获取已完成块移动任务的完成服务
   * @param handler 处理块移动完成结果的处理器
   */
  public BlockStorageMovementTracker(
      CompletionService<BlockMovementAttemptFinished> moverCompletionService,
      BlocksMovementsStatusHandler handler) {
    this.moverCompletionService = moverCompletionService;
    this.blksMovementsStatusHandler = handler;
  }

  @Override
  public void run() {
    // 持续运行直到停止追踪
    while (running) {
      try {
        // 阻塞获取下一个已完成的任务结果
        Future<BlockMovementAttemptFinished> future = moverCompletionService
            .take();
        if (future != null) {
          // 获取块移动任务的完成结果
          BlockMovementAttemptFinished result = future.get();
          LOG.debug("Completed block movement. {}", result);
          // 如果追踪器仍在运行且处理器存在，通知处理器处理结果
          if (running && blksMovementsStatusHandler != null) {
            // handle completed block movement.
            blksMovementsStatusHandler.handle(result);
          }
        }
      } catch (InterruptedException e) {
        // 仅在仍运行时打印异常，正常退出时不打印错误
        if (running) {
          LOG.error("Exception while moving block replica to target storage"
              + " type", e);
        }
      } catch (ExecutionException e) {
        // TODO: Do we need failure retries and implement the same if required.
        LOG.error("Exception while moving block replica to target storage type",
            e);
      }
    }
  }

  /**
   * 停止块移动任务追踪，终止线程运行。
   */
  public void stopTracking() {
    running = false;
  }
}