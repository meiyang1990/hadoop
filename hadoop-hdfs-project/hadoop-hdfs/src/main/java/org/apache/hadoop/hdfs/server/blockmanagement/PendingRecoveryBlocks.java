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

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.hdfs.util.LightWeightHashSet;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;

import java.util.concurrent.TimeUnit;

/**
 * 待恢复块管理器，跟踪每个块的恢复尝试和它们的超时时间
 * 确保同一时间不会对同一个块发起多个恢复，且仅在上一次恢复超时后才会重试
 */
class PendingRecoveryBlocks {
  private static final Logger LOG = BlockManager.LOG;

  /** 存储所有块恢复尝试及其过期时间的集合 */
  private final LightWeightHashSet<BlockRecoveryAttempt> recoveryTimeouts =
      new LightWeightHashSet<>();

  /** 块恢复重试的超时时间间隔，该值应大于完成一次块恢复所需的时间 */
  private long recoveryTimeoutInterval;

  /**
   * 构造待恢复块管理器，设置恢复超时间隔
   * @param timeout 重试超时间隔（毫秒）
   */
  PendingRecoveryBlocks(long timeout) {
    this.recoveryTimeoutInterval = timeout;
  }

  /**
   * 移除指定块的恢复尝试记录，标记块恢复完成
   * @param block 需要移除记录的块
   */
  synchronized void remove(BlockInfo block) {
    recoveryTimeouts.remove(new BlockRecoveryAttempt(block));
  }

  /**
   * 尝试添加一个新的块恢复尝试，检查是否允许发起本次恢复
   * 如果已有未超时的恢复尝试，则拒绝本次恢复
   * @param block 需要恢复的块
   * @return true表示允许发起恢复（无历史尝试或历史尝试已超时），false表示不允许
   */
  synchronized boolean add(BlockInfo block) {
    boolean added = false;
    long curTime = getTime();
    BlockRecoveryAttempt recoveryAttempt =
        recoveryTimeouts.getElement(new BlockRecoveryAttempt(block));

    if (recoveryAttempt == null) {
      // 无历史恢复尝试，创建新尝试并添加到集合
      BlockRecoveryAttempt newAttempt = new BlockRecoveryAttempt(
          block, curTime + recoveryTimeoutInterval);
      added = recoveryTimeouts.add(newAttempt);
    } else if (recoveryAttempt.hasTimedOut(curTime)) {
      // 历史恢复尝试已超时，重置超时时间，允许重试
      recoveryAttempt.setTimeout(curTime + recoveryTimeoutInterval);
      added = true;
    } else {
      // 历史恢复尝试未超时，拒绝本次恢复，打印日志提示
      long timeoutIn = TimeUnit.MILLISECONDS.toSeconds(
          recoveryAttempt.timeoutAt - curTime);
      LOG.info("Block recovery attempt for " + block + " rejected, as the " +
          "previous attempt times out in " + timeoutIn + " seconds.");
    }
    return added;
  }

  /**
   * 检查指定块当前是否正在恢复中
   * @param b 需要检查的块
   * @return true表示块正在恢复，false表示块未在恢复
   */
  synchronized boolean isUnderRecovery(BlockInfo b) {
    BlockRecoveryAttempt recoveryAttempt =
        recoveryTimeouts.getElement(new BlockRecoveryAttempt(b));
    return recoveryAttempt != null;
  }

  /**
   * 获取当前时间，用于超时判断，可被子类覆盖测试
   * @return 当前时间（单调递增，毫秒）
   */
  long getTime() {
    return Time.monotonicNow();
  }

  /**
   * 设置恢复超时间隔，仅用于测试
   * @param recoveryTimeoutInterval 新的超时间隔（毫秒）
   */
  @VisibleForTesting
  synchronized void setRecoveryTimeoutInterval(long recoveryTimeoutInterval) {
    this.recoveryTimeoutInterval = recoveryTimeoutInterval;
  }

  /**
   * 单个块恢复尝试的记录，跟踪块信息和恢复超时时间点
   */
  private static class BlockRecoveryAttempt {
    private final BlockInfo blockInfo;
    private long timeoutAt;

    private BlockRecoveryAttempt(BlockInfo blockInfo) {
      this(blockInfo, 0);
    }

    /**
     * 构造块恢复尝试记录
     * @param blockInfo 需要恢复的块信息
     * @param timeoutAt 超时时间点（毫秒）
     */
    BlockRecoveryAttempt(BlockInfo blockInfo, long timeoutAt) {
      this.blockInfo = blockInfo;
      this.timeoutAt = timeoutAt;
    }

    /**
     * 检查当前恢复尝试是否已超时
     * @param currentTime 当前时间
     * @return true表示已超时，false表示未超时
     */
    boolean hasTimedOut(long currentTime) {
      return currentTime > timeoutAt;
    }

    /**
     * 设置新的超时时间点
     * @param newTimeoutAt 新的超时时间点
     */
    void setTimeout(long newTimeoutAt) {
      this.timeoutAt = newTimeoutAt;
    }

    @Override
    public int hashCode() {
      return blockInfo.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
      if (obj instanceof BlockRecoveryAttempt) {
        return this.blockInfo.equals(((BlockRecoveryAttempt) obj).blockInfo);
      }
      return false;
    }
  }
}