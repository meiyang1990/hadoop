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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableSet;
import org.apache.hadoop.yarn.api.records.QueueState;
import org.apache.hadoop.yarn.exceptions.YarnException;

import java.util.Set;

/**
 * 文件: 容量调度器队列状态管理辅助工具
 * 功能: 封装所有队列状态转移的处理逻辑，负责根据配置、父队列状态和当前状态计算目标队列状态
 */
public final class QueueStateHelper {
  // 合法的配置状态集合，仅允许RUNNING和STOPPED
  private static final Set<QueueState> VALID_STATE_CONFIGURATIONS = ImmutableSet.of(
      QueueState.RUNNING, QueueState.STOPPED);
  // 队列默认状态，默认为运行中
  private static final QueueState DEFAULT_STATE = QueueState.RUNNING;

  private QueueStateHelper() {}

  /**
   * 根据队列之前状态、父队列状态和配置状态，设置队列当前实际状态
   * @param queue 需要设置状态的目标队列
   */
  public static void setQueueState(AbstractCSQueue queue) {
    QueueState previousState = queue.getState();
    // 获取队列配置中指定的状态
    QueueState configuredState = queue.getQueueContext().getConfiguration().getConfiguredState(
        queue.getQueuePathObject());
    // 获取父队列当前状态，如果是根队列则为null
    QueueState parentState = (queue.getParent() == null) ? null : queue.getParent().getState();

    // 校验配置状态，仅允许RUNNING/STOPPED
    if (configuredState != null && !VALID_STATE_CONFIGURATIONS.contains(configuredState)) {
      throw new IllegalArgumentException("Invalid queue state configuration."
          + " We can only use RUNNING or STOPPED.");
    }

    // 首次初始化队列状态走初始化流程，已有状态走更新流程
    if (previousState == null) {
      initializeState(queue, configuredState, parentState);
    } else {
      reinitializeState(queue, previousState, configuredState);
    }
  }

  /**
   * 重新初始化队列状态，用于队列刷新场景，处理状态变更
   */
  private static void reinitializeState(
      AbstractCSQueue queue, QueueState previousState, QueueState configuredState) {
    // 原状态为运行中，配置改为停止 -> 停止队列
    if (previousState == QueueState.RUNNING) {
      if (configuredState == QueueState.STOPPED) {
        queue.stopQueue();
      }
    } else {
      // 原状态非运行中，配置改为运行 -> 激活队列
      if (configuredState == QueueState.RUNNING) {
        try {
          queue.activateQueue();
        } catch (YarnException ex) {
          throw new IllegalArgumentException(ex.getMessage());
        }
      }
    }
  }

  /**
   * 初始化新创建队列的状态，处理父子队列状态约束
   */
  private static void initializeState(
      AbstractCSQueue queue, QueueState configuredState, QueueState parentState) {
    // 未配置状态则使用默认状态RUNNING
    QueueState currentState = configuredState == null ? DEFAULT_STATE : configuredState;

    if (parentState != null) {
      // 校验状态约束：子队列配置为运行时，父队列不能是停止状态
      if (configuredState == QueueState.RUNNING && parentState != QueueState.RUNNING) {
        throw new IllegalArgumentException(
            "The parent queue:" + queue.getParent().getQueuePath()
                + " cannot be STOPPED as the child queue:" + queue.getQueuePath()
                + " is in RUNNING state.");
      }

      // 未显式配置状态时，继承父队列状态，父队列DRAINING时则设置为STOPPED
      if (configuredState == null) {
        currentState = parentState == QueueState.DRAINING ? QueueState.STOPPED : parentState;
      }
    }

    // 更新队列到计算后的最终状态
    queue.updateQueueState(currentState);
  }
}