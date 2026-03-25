// 这个文件已经全部加上中文注释
/*
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
package org.apache.hadoop.yarn.server.nodemanager.containermanager.container;

import org.apache.hadoop.util.Preconditions;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.yarn.api.records.ContainerRetryContext;
import org.apache.hadoop.yarn.api.records.ContainerRetryPolicy;
import org.apache.hadoop.yarn.util.Clock;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * <p>YARN NodeManager 容器重启动的滑动窗口重试策略。
 * 在指定时间窗口内统计容器失败次数，控制重试次数避免容器无限重启。</p>
 */
@InterfaceStability.Unstable
public class SlidingWindowRetryPolicy {

  private Clock clock;

  /**
   * 构造滑动窗口重试策略，使用指定时钟获取时间。
   * @param clock 时钟对象，用于获取当前时间
   */
  public SlidingWindowRetryPolicy(Clock clock)  {
    this.clock = Preconditions.checkNotNull(clock);
  }

  /**
   * 判断当前容器失败是否应该进行重试。
   * @param retryContext 重试上下文，包含重试配置与历史信息
   * @param errorCode 当前容器失败的错误码
   * @return true 允许重试，false 不允许重试
   */
  public boolean shouldRetry(RetryContext retryContext,
      int errorCode) {
    ContainerRetryContext containerRC = retryContext.containerRetryContext;
    Preconditions.checkNotNull(containerRC, "container retry context null");
    ContainerRetryPolicy retryPolicy = containerRC.getRetryPolicy();
    // 根据重试策略判断当前错误是否符合重试条件
    if (retryPolicy == ContainerRetryPolicy.RETRY_ON_ALL_ERRORS
        || (retryPolicy == ContainerRetryPolicy.RETRY_ON_SPECIFIC_ERROR_CODES
        && containerRC.getErrorCodes() != null
        && containerRC.getErrorCodes().contains(errorCode))) {
      // 如果是永久重试，直接允许；否则检查剩余重试次数
      return containerRC.getMaxRetries() == ContainerRetryContext.RETRY_FOREVER
          || calculateRemainingRetries(retryContext) > 0;
    }
    // 当前错误不符合重试策略，不允许重试
    return false;
  }

  /**
   * 计算当前还剩余多少次重试机会。
   * @param retryContext 重试上下文
   * @return 剩余重试次数
   */
  private int calculateRemainingRetries(RetryContext retryContext) {
    ContainerRetryContext containerRC =
        retryContext.containerRetryContext;
    // 配置了失败有效性间隔（滑动窗口），只统计窗口内的失败次数
    if (containerRC.getFailuresValidityInterval() > 0) {
      int validFailuresCount = 0;
      long currentTime = clock.getTime();
      // 从最新的重启时间向前遍历，统计窗口内失败次数
      for (int i = retryContext.restartTimes.size() - 1; i >= 0; i--) {
        long restartTime = retryContext.restartTimes.get(i);
        if (currentTime - restartTime
            <= containerRC.getFailuresValidityInterval()) {
          validFailuresCount++;
        } else {
          // 更早的失败一定也超出窗口，直接跳出循环
          break;
        }
      }
      // 总重试次数减去窗口内有效失败次数得到剩余次数
      return containerRC.getMaxRetries() - validFailuresCount;
    } else {
      // 未配置滑动窗口，直接使用上下文记录的剩余重试次数
      return retryContext.getRemainingRetries();
    }
  }

  /**
   * 容器重试后更新重试上下文，更新剩余重试次数和重启时间。
   * 如果配置了失败有效性间隔，会清理掉超出间隔的过期重启时间记录。
   * @param retryContext 待更新的重试上下文
   */
  protected void updateRetryContext(RetryContext retryContext) {
    if (retryContext.containerRetryContext.getFailuresValidityInterval() > 0) {
      ContainerRetryContext containerRC = retryContext.containerRetryContext;
      Iterator<Long> iterator = retryContext.getRestartTimes().iterator();
      long currentTime = clock.getTime();

      // 从最早的重启时间开始遍历，移除超出有效性间隔的过期记录
      while (iterator.hasNext()) {
        long restartTime = iterator.next();
        if (currentTime - restartTime
            > containerRC.getFailuresValidityInterval()) {
          iterator.remove();
        } else {
          // 后续的重启时间都更新更近，不需要移除，跳出循环
          break;
        }
      }
      // 更新剩余重试次数，并添加本次重启时间
      retryContext.setRemainingRetries(containerRC.getMaxRetries() -
          retryContext.restartTimes.size());
      retryContext.getRestartTimes().add(currentTime);
    } else {
      // 未配置滑动窗口，直接递减剩余重试次数
      retryContext.remainingRetries--;
    }
  }

  /**
   * 设置时钟对象。
   * @param clock 新的时钟对象
   */
  public void setClock(Clock clock) {
    this.clock = Preconditions.checkNotNull(clock);
  }

  /**
   * 滑动窗口重试策略的上下文类，保存重试状态信息。
   * 除了容器自带的重试配置外，额外保存：
   * <ul>
   * <li>
   * <em>remainingRetries</em>: 剩余可重试次数，初始值为配置的最大重试次数
   * </li>
   * <li>
   * <em>restartTimes</em>: 当配置失败有效性间隔时，记录所有容器重启的时间点，用于滑动窗口统计
   * </li>
   * </ul>
   */
  static class RetryContext {

    private final ContainerRetryContext containerRetryContext;
    private List<Long> restartTimes = new ArrayList<>();
    private int remainingRetries;

    /**
     * 构造重试上下文，基于容器自带的重试配置初始化。
     * @param containerRetryContext 容器自带的重试配置
     */
    RetryContext(ContainerRetryContext containerRetryContext) {
      this.containerRetryContext = Preconditions
          .checkNotNull(containerRetryContext);
      this.remainingRetries = containerRetryContext.getMaxRetries();
    }

    ContainerRetryContext getContainerRetryContext() {
      return containerRetryContext;
    }

    /**
     * 获取当前剩余可重试次数。
     * @return 剩余可重试次数，永久重试返回特殊标记 RETRY_FOREVER
     */
    int getRemainingRetries() {
      if (containerRetryContext.getMaxRetries() ==
          ContainerRetryContext.RETRY_FOREVER) {
        return ContainerRetryContext.RETRY_FOREVER;
      }
      return remainingRetries;
    }

    void setRemainingRetries(int remainingRetries) {
      this.remainingRetries = remainingRetries;
    }

    List<Long> getRestartTimes() {
      return restartTimes;
    }

    void setRestartTimes(List<Long> restartTimes) {
      if (restartTimes != null) {
        this.restartTimes.clear();
        this.restartTimes.addAll(restartTimes);
      }
    }
  }
}