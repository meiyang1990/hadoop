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

package org.apache.hadoop.yarn.server.timelineservice.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

/**
 * 文件说明: 时间线服务存储健康监控抽象基类，负责定时检查后端存储可用性，维护存储状态
 * 核心功能: 定时触发健康检查，维护存储上下线状态，对外提供存储状态检查能力
 */
public abstract class TimelineStorageMonitor  {
  private static final Logger LOG = LoggerFactory
      .getLogger(TimelineStorageMonitor.class);

  /** 支持的存储类型枚举，当前仅支持HBase存储 */
  public enum Storage {
    HBase
  }

  // 定时监控任务执行线程池
  private ScheduledExecutorService monitorExecutorService;
  // 健康检查间隔时间（毫秒）
  private long monitorInterval;
  // 被监控的存储类型
  private Storage storage;
  // 存储是否下线标记，原子变量保证线程安全
  private AtomicBoolean storageDown = new AtomicBoolean();

  /**
   * 构造函数，初始化监控配置
   * @param conf Hadoop配置对象
   * @param storage 要监控的存储类型
   */
  public TimelineStorageMonitor(Configuration conf, Storage storage) {
    this.storage = storage;
    this.monitorInterval = conf.getLong(
        YarnConfiguration.TIMELINE_SERVICE_READER_STORAGE_MONITOR_INTERVAL_MS,
        YarnConfiguration.DEFAULT_TIMELINE_SERVICE_STORAGE_MONITOR_INTERVAL_MS
        );
  }

  /**
   * 启动定时健康监控任务
   */
  public void start() {
    LOG.info("Scheduling {} storage monitor at interval {}",
        this.storage, monitorInterval);
    // 创建单线程定时线程池
    monitorExecutorService = Executors.newScheduledThreadPool(1);
    // 启动固定间隔的定时任务，首次立即执行
    monitorExecutorService.scheduleAtFixedRate(new MonitorThread(), 0,
        monitorInterval, TimeUnit.MILLISECONDS);
  }

  /**
   * 停止监控任务，关闭线程池
   * @throws Exception 停止超时或中断时抛出异常
   */
  public void stop() throws Exception {
    if (monitorExecutorService != null) {
      // 立即关闭线程池，中断正在执行的任务
      monitorExecutorService.shutdownNow();
      // 等待任务终止，最长等待30秒
      if (!monitorExecutorService.awaitTermination(30, TimeUnit.SECONDS)) {
        LOG.warn("Failed to stop the monitor task in time. " +
            "will still proceed to close the monitor.");
      }
    }
  }

  /**
   * 抽象健康检查方法，由具体存储实现类实现存储特定的检查逻辑
   * @throws Exception 健康检查失败时抛出异常
   */
  abstract public void healthCheck() throws Exception;

  /**
   * 检查存储当前是否可用，不可用则抛出IO异常
   * @throws IOException 存储已下线时抛出异常
   */
  public void checkStorageIsUp() throws IOException {
    if (storageDown.get()) {
      throw new IOException(storage + " is down");
    }
  }

  /**
   * 定时执行的健康检查任务线程
   */
  private class MonitorThread implements Runnable {
    @Override
    public void run() {
      try {
        LOG.debug("Running Timeline Storage monitor");
        // 调用抽象健康检查
        healthCheck();
        // 状态从下线变为上线，打印日志
        if (storageDown.getAndSet(false)) {
          LOG.debug("{} health check succeeded, " +
              "assuming storage is up", storage);
        }
      } catch (Exception e) {
        // 健康检查失败，标记存储为下线，打印警告日志
        LOG.warn(String.format("Got failure attempting to read from %s, " +
            "assuming Storage is down", storage), e);
        storageDown.getAndSet(true);
      }
    }
  }

}