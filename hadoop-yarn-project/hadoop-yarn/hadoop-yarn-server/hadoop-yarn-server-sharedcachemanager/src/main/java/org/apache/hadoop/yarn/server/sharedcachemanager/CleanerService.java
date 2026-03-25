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

package org.apache.hadoop.yarn.server.sharedcachemanager;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.util.concurrent.HadoopExecutors;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.sharedcachemanager.metrics.CleanerMetrics;
import org.apache.hadoop.yarn.server.sharedcachemanager.store.SCMStore;

import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// 这个文件已经全部加上中文注释
// 共享缓存清理服务，负责维护共享缓存区域并定期清理过期条目
/**
 * 文件说明：YARN共享缓存管理器清理服务，负责定期维护共享缓存区域，清理过期过时的缓存条目
 * 核心职责：保证共享缓存不会无限增长，自动清理无人使用的陈旧缓存资源，通过PID文件保证集群中同一时间只有一个清理服务运行
 */
@Private
@Evolving
public class CleanerService extends CompositeService {
  /**
   * 全局清理锁文件名，用于标识当前正在执行清理流程，防止多实例并发清理
   */
  public static final String GLOBAL_CLEANER_PID = ".cleaner_pid";

  private static final Logger LOG =
      LoggerFactory.getLogger(CleanerService.class);

  private Configuration conf;
  private CleanerMetrics metrics;
  private ScheduledExecutorService scheduledExecutor;
  private final SCMStore store;
  private final Lock cleanerTaskLock;

  /**
   * 构造清理服务，依赖共享缓存存储
   * @param store 共享缓存存储对象
   */
  public CleanerService(SCMStore store) {
    super("CleanerService");
    this.store = store;
    this.cleanerTaskLock = new ReentrantLock();
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    this.conf = conf;

    // 创建清理任务调度线程池，使用2个线程同时容纳定时任务和按需任务，降低任务排队概率
    ThreadFactory tf =
        new ThreadFactoryBuilder().setNameFormat("Shared cache cleaner").build();
    scheduledExecutor = HadoopExecutors.newScheduledThreadPool(2, tf);
    super.serviceInit(conf);
  }

  @Override
  protected void serviceStart() throws Exception {
    // 尝试创建全局PID文件，失败则说明已有实例运行，抛出异常终止启动
    if (!writeGlobalCleanerPidFile()) {
      throw new YarnException("The global cleaner pid file already exists! " +
          "It appears there is another CleanerService running in the cluster");
    }

    // 获取清理服务指标实例
    this.metrics = CleanerMetrics.getInstance();

    // 启动依赖的子服务（如AppChecker应用检查服务）
    super.serviceStart();

    // 创建清理任务实例
    Runnable task =
        CleanerTask.create(conf, store, metrics, cleanerTaskLock);
    // 从配置读取清理周期（单位：分钟）
    long periodInMinutes = getPeriod(conf);
    // 按固定周期调度清理任务，设置初始延迟和执行周期
    scheduledExecutor.scheduleAtFixedRate(task, getInitialDelay(conf),
        periodInMinutes, TimeUnit.MINUTES);
    LOG.info("Scheduled the shared cache cleaner task to run every "
        + periodInMinutes + " minutes.");
  }

  @Override
  protected void serviceStop() throws Exception {
    LOG.info("Shutting down the background thread.");
    // 立即关闭调度线程池，中断正在执行的任务
    scheduledExecutor.shutdownNow();
    try {
      // 等待任务终止，超时时间10秒
      if (scheduledExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
        LOG.info("The background thread stopped.");
      } else {
        LOG.warn("Gave up waiting for the cleaner task to shutdown.");
      }
    } catch (InterruptedException e) {
      LOG.warn("The cleaner service was interrupted while shutting down the task.",
          e);
    }

    // 服务停止后删除全局PID文件，释放锁
    removeGlobalCleanerPidFile();

    super.serviceStop();
  }

  /**
   * 触发一次按需清理任务，提交到调度线程池异步执行
   */
  protected void runCleanerTask() {
    Runnable task =
        CleanerTask.create(conf, store, metrics, cleanerTaskLock);
    // 非阻塞调用，仅提交任务到队列后立即返回
    this.scheduledExecutor.execute(task);
  }

  /**
   * 创建全局PID文件，防止集群中多个清理服务实例同时运行，保证单实例运行
   * PID文件会记录当前运行节点的主机名和进程ID
   * @return 创建成功返回true，文件已存在返回false
   * @throws YarnException IO异常时封装抛出
   */
  private boolean writeGlobalCleanerPidFile() throws YarnException {
    // 获取共享缓存根目录
    String root =
        conf.get(YarnConfiguration.SHARED_CACHE_ROOT,
            YarnConfiguration.DEFAULT_SHARED_CACHE_ROOT);
    Path pidPath = new Path(root, GLOBAL_CLEANER_PID);
    try {
      FileSystem fs = FileSystem.get(this.conf);

      // 文件已存在说明已有实例运行，直接返回失败
      if (fs.exists(pidPath)) {
        return false;
      }

      // 创建新PID文件，不覆盖已有文件
      FSDataOutputStream os = fs.create(pidPath, false);
      // 写入当前JVM的进程标识（主机名+进程ID）
      final String ID = ManagementFactory.getRuntimeMXBean().getName();
      os.writeUTF(ID);
      os.close();
      // 注册JVM退出时自动删除该文件，防止服务异常退出后文件残留
      fs.deleteOnExit(pidPath);
    } catch (IOException e) {
      throw new YarnException(e);
    }
    LOG.info("Created the global cleaner pid file at " + pidPath.toString());
    return true;
  }

  /**
   * 删除全局PID文件，释放清理服务运行锁
   */
  private void removeGlobalCleanerPidFile() {
    try {
      FileSystem fs = FileSystem.get(this.conf);
      // 获取共享缓存根目录
      String root =
          conf.get(YarnConfiguration.SHARED_CACHE_ROOT,
              YarnConfiguration.DEFAULT_SHARED_CACHE_ROOT);

      Path pidPath = new Path(root, GLOBAL_CLEANER_PID);

      // 删除PID文件
      fs.delete(pidPath, false);
      LOG.info("Removed the global cleaner pid file at " + pidPath.toString());
    } catch (IOException e) {
      LOG.error(
          "Unable to remove the global cleaner pid file! The file may need "
              + "to be removed manually.", e);
    }
  }

  /**
   * 从配置读取清理任务初始延迟时间，校验参数合法性
   * @param conf 配置对象
   * @return 初始延迟分钟数
   */
  private static int getInitialDelay(Configuration conf) {
    int initialDelayInMinutes =
        conf.getInt(YarnConfiguration.SCM_CLEANER_INITIAL_DELAY_MINS,
            YarnConfiguration.DEFAULT_SCM_CLEANER_INITIAL_DELAY_MINS);
    // 初始延迟不能为负，非法值抛出异常
    if (initialDelayInMinutes < 0) {
      throw new HadoopIllegalArgumentException("Negative initial delay value: "
          + initialDelayInMinutes
          + ". The initial delay must be greater than zero.");
    }
    return initialDelayInMinutes;
  }

  /**
   * 从配置读取清理任务执行周期，校验参数合法性
   * @param conf 配置对象
   * @return 清理周期分钟数
   */
  private static int getPeriod(Configuration conf) {
    int periodInMinutes =
        conf.getInt(YarnConfiguration.SCM_CLEANER_PERIOD_MINS,
            YarnConfiguration.DEFAULT_SCM_CLEANER_PERIOD_MINS);
    // 周期必须为正，非法值抛出异常
    if (periodInMinutes <= 0) {
      throw new HadoopIllegalArgumentException("Non-positive period value: "
          + periodInMinutes
          + ". The cleaner period must be greater than or equal to zero.");
    }
    return periodInMinutes;
  }
}