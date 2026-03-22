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

package org.apache.hadoop.mapreduce.v2.hs;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.JobState;
import org.apache.hadoop.mapreduce.v2.app.ClusterInfo;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.app.TaskAttemptFinishingMonitor;
import org.apache.hadoop.mapreduce.v2.hs.HistoryFileManager.HistoryFileInfo;
import org.apache.hadoop.mapreduce.v2.hs.webapp.dao.JobsInfo;
import org.apache.hadoop.mapreduce.v2.jobhistory.JHAdminConfig;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.concurrent.HadoopScheduledThreadPoolExecutor;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.event.Event;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.security.client.ClientToAMTokenSecretManager;
import org.apache.hadoop.yarn.util.Clock;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 文件: JobHistory.java
 * 归属模块: hadoop-mapreduce-client-hs
 * 核心职责: 负责作业历史文件的加载、缓存管理和定期清理，为MapReduce作业历史服务器提供已完成作业的查询能力
 */

/**
 * 加载和管理作业历史缓存，实现HistoryContext接口，提供作业历史查询能力
 */
public class JobHistory extends AbstractService implements HistoryContext {
  private static final Logger LOG = LoggerFactory.getLogger(JobHistory.class);

  public static final Pattern CONF_FILENAME_REGEX = Pattern.compile("("
      + JobID.JOBID_REGEX + ")_conf.xml(?:\\.[0-9]+\\.old)?");
  public static final String OLD_SUFFIX = ".old";

  // 移动中间历史文件线程的执行间隔
  private long moveThreadInterval;

  private Configuration conf;

  private ScheduledThreadPoolExecutor scheduledExecutor = null;

  private HistoryStorage storage = null;
  private HistoryFileManager hsManager = null;
  ScheduledFuture<?> futureHistoryCleaner = null;
  
  // 历史作业清理线程执行间隔
  private long cleanerInterval;
  
  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    LOG.info("JobHistory Init");
    this.conf = conf;
    this.appID = ApplicationId.newInstance(0, 0);
    this.appAttemptID = RecordFactoryProvider.getRecordFactory(conf)
        .newRecordInstance(ApplicationAttemptId.class);
    // 从配置读取移动中间文件线程间隔
    moveThreadInterval = conf.getLong(
        JHAdminConfig.MR_HISTORY_MOVE_INTERVAL_MS,
        JHAdminConfig.DEFAULT_MR_HISTORY_MOVE_INTERVAL_MS);
    // 创建并初始化历史文件管理器
    hsManager = createHistoryFileManager();
    hsManager.init(conf);
    try {
      // 初始化已存在的历史目录
      hsManager.initExisting();
    } catch (IOException e) {
      throw new YarnRuntimeException("Failed to initialize existing directories", e);
    }
    // 创建历史存储实例
    storage = createHistoryStorage();
    // 如果存储是服务则初始化
    if (storage instanceof Service) {
      ((Service) storage).init(conf);
    }
    storage.setHistoryFileManager(hsManager);

    super.serviceInit(conf);
  }

  /**
   * 通过反射创建配置指定的历史存储实现实例
   * @return 历史存储实例
   */
  protected HistoryStorage createHistoryStorage() {
    return ReflectionUtils.newInstance(conf.getClass(
        JHAdminConfig.MR_HISTORY_STORAGE, CachedHistoryStorage.class,
        HistoryStorage.class), conf);
  }
  
  /**
   * 创建历史文件管理器实例，子类可覆盖
   * @return 历史文件管理器实例
   */
  protected HistoryFileManager createHistoryFileManager() {
    return new HistoryFileManager();
  }

  @Override
  protected void serviceStart() throws Exception {
    // 启动历史文件管理器
    hsManager.start();
    if (storage instanceof Service) {
      ((Service) storage).start();
    }
    // 创建定时线程池，用于日志扫描和清理任务
    scheduledExecutor = new HadoopScheduledThreadPoolExecutor(2,
        new ThreadFactoryBuilder().setNameFormat("Log Scanner/Cleaner #%d")
            .build());
    // 启动定时任务，定期将中间历史文件移动到完成目录
    scheduledExecutor.scheduleAtFixedRate(new MoveIntermediateToDoneRunnable(),
        moveThreadInterval, moveThreadInterval, TimeUnit.MILLISECONDS);
    // 启动历史清理定时任务
    scheduleHistoryCleaner();
    super.serviceStart();
  }

  /**
   * 获取历史清理任务初始延迟秒数
   * @return 初始延迟秒数
   */
  protected int getInitDelaySecs() {
    return 30;
  }

  @Override
  protected void serviceStop() throws Exception {
    LOG.info("Stopping JobHistory");
    if (scheduledExecutor != null) {
      LOG.info("Stopping History Cleaner/Move To Done");
      scheduledExecutor.shutdown();
      int retryCnt = 50;
      try {
        // 等待线程池优雅终止，超时则强制关闭
        while (!scheduledExecutor.awaitTermination(20,
            TimeUnit.MILLISECONDS)) {
          if (--retryCnt == 0) {
            scheduledExecutor.shutdownNow();
            break;
          }
        }
      } catch (InterruptedException iex) {
        LOG.warn("HistoryCleanerService/move to done shutdown may not have " +
            "succeeded, Forcing a shutdown", iex);
        if (!scheduledExecutor.isShutdown()) {
          scheduledExecutor.shutdownNow();
        }
      }
      scheduledExecutor = null;
    }
    // 停止存储服务
    if (storage != null && storage instanceof Service) {
      ((Service) storage).stop();
    }
    // 停止历史文件管理器
    if (hsManager != null) {
      hsManager.stop();
    }
    super.serviceStop();
  }

  /**
   * 构造JobHistory实例
   */
  public JobHistory() {
    super(JobHistory.class.getName());
  }

  @Override
  public String getApplicationName() {
    return "Job History Server";
  }

  /**
   * 定时将中间完成的作业历史文件移动到最终Done目录的任务
   */
  private class MoveIntermediateToDoneRunnable implements Runnable {
    @Override
    public void run() {
      try {
        LOG.info("Starting scan to move intermediate done files");
        // 扫描中间目录移动文件
        hsManager.scanIntermediateDirectory();
      } catch (IOException e) {
        LOG.error("Error while scanning intermediate done dir ", e);
      }
    }
  }
  
  /**
   * 定时清理过期历史作业文件的任务
   */
  private class HistoryCleaner implements Runnable {
    public void run() {
      LOG.info("History Cleaner started");
      try {
        // 执行过期文件清理
        hsManager.clean();
      } catch (IOException e) {
        LOG.warn("Error trying to clean up ", e);
      }
      LOG.info("History Cleaner complete");
    }
  }

  /**
   * 测试用辅助方法：获取指定作业的历史文件信息
   * @param jobId 作业ID
   * @return 历史文件信息
   * @throws IOException IO异常
   */
  @VisibleForTesting
  HistoryFileInfo getJobFileInfo(JobId jobId) throws IOException {
    return hsManager.getFileInfo(jobId);
  }

  @Override
  public Job getJob(JobId jobId) {
    return storage.getFullJob(jobId);
  }

  @Override
  public Map<JobId, Job> getAllJobs(ApplicationId appID) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Called getAllJobs(AppId): " + appID);
    }
    // currently there is 1 to 1 mapping between app and job id
    org.apache.hadoop.mapreduce.JobID oldJobID = TypeConverter.fromYarn(appID);
    Map<JobId, Job> jobs = new HashMap<JobId, Job>();
    JobId jobID = TypeConverter.toYarn(oldJobID);
    jobs.put(jobID, getJob(jobID));
    return jobs;
  }

  @Override
  public Map<JobId, Job> getAllJobs() {
    return storage.getAllPartialJobs();
  }

  /**
   * 刷新已加载的作业缓存，触发缓存重新加载
   */
  public void refreshLoadedJobCache() {
    if (getServiceState() == STATE.STARTED) {
      if (storage instanceof CachedHistoryStorage) {
        ((CachedHistoryStorage) storage).refreshLoadedJobCache();
      } else {
        throw new UnsupportedOperationException(storage.getClass().getName()
            + " is expected to be an instance of "
            + CachedHistoryStorage.class.getName());
      }
    } else {
      LOG.warn("Failed to execute refreshLoadedJobCache: JobHistory service is not started");
    }
  }

  @VisibleForTesting
  HistoryStorage getHistoryStorage() {
    return storage;
  }
  
  @Override
  public JobsInfo getPartialJobs(Long offset, Long count, String user,
      String queue, Long sBegin, Long sEnd, Long fBegin, Long fEnd,
      JobState jobState) {
    return storage.getPartialJobs(offset, count, user, queue, sBegin, sEnd,
        fBegin, fEnd, jobState);
  }

  /**
   * 刷新作业历史保留设置，重新读取配置更新清理参数
   */
  public void refreshJobRetentionSettings() {
    if (getServiceState() == STATE.STARTED) {
      conf = createConf();
      // 重新读取最大历史保留时间
      long maxHistoryAge = conf.getLong(JHAdminConfig.MR_HISTORY_MAX_AGE_MS,
          JHAdminConfig.DEFAULT_MR_HISTORY_MAX_AGE);
      hsManager.setMaxHistoryAge(maxHistoryAge);
      // 取消原有清理任务，重新调度
      if (futureHistoryCleaner != null) {
        futureHistoryCleaner.cancel(false);
      }
      futureHistoryCleaner = null;
      scheduleHistoryCleaner();
    } else {
      LOG.warn("Failed to execute refreshJobRetentionSettings : Job History service is not started");
    }
  }

  /**
   * 调度历史清理定时任务
   */
  private void scheduleHistoryCleaner() {
    // 从配置读取清理开关
    boolean startCleanerService = conf.getBoolean(
        JHAdminConfig.MR_HISTORY_CLEANER_ENABLE, true);
    if (startCleanerService) {
      // 读取清理间隔
      cleanerInterval = conf.getLong(
          JHAdminConfig.MR_HISTORY_CLEANER_INTERVAL_MS,
          JHAdminConfig.DEFAULT_MR_HISTORY_CLEANER_INTERVAL_MS);
      // 启动定时清理任务
      futureHistoryCleaner = scheduledExecutor.scheduleAtFixedRate(
          new HistoryCleaner(), getInitDelaySecs() * 1000l, cleanerInterval,
          TimeUnit.MILLISECONDS);
    }
  }

  /**
   * 创建新的配置实例，用于刷新配置
   * @return 新配置实例
   */
  protected Configuration createConf() {
    return new Configuration();
  }
  
  public long getCleanerInterval() {
    return cleanerInterval;
  }
  // TODO AppContext - Not Required
  private ApplicationAttemptId appAttemptID;

  @Override
  public ApplicationAttemptId getApplicationAttemptId() {
    // TODO fixme - bogus appAttemptID for now
    return appAttemptID;
  }

  // TODO AppContext - Not Required
  private ApplicationId appID;

  @Override
  public ApplicationId getApplicationID() {
    // TODO fixme - bogus appID for now
    return appID;
  }

  // TODO AppContext - Not Required
  @Override
  public EventHandler<Event> getEventHandler() {
    // TODO Auto-generated method stub
    return null;
  }

  // TODO AppContext - Not Required
  private String userName;

  @Override
  public CharSequence getUser() {
    if (userName != null) {
      userName = conf.get(MRJobConfig.USER_NAME, "history-user");
    }
    return userName;
  }

  // TODO AppContext - Not Required
  @Override
  public Clock getClock() {
    return null;
  }

  // TODO AppContext - Not Required
  @Override
  public ClusterInfo getClusterInfo() {
    return null;
  }

  // TODO AppContext - Not Required
  @Override
  public Set<String> getBlacklistedNodes() {
    // Not Implemented
    return null;
  }
  @Override
  public ClientToAMTokenSecretManager getClientToAMTokenSecretManager() {
    // Not implemented.
    return null;
  }

  @Override
  public boolean isLastAMRetry() {
    // bogus - Not Required
    return false;
  }

  @Override
  public boolean hasSuccessfullyUnregistered() {
    // bogus - Not Required
    return true;
  }

  @Override
  public String getNMHostname() {
    // bogus - Not Required
    return null;
  }

  @Override
  public TaskAttemptFinishingMonitor getTaskAttemptFinishingMonitor() {
    return null;
  }

  @Override
  public String getHistoryUrl() {
    return null;
  }

  @Override
  public void setHistoryUrl(String historyUrl) {
    return;
  }
}