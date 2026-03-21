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

package org.apache.hadoop.yarn.server.nodemanager.logaggregation.tracker;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.LogAggregationStatus;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.api.protocolrecords.LogAggregationReport;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.Application;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * NodeManager日志聚合状态跟踪器，用于缓存已完成应用的日志聚合状态，
 * 并定期清理过期的缓存状态，供RM恢复时获取日志聚合进度。
 *
 */
public class NMLogAggregationStatusTracker extends CompositeService {

  private static final Logger LOG =
       LoggerFactory.getLogger(NMLogAggregationStatusTracker.class);

  /** 读锁，保护缓存并发访问 */
  private final ReadLock readLocker;
  /** 写锁，保护缓存并发访问 */
  private final WriteLock writeLocker;
  /** NodeManager上下文引用 */
  private final Context nmContext;
  /** 缓存过期滚动清理间隔（毫秒） */
  private final long rollingInterval;
  /** 定期清理任务定时器 */
  private final Timer timer;
  /** 应用日志聚合状态缓存，键为应用ID，值为聚合状态信息 */
  private final Map<ApplicationId, AppLogAggregationStatusForRMRecovery>
      recoveryStatuses;
  /** 日志聚合功能是否禁用标记 */
  private boolean disabled = false;

  /**
   * 构造日志聚合状态跟踪器
   * @param context NodeManager上下文
   */
  public NMLogAggregationStatusTracker(Context context) {
    super(NMLogAggregationStatusTracker.class.getName());
    this.nmContext = context;
    Configuration conf = context.getConf();
    // 检查日志聚合是否启用，未启用则禁用本跟踪器
    if (!conf.getBoolean(YarnConfiguration.LOG_AGGREGATION_ENABLED,
        YarnConfiguration.DEFAULT_LOG_AGGREGATION_ENABLED)) {
      disabled = true;
    }
    this.recoveryStatuses = new ConcurrentHashMap<>();
    ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    this.readLocker = lock.readLock();
    this.writeLocker = lock.writeLock();
    this.timer = new Timer();
    // 读取配置的缓存过期时间
    long configuredRollingInterval = conf.getLong(
        YarnConfiguration.LOG_AGGREGATION_STATUS_TIME_OUT_MS,
        YarnConfiguration.DEFAULT_LOG_AGGREGATION_STATUS_TIME_OUT_MS);
    if (configuredRollingInterval <= 0) {
      // 配置非法，使用默认值
      this.rollingInterval = YarnConfiguration
          .DEFAULT_LOG_AGGREGATION_STATUS_TIME_OUT_MS;
      LOG.warn("The configured log-aggregation-status.time-out.ms is "
          + configuredRollingInterval + " which should be larger than 0. "
          + "Using the default value:" + this.rollingInterval + " instead.");
    } else {
      this.rollingInterval = configuredRollingInterval;
    }
    LOG.info("the rolling interval seconds for the NodeManager Cached Log "
        + "aggregation status is " + (rollingInterval/1000));
  }

  @Override
  protected void serviceStart() throws Exception {
    if (disabled) {
      LOG.warn("Log Aggregation is disabled."
          + "So is the LogAggregationStatusTracker.");
    } else {
      // 启动定期清理任务，按滚动间隔执行
      this.timer.scheduleAtFixedRate(new LogAggregationStatusRoller(),
          rollingInterval, rollingInterval);
    }
  }

  @Override
  public void serviceStop() throws Exception {
    this.timer.cancel();
  }

  /**
   * 更新指定应用的日志聚合状态缓存
   * @param appId 应用ID
   * @param logAggregationStatus 日志聚合状态
   * @param updateTime 更新时间戳
   * @param diagnosis 诊断信息
   * @param finalized 是否聚合完成
   */
  public void updateLogAggregationStatus(ApplicationId appId,
      LogAggregationStatus logAggregationStatus, long updateTime,
      String diagnosis, boolean finalized) {
    if (disabled) {
      LOG.warn("The log aggregation is disabled. No need to update "
          + "the log aggregation status");
    }
    // 每个应用仅单个聚合线程更新状态，使用读锁允许并发更新不同应用
    this.readLocker.lock();
    try {
      AppLogAggregationStatusForRMRecovery tracker = recoveryStatuses
          .get(appId);
      if (tracker == null) {
        // 新增缓存条目
        Application application = this.nmContext.getApplications().get(appId);
        if (application == null) {
          // 应用已被NM移除，忽略更新
          LOG.warn("The application:" + appId + " has already finished,"
              + " and has been removed from NodeManager, we should not "
              + "receive the log aggregation status update for "
              + "this application.");
          return;
        }
        AppLogAggregationStatusForRMRecovery newTracker =
            new AppLogAggregationStatusForRMRecovery(logAggregationStatus,
                diagnosis);
        newTracker.setLastModifiedTime(updateTime);
        newTracker.setFinalized(finalized);
        recoveryStatuses.put(appId, newTracker);
      } else {
        // 更新已有缓存条目
        if (tracker.isFinalized()) {
          // 已完成聚合，忽略后续更新
          LOG.warn("Ignore the log aggregation status update request "
              + "for the application:" + appId + ". The cached log aggregation "
              + "status is " + tracker.getLogAggregationStatus() + ".");
        } else {
          if (tracker.getLastModifiedTime() > updateTime) {
            // 新请求时间早于缓存时间，忽略过时更新
            LOG.warn("Ignore the log aggregation status update request "
                + "for the application:" + appId + ". The request log "
                + "aggregation status update is older than the cached "
                + "log aggregation status.");
          } else {
            // 更新缓存状态
            tracker.setLogAggregationStatus(logAggregationStatus);
            tracker.setDiagnosis(diagnosis);
            tracker.setLastModifiedTime(updateTime);
            tracker.setFinalized(finalized);
            recoveryStatuses.put(appId, tracker);
          }
        }
      }
    } finally {
      this.readLocker.unlock();
    }
  }

  /**
   * 拉取当前NM所有缓存的日志聚合报告，供RM获取
   * @return 日志聚合报告列表
   */
  public List<LogAggregationReport> pullCachedLogAggregationReports() {
    List<LogAggregationReport> reports = new ArrayList<>();
    if (disabled) {
      LOG.warn("The log aggregation is disabled."
          + "There is no cached log aggregation status.");
      return reports;
    }
    // 拉取全量缓存需要阻塞所有更新操作，使用写锁
    this.writeLocker.lock();
    try {
      // 遍历所有缓存生成报告
      for(Entry<ApplicationId, AppLogAggregationStatusForRMRecovery> tracker :
          recoveryStatuses.entrySet()) {
        AppLogAggregationStatusForRMRecovery current = tracker.getValue();
        LogAggregationReport report = LogAggregationReport.newInstance(
            tracker.getKey(), current.getLogAggregationStatus(),
            current.getDiagnosis());
        reports.add(report);
      }
      return reports;
    } finally {
      this.writeLocker.unlock();
    }
  }

  /**
   * 定时滚动清理任务实现类
   */
  private class LogAggregationStatusRoller extends TimerTask {
    @Override
    public void run() {
      rollLogAggregationStatus();
    }
  }

  /**
   * 滚动清理过期的日志聚合状态缓存
   */
  private void rollLogAggregationStatus() {
    // 清理全量过期缓存需要阻塞更新和拉取操作，使用写锁
    this.writeLocker.lock();
    try {
      long currentTimeStamp = System.currentTimeMillis();
      LOG.info("Rolling over the cached log aggregation status.");
      Iterator<Entry<ApplicationId, AppLogAggregationStatusForRMRecovery>> it
          = recoveryStatuses.entrySet().iterator();
      while (it.hasNext()) {
        Entry<ApplicationId, AppLogAggregationStatusForRMRecovery> tracker =
            it.next();
        // 仅清理NM中已经移除的应用
        if (nmContext.getApplications().get(tracker.getKey()) == null) {
          // 删除超过过期时间的缓存
          if (currentTimeStamp - tracker.getValue().getLastModifiedTime()
              > rollingInterval) {
            it.remove();
          }
        }
      }
    } finally {
      this.writeLocker.unlock();
    }
  }

  /**
   * 存储单个应用日志聚合状态的内部数据结构，供RM恢复使用
   */
  private static class AppLogAggregationStatusForRMRecovery {
    private LogAggregationStatus logAggregationStatus;
    private long lastModifiedTime;
    private boolean finalized;
    private String diagnosis;

    AppLogAggregationStatusForRMRecovery(
        LogAggregationStatus logAggregationStatus, String diagnosis) {
      this.setLogAggregationStatus(logAggregationStatus);
      this.setDiagnosis(diagnosis);
    }

    public LogAggregationStatus getLogAggregationStatus() {
      return logAggregationStatus;
    }

    public void setLogAggregationStatus(
        LogAggregationStatus logAggregationStatus) {
      this.logAggregationStatus = logAggregationStatus;
    }

    public long getLastModifiedTime() {
      return lastModifiedTime;
    }

    public void setLastModifiedTime(long lastModifiedTime) {
      this.lastModifiedTime = lastModifiedTime;
    }

    public boolean isFinalized() {
      return finalized;
    }

    public void setFinalized(boolean finalized) {
      this.finalized = finalized;
    }

    public String getDiagnosis() {
      return diagnosis;
    }

    public void setDiagnosis(String diagnosis) {
      this.diagnosis = diagnosis;
    }
  }
}