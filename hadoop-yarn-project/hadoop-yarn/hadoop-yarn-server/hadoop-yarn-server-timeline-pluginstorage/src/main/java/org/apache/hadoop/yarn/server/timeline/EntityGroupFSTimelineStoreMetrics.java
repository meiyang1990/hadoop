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
package org.apache.hadoop.yarn.server.timeline;

import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.hadoop.metrics2.lib.MutableStat;

// 这个文件已经全部加上中文注释
// 跟踪EntityGroupFSTimelineStore指标的度量类，记录时间线服务器v1.5的读写指标，作为TimelineDataManagerMetrics的补充
/**
 * 管理EntityGroupFSTimelineStore的运行指标，收集时间线服务v1.5版本的读写操作统计，作为TimelineDataManagerMetrics的补充。
 */
@Metrics(about="Metrics for EntityGroupFSTimelineStore", context="yarn")
public class EntityGroupFSTimelineStoreMetrics {
  private static final String DEFAULT_VALUE_WITH_SCALE = "TimeMs";

  // 常规读操作相关指标
  @Metric("getEntity calls to summary storage")
  private MutableCounterLong getEntityToSummaryOps;

  @Metric("getEntity calls to detail storage")
  private MutableCounterLong getEntityToDetailOps;

  // 摘要数据相关指标
  @Metric(value = "summary log read ops and time",
      valueName = DEFAULT_VALUE_WITH_SCALE)
  private MutableStat summaryLogRead;

  @Metric("entities read into the summary storage")
  private MutableCounterLong entitiesReadToSummary;

  // 详情数据缓存相关指标
  @Metric("cache storage read that does not require a refresh")
  private MutableCounterLong noRefreshCacheRead;

  @Metric("cache storage refresh due to the cached storage is stale")
  private MutableCounterLong cacheStaleRefreshes;

  @Metric("cache storage evicts")
  private MutableCounterLong cacheEvicts;

  @Metric(value = "cache storage refresh ops and time",
      valueName = DEFAULT_VALUE_WITH_SCALE)
  private MutableStat cacheRefresh;

  // 日志扫描器和清理器相关指标
  @Metric(value = "active log scan ops and time",
      valueName = DEFAULT_VALUE_WITH_SCALE)
  private MutableStat activeLogDirScan;

  @Metric(value = "log cleaner purging ops and time",
      valueName = DEFAULT_VALUE_WITH_SCALE)
  private MutableStat logClean;

  @Metric("log cleaner dirs purged")
  private MutableCounterLong logsDirsCleaned;

  private static EntityGroupFSTimelineStoreMetrics instance = null;

  EntityGroupFSTimelineStoreMetrics() {
  }

  /**
   * 单例模式创建并获取指标实例，注册到默认指标系统。
   * @return EntityGroupFSTimelineStoreMetrics单例实例
   */
  public static synchronized EntityGroupFSTimelineStoreMetrics create() {
    if (instance == null) {
      MetricsSystem ms = DefaultMetricsSystem.instance();
      instance = ms.register(new EntityGroupFSTimelineStoreMetrics());
    }
    return instance;
  }

  // 指标更新方法
  // 常规读操作相关
  public void incrGetEntityToSummaryOps() {
    getEntityToSummaryOps.incr();
  }

  public void incrGetEntityToDetailOps() {
    getEntityToDetailOps.incr();
  }

  // 摘要数据相关
  public void addSummaryLogReadTime(long msec) {
    summaryLogRead.add(msec);
  }

  public void incrEntitiesReadToSummary(long delta) {
    entitiesReadToSummary.incr(delta);
  }

  // 缓存相关
  public void incrNoRefreshCacheRead() {
    noRefreshCacheRead.incr();
  }

  public void incrCacheStaleRefreshes() {
    cacheStaleRefreshes.incr();
  }

  public void incrCacheEvicts() {
    cacheEvicts.incr();
  }

  public void addCacheRefreshTime(long msec) {
    cacheRefresh.add(msec);
  }

  // 日志扫描器和清理器相关
  public void addActiveLogDirScanTime(long msec) {
    activeLogDirScan.add(msec);
  }

  public void addLogCleanTime(long msec) {
    logClean.add(msec);
  }

  public void incrLogsDirsCleaned() {
    logsDirsCleaned.incr();
  }

  // 指标获取方法
  MutableCounterLong getEntitiesReadToSummary() {
    return entitiesReadToSummary;
  }

  MutableCounterLong getLogsDirsCleaned() {
    return logsDirsCleaned;
  }

  MutableCounterLong getGetEntityToSummaryOps() {
    return getEntityToSummaryOps;
  }

  MutableCounterLong getGetEntityToDetailOps() {
    return getEntityToDetailOps;
  }

  MutableStat getCacheRefresh() {
    return cacheRefresh;
  }
}