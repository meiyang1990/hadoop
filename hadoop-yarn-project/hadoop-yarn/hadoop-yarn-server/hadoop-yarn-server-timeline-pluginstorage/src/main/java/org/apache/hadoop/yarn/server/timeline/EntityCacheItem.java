// 这个文件已经全部加上中文注释
/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.yarn.server.timeline;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntityGroupId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.timeline.security.TimelineACLsManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * 文件级：时间线服务v1.5实体组缓存项，每个缓存项对应一个实体组，封装该组所有时间线实体数据的存储与刷新逻辑
 * 缓存项用于时间线服务器v1.5读取器缓存，每个缓存项包含一个可填充单个实体组数据的TimelineStore实例。
 */
public class EntityCacheItem {
  private static final Logger LOG
      = LoggerFactory.getLogger(EntityCacheItem.class);

  private TimelineStore store;
  private TimelineEntityGroupId groupId;
  private EntityGroupFSTimelineStore.AppLogs appLogs;
  private long lastRefresh;
  private Configuration config;

  /**
   * 构造指定实体组的缓存项实例
   * @param gId 实体组ID
   * @param config Yarn配置对象
   */
  public EntityCacheItem(TimelineEntityGroupId gId, Configuration config) {
    this.groupId = gId;
    this.config = config;
  }

  /**
   * 获取当前缓存项关联的应用日志对象，可能为空
   * @return 当前缓存项关联的应用日志对象，可能为null
   */
  public synchronized EntityGroupFSTimelineStore.AppLogs getAppLogs() {
    return this.appLogs;
  }

  /**
   * 设置当前缓存项关联的应用日志对象
   * @param incomingAppLogs 要关联的应用日志对象
   */
  public synchronized void setAppLogs(
      EntityGroupFSTimelineStore.AppLogs incomingAppLogs) {
    this.appLogs = incomingAppLogs;
  }

  /**
   * 获取当前缓存项的时间线存储实例，无论是否已加载数据，该方法不会阻止存储被回收
   * @return 当前缓存项的时间线存储实例
   */
  public synchronized TimelineStore getStore() {
    return store;
  }

  /**
   * 如果缓存过期则刷新缓存项，强制重新扫描应用日志并加载新数据，刷新过程与同缓存项的其他操作同步互斥
   * @param aclManager 时间线存储的ACL访问控制管理器
   * @param metrics 实体组存储状态指标收集对象
   * @return 填充了当前实体组所有实体数据的TimelineStore实例
   * @throws IOException 刷新过程中IO异常
   */
  public synchronized TimelineStore refreshCache(TimelineACLsManager aclManager,
      EntityGroupFSTimelineStoreMetrics metrics) throws IOException {
    // 判断是否需要刷新缓存
    if (needRefresh()) {
      // 记录刷新开始时间
      long startTime = Time.monotonicNow();
      // 应用未完成时只更新摘要日志，应用已完成时扫描完整详情日志
      if (!appLogs.isDone()) {
        // 应用未运行结束，仅解析摘要日志
        appLogs.parseSummaryLogs();
      } else if (appLogs.getDetailLogs().isEmpty()) {
        // 应用已结束，还未扫描过详情日志，扫描日志文件
        appLogs.scanForLogs();
      }
      // 如果存在可加载的详情日志，加载数据到缓存存储
      if (!appLogs.getDetailLogs().isEmpty()) {
        if (store == null) {
          // 第一次加载，通过反射创建配置指定的TimelineStore实现类实例
          store = ReflectionUtils.newInstance(config.getClass(
              YarnConfiguration
                  .TIMELINE_SERVICE_ENTITYGROUP_FS_STORE_CACHE_STORE,
              MemoryTimelineStore.class, TimelineStore.class),
              config);
          // 初始化存储
          store.init(config);
          // 启动存储服务
          store.start();
        } else {
          // 存储已存在，本次刷新是因为缓存过期，指标计数+1
          metrics.incrCacheStaleRefreshes();
        }
        // 使用try-with-resources自动关闭TimelineDataManager
        try (TimelineDataManager tdm =
                new TimelineDataManager(store, aclManager)) {
          // 初始化数据管理器
          tdm.init(config);
          // 启动数据管理器
          tdm.start();
          // 从应用日志加载详情数据到存储
          appLogs.loadDetailLog(tdm, groupId);
        }
      }
      // 更新最后刷新时间为当前时间
      updateRefreshTimeToNow();
      // 记录本次刷新耗时到指标
      metrics.addCacheRefreshTime(Time.monotonicNow() - startTime);
    } else {
      // 缓存足够新，跳过刷新
      LOG.debug("Cache new enough, skip refreshing");
      // 指标增加未刷新读取计数
      metrics.incrNoRefreshCacheRead();
    }
    return store;
  }

  /**
   * 强制释放当前缓存项资源，即使存在活跃引用也会释放
   */
  public synchronized void forceRelease() {
    try {
      if (store != null) {
        // 关闭存储实例
        store.close();
      }
    } catch (IOException e) {
      // 关闭失败记录警告日志
      LOG.warn("Error closing timeline store", e);
    }
    // 置空存储引用
    store = null;
    // 重置所有当前组日志文件的读取偏移量，下次加载从头开始解析
    for (LogInfo log : appLogs.getDetailLogs()) {
      if (log.getFilename().contains(groupId.toString())) {
        log.setOffset(0);
      }
    }
    LOG.debug("Cache for group {} released. ", groupId);
  }

  /**
   * 判断当前缓存是否需要刷新，超过10秒未刷新则需要刷新
   * @return true表示需要刷新，false表示不需要
   */
  private boolean needRefresh() {
    return (Time.monotonicNow() - lastRefresh > 10000);
  }

  /**
   * 将最后刷新时间更新为当前时间
   */
  private void updateRefreshTimeToNow() {
    this.lastRefresh = Time.monotonicNow();
  }
}