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

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.MappingJsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.TypeFactory;
import com.fasterxml.jackson.module.jaxb.JaxbAnnotationIntrospector;
import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.service.ServiceOperations;
import org.apache.hadoop.ipc.CallerContext;
import org.apache.hadoop.util.ApplicationClassLoader;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.timeline.TimelineDomain;
import org.apache.hadoop.yarn.api.records.timeline.TimelineDomains;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntities;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntityGroupId;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEvents;
import org.apache.hadoop.yarn.api.records.timeline.TimelinePutResponse;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.ApplicationNotFoundException;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.timeline.TimelineDataManager.CheckAcl;
import org.apache.hadoop.yarn.server.timeline.security.TimelineACLsManager;
import org.apache.hadoop.yarn.util.Apps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.net.MalformedURLException;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 支持时间线服务器v1.5 API的插件式存储实现，使用文件系统按分组存储时间线实体
 * 按应用分组将时间线数据存储在HDFS上，已完成应用的日志会归档清理，支持分页查询
 */
public class EntityGroupFSTimelineStore extends CompositeService
    implements TimelineStore {

  static final String DOMAIN_LOG_PREFIX = "domainlog-";
  static final String SUMMARY_LOG_PREFIX = "summarylog-";
  static final String ENTITY_LOG_PREFIX = "entitylog-";

  static final String ATS_V15_SERVER_DFS_CALLER_CTXT = "yarn_ats_server_v1_5";

  private static final Logger LOG = LoggerFactory.getLogger(
      EntityGroupFSTimelineStore.class);
  private static final FsPermission ACTIVE_DIR_PERMISSION =
      new FsPermission((short) 01777);
  private static final FsPermission DONE_DIR_PERMISSION =
      new FsPermission((short) 0700);

  // Active dir: <activeRoot>/appId/attemptId/cacheId.log
  // Done dir: <doneRoot>/cluster_ts/hash1/hash2/appId/attemptId/cacheId.log
  private static final String APP_DONE_DIR_PREFIX_FORMAT =
      "%d" + Path.SEPARATOR     // cluster timestamp
          + "%04d" + Path.SEPARATOR // app num / 1,000,000
          + "%03d" + Path.SEPARATOR // (app num / 1000) % 1000
          + "%s" + Path.SEPARATOR; // full app id
  // 即使仍有活跃读请求，强制释放缓存项的阈值，增大此因子会增加内存占用
  private static final int CACHE_ITEM_OVERFLOW_FACTOR = 2;

  private YarnClient yarnClient;
  private TimelineStore summaryStore;
  private TimelineACLsManager aclManager;
  private TimelineDataManager summaryTdm;
  // 维护应用ID到该应用所有日志信息的映射
  private ConcurrentMap<ApplicationId, AppLogs> appIdLogMap =
      new ConcurrentHashMap<ApplicationId, AppLogs>();
  private ScheduledThreadPoolExecutor executor;
  // 服务停止标志，用于中断后台扫描清理线程
  private AtomicBoolean stopExecutors = new AtomicBoolean(false);
  private FileSystem fs;
  private ObjectMapper objMapper;
  private JsonFactory jsonFactory;
  // 活跃应用日志存储根目录
  private Path activeRootPath;
  // 已完成应用日志存储根目录
  private Path doneRootPath;
  // 已完成日志保留时长毫秒
  private long logRetainMillis;
  // 未知状态应用被视为已完成的超时时间毫秒
  private long unknownActiveMillis;
  // 实体组缓存最大容量
  private int appCacheMaxSize = 0;
  // 是否启用恢复功能
  private boolean recoveryEnabled;
  // 检查点文件路径，用于重启恢复
  private Path checkpointFile;
  // 恢复得到的日志处理进度信息：key=日志路径，value=(最后处理时间, 处理偏移量)
  private ConcurrentHashMap<String, Pair<Long, Long>> recoveredLogs =
      new ConcurrentHashMap<String, Pair<Long, Long>>();

  // 加载的实体组插件列表，用于将实体映射到对应实体组
  private List<TimelineEntityGroupPlugin> cacheIdPlugins;
  // 实体组缓存，按访问顺序排序支持LRU淘汰
  private Map<TimelineEntityGroupId, EntityCacheItem> cachedLogs;
  // 是否开启ACL权限检查
  private boolean aclsEnabled;

  @VisibleForTesting
  @InterfaceAudience.Private
  EntityGroupFSTimelineStoreMetrics metrics;

  /**
   * 构造函数
   */
  public EntityGroupFSTimelineStore() {
    super(EntityGroupFSTimelineStore.class.getSimpleName());
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    // 初始化指标统计
    metrics = EntityGroupFSTimelineStoreMetrics.create();
    // 创建汇总存储（默认Leveldb）
    summaryStore = createSummaryStore();
    addService(summaryStore);

    // 读取日志保留时长配置
    long logRetainSecs = conf.getLong(
        YarnConfiguration.TIMELINE_SERVICE_ENTITYGROUP_FS_STORE_RETAIN_SECONDS,
        YarnConfiguration
            .TIMELINE_SERVICE_ENTITYGROUP_FS_STORE_RETAIN_SECONDS_DEFAULT);
    logRetainMillis = logRetainSecs * 1000;
    LOG.info("Cleaner set to delete logs older than {} seconds", logRetainSecs);
    // 读取未知应用超时配置
    long unknownActiveSecs = conf.getLong(
        YarnConfiguration
            .TIMELINE_SERVICE_ENTITYGROUP_FS_STORE_UNKNOWN_ACTIVE_SECONDS,
        YarnConfiguration.
            TIMELINE_SERVICE_ENTITYGROUP_FS_STORE_UNKNOWN_ACTIVE_SECONDS_DEFAULT
    );
    unknownActiveMillis = unknownActiveSecs * 1000;
    LOG.info("Unknown apps will be treated as complete after {} seconds",
        unknownActiveSecs);
    // 读取缓存容量配置
    appCacheMaxSize = conf.getInt(
        YarnConfiguration.TIMELINE_SERVICE_ENTITYGROUP_FS_STORE_APP_CACHE_SIZE,
        YarnConfiguration
            .TIMELINE_SERVICE_ENTITYGROUP_FS_STORE_APP_CACHE_SIZE_DEFAULT);
    LOG.info("Application cache size is {}", appCacheMaxSize);
    // 创建LRU缓存，超出容量自动淘汰最久未使用项
    cachedLogs = Collections.synchronizedMap(
      new LinkedHashMap<TimelineEntityGroupId, EntityCacheItem>(
          appCacheMaxSize + 1, 0.75f, true) {
          @Override
          protected boolean removeEldestEntry(
              Map.Entry<TimelineEntityGroupId, EntityCacheItem> eldest) {
            // 缓存大小超过限制时淘汰
            if (super.size() > appCacheMaxSize) {
              TimelineEntityGroupId groupId = eldest.getKey();
              LOG.debug("Evicting {} due to space limitations", groupId);
              EntityCacheItem cacheItem = eldest.getValue();
              LOG.debug("Force release cache {}.", groupId);
              // 强制释放缓存资源
              cacheItem.forceRelease();
              // 如果应用已完成，从应用映射中移除
              if (cacheItem.getAppLogs().isDone()) {
                appIdLogMap.remove(groupId.getApplicationId());
              }
              metrics.incrCacheEvicts();
              return true;
            }
            return false;
          }
      });
    // 加载实体组插件
    cacheIdPlugins = loadPlugIns(conf);
    // 初始化YARN客户端用于查询应用状态
    yarnClient = createAndInitYarnClient(conf);
    // 如果yarnClient是服务，加入生命周期管理
    addIfService(yarnClient);
    // 读取活跃目录配置
    activeRootPath = new Path(conf.get(
        YarnConfiguration.TIMELINE_SERVICE_ENTITYGROUP_FS_STORE_ACTIVE_DIR,
        YarnConfiguration
            .TIMELINE_SERVICE_ENTITYGROUP_FS_STORE_ACTIVE_DIR_DEFAULT));
    // 读取已完成目录配置
    doneRootPath = new Path(conf.get(
        YarnConfiguration.TIMELINE_SERVICE_ENTITYGROUP_FS_STORE_DONE_DIR,
        YarnConfiguration
            .TIMELINE_SERVICE_ENTITYGROUP_FS_STORE_DONE_DIR_DEFAULT));
    // 获取文件系统实例
    fs = activeRootPath.getFileSystem(conf);
    checkpointFile = new Path(fs.getHomeDirectory(), "atscheckpoint");
    // 读取恢复功能配置
    recoveryEnabled = conf.getBoolean(
        YarnConfiguration.TIMELINE_SERVICE_ENTITYGROUP_FS_STORE_RECOVERY_ENABLED,
        YarnConfiguration.TIMELINE_SERVICE_ENTITYGROUP_FS_STORE_RECOVERY_ENABLED_DEFAULT);
    // 读取ACL开关配置
    aclsEnabled = conf.getBoolean(YarnConfiguration.YARN_ACL_ENABLE,
    YarnConfiguration.DEFAULT_YARN_ACL_ENABLE);
    // 设置DFS调用上下文标识
    CallerContext.setCurrent(
        new CallerContext.Builder(ATS_V15_SERVER_DFS_CALLER_CTXT).build());
    super.serviceInit(conf);
  }

  /**
   * 从配置加载实体组插件，支持自定义类加载器隔离插件
   */
  private List<TimelineEntityGroupPlugin> loadPlugIns(Configuration conf)
      throws RuntimeException {
    // 获取插件类名列表
    Collection<String> pluginNames = conf.getTrimmedStringCollection(
        YarnConfiguration.TIMELINE_SERVICE_ENTITY_GROUP_PLUGIN_CLASSES);
    // 获取插件额外类路径
    String pluginClasspath = conf.getTrimmed(
        YarnConfiguration.TIMELINE_SERVICE_ENTITY_GROUP_PLUGIN_CLASSPATH);
    // 获取系统类列表
    String[] systemClasses = conf.getTrimmedStrings(
        YarnConfiguration.TIMELINE_SERVICE_ENTITY_GROUP_PLUGIN_SYSTEM_CLASSES);

    List<TimelineEntityGroupPlugin> pluginList
        = new LinkedList<TimelineEntityGroupPlugin>();
    ClassLoader customClassLoader = null;
    // 如果配置了额外类路径，创建自定义类加载器
    if (pluginClasspath != null && pluginClasspath.length() > 0) {
      try {
        customClassLoader = createPluginClassLoader(pluginClasspath,
            systemClasses);
      } catch (IOException ioe) {
        LOG.warn("Error loading classloader", ioe);
      }
    }
    // 遍历加载每个插件
    for (final String name : pluginNames) {
      LOG.debug("Trying to load plugin class {}", name);
      TimelineEntityGroupPlugin cacheIdPlugin = null;

      try {
        if (customClassLoader != null) {
          // 使用自定义类加载器加载插件
          LOG.debug("Load plugin {} with classpath: {}", name, pluginClasspath);
          Class<?> clazz = Class.forName(name, true, customClassLoader);
          Class<? extends TimelineEntityGroupPlugin> sClass = clazz.asSubclass(
              TimelineEntityGroupPlugin.class);
          cacheIdPlugin = ReflectionUtils.newInstance(sClass, conf);
        } else {
          // 使用系统类加载器加载插件
          LOG.debug("Load plugin class with system classpath");
          Class<?> clazz = conf.getClassByName(name);
          cacheIdPlugin =
              (TimelineEntityGroupPlugin) ReflectionUtils.newInstance(
                  clazz, conf);
        }
      } catch (Exception e) {
        LOG.warn("Error loading plugin " + name, e);
        throw new RuntimeException("No class defined for " + name, e);
      }

      LOG.info("Load plugin class {}", cacheIdPlugin.getClass().getName());
      pluginList.add(cacheIdPlugin);
    }
    return pluginList;
  }

  /**
   * 创建汇总存储实例，默认使用LeveldbTimelineStore
   */
  private TimelineStore createSummaryStore() {
    return ReflectionUtils.newInstance(getConfig().getClass(
        YarnConfiguration.TIMELINE_SERVICE_ENTITYGROUP_FS_STORE_SUMMARY_STORE,
        LeveldbTimelineStore.class, TimelineStore.class), getConfig());
  }

  @Override
  protected void serviceStart() throws Exception {

    super.serviceStart();
    LOG.info("Starting {}", getName());
    summaryStore.start();

    Configuration conf = getConfig();
    // 初始化ACL管理器
    aclManager = new TimelineACLsManager(conf);
    aclManager.setTimelineStore(summaryStore);
    // 初始化汇总数据管理器
    summaryTdm = new TimelineDataManager(summaryStore, aclManager);
    summaryTdm.init(conf);
    addService(summaryTdm);
    // 启动所有子服务
    super.serviceStart();

    // 创建活跃目录并设置权限
    if (!fs.exists(activeRootPath)) {
      fs.mkdirs(activeRootPath);
      fs.setPermission(activeRootPath, ACTIVE_DIR_PERMISSION);
    }
    // 创建已完成目录并设置权限
    if (!fs.exists(doneRootPath)) {
      fs.mkdirs(doneRootPath);
      fs.setPermission(doneRootPath, DONE_DIR_PERMISSION);
    }

    // 从检查点恢复日志处理进度
    if (recoveryEnabled && fs.exists(checkpointFile)) {
      try (FSDataInputStream in = fs.open(checkpointFile)) {
        recoveredLogs.putAll(recoverLogFiles(in));
      } catch (IOException e) {
        LOG.warn("Failed to recover summarylog files from the checkpointfile", e);
      }
    }

    // 初始化JSON序列化工具
    objMapper = new ObjectMapper();
    objMapper.setAnnotationIntrospector(
        new JaxbAnnotationIntrospector(TypeFactory.defaultInstance()));
    jsonFactory = new MappingJsonFactory(objMapper);
    // 读取活跃目录扫描间隔配置
    final long scanIntervalSecs = conf.getLong(
        YarnConfiguration
            .TIMELINE_SERVICE_ENTITYGROUP_FS_STORE_SCAN_INTERVAL_SECONDS,
        YarnConfiguration
            .TIMELINE_SERVICE_ENTITYGROUP_FS_STORE_SCAN_INTERVAL_SECONDS_DEFAULT
    );
    // 读取日志清理间隔配置
    final long cleanerIntervalSecs = conf.getLong(
        YarnConfiguration
          .TIMELINE_SERVICE_ENTITYGROUP_FS_STORE_CLEANER_INTERVAL_SECONDS,
        YarnConfiguration
          .TIMELINE_SERVICE_ENTITYGROUP_FS_STORE_CLEANER_INTERVAL_SECONDS_DEFAULT
    );
    // 读取工作