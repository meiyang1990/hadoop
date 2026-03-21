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

package org.apache.hadoop.yarn.server.sharedcachemanager.store;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.StringInterner;
import org.apache.hadoop.util.concurrent.HadoopExecutors;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.sharedcache.SharedCacheUtil;
import org.apache.hadoop.yarn.server.sharedcachemanager.AppChecker;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 线程安全的共享缓存管理器内存存储实现。线程安全通过两点保障：
 * (1) 使用ConcurrentHashMap存储资源及其引用，支持并发访问；
 * (2) 基于键级别锁保证同一资源操作的互斥性。<br>
 * <br>
 * 为实现安全的键级锁，使用Hadoop的StringInterner对键进行弱驻留，避免了内置String驻留的缺陷：
 * 驻留后的字符串是弱引用，使用完成后可被垃圾回收，同时降低了代码其他部分误将这些键用作锁的风险。<br>
 * <br>
 * 内存存储中的资源基于过期时间策略进行驱逐：如果一个资源在指定周期内未被引用，则被标记为过期，满足驱逐条件。
 */
@Private
@Evolving
public class InMemorySCMStore extends SCMStore {
  private static final Logger LOG =
      LoggerFactory.getLogger(InMemorySCMStore.class);

  // 存储所有缓存资源，key为资源校验和，value为缓存资源对象
  private final Map<String, SharedCacheResource> cachedResources =
      new ConcurrentHashMap<String, SharedCacheResource>();
  // 存储服务启动时正在运行的应用列表，用于初始阶段的资源可驱逐性判断
  private Collection<ApplicationId> initialApps =
      new ArrayList<ApplicationId>();
  // 初始应用列表的同步锁
  private final Object initialAppsLock = new Object();
  // 存储服务启动时间，用于判断资源过期
  private long startTime;
  // 资源过期判断的时间阈值（分钟）
  private int stalenessMinutes;
  // 定时检查应用状态的调度器
  private ScheduledExecutorService scheduler;
  // 首次检查应用状态的初始延迟（分钟）
  private int initialDelayMin;
  // 应用状态检查的周期（分钟）
  private int checkPeriodMin;

  public InMemorySCMStore() {
    super(InMemorySCMStore.class.getName());
  }

  @VisibleForTesting
  public InMemorySCMStore(AppChecker appChecker) {
    super(InMemorySCMStore.class.getName(), appChecker);
  }

  // 对键进行弱驻留，用于键级别锁
  private String intern(String key) {
    return StringInterner.weakIntern(key);
  }

  /**
   * 内存存储从HDFS中已存在的共享缓存条目完成自举初始化。
   */
  @Override
  protected void serviceInit(Configuration conf) throws Exception {

    this.startTime = System.currentTimeMillis();
    this.initialDelayMin = getInitialDelay(conf);
    this.checkPeriodMin = getCheckPeriod(conf);
    this.stalenessMinutes = getStalenessPeriod(conf);

    bootstrap(conf);

    ThreadFactory tf =
        new ThreadFactoryBuilder().setNameFormat("InMemorySCMStore")
            .build();
    // 创建单线程定时调度器
    scheduler = HadoopExecutors.newSingleThreadScheduledExecutor(tf);

    super.serviceInit(conf);
  }

  @Override
  protected void serviceStart() throws Exception {
    // 先启动组合服务
    super.serviceStart();

    // 获取初始运行应用列表
    LOG.info("Getting the active app list to initialize the in-memory scm store");
    synchronized (initialAppsLock) {
      initialApps = appChecker.getActiveApplications();
    }
    LOG.info(initialApps.size() + " apps recorded as active at this time");

    // 创建应用检查任务并启动定时调度
    Runnable task = new AppCheckTask(appChecker);
    scheduler.scheduleAtFixedRate(task, initialDelayMin, checkPeriodMin,
        TimeUnit.MINUTES);
    LOG.info("Scheduled the in-memory scm store app check task to run every "
        + checkPeriodMin + " minutes.");
  }

  @Override
  protected void serviceStop() throws Exception {
    LOG.info("Stopping the " + InMemorySCMStore.class.getSimpleName()
        + " service.");
    if (scheduler != null) {
      LOG.info("Shutting down the background thread.");
      // 立即关闭调度器
      scheduler.shutdownNow();
      try {
        // 等待任务终止，最多10秒
        if (!scheduler.awaitTermination(10, TimeUnit.SECONDS)) {
          LOG.warn("Gave up waiting for the app check task to shutdown.");
        }
      } catch (InterruptedException e) {
        LOG.warn(
            "The InMemorySCMStore was interrupted while shutting down the "
                + "app check task.", e);
      }
      LOG.info("The background thread stopped.");
    }
    super.serviceStop();
  }

  // 从HDFS文件系统加载已有缓存资源，完成内存存储初始化
  private void bootstrap(Configuration conf) throws IOException {
    // 从文件系统获取所有初始缓存资源
    Map<String, String> initialCachedResources =
        getInitialCachedResources(FileSystem.get(conf), conf);
    LOG.info("Bootstrapping from " + initialCachedResources.size()
        + " cache resources located in the file system");
    Iterator<Map.Entry<String, String>> it =
        initialCachedResources.entrySet().iterator();
    while (it.hasNext()) {
      Map.Entry<String, String> e = it.next();
      String key = intern(e.getKey());
      String fileName = e.getValue();
      SharedCacheResource resource = new SharedCacheResource(fileName);
      // 初始化阶段无需加锁，serviceInit过程单线程执行
      cachedResources.put(key, resource);
      // 清理临时map，减少内存占用
      it.remove();
    }
    LOG.info("Bootstrapping complete");
  }

  @VisibleForTesting
  Map<String, String> getInitialCachedResources(FileSystem fs,
      Configuration conf) throws IOException {
    // 获取共享缓存根目录
    String location =
        conf.get(YarnConfiguration.SHARED_CACHE_ROOT,
            YarnConfiguration.DEFAULT_SHARED_CACHE_ROOT);
    Path root = new Path(location);
    try {
      // 检查根目录是否存在
      fs.getFileStatus(root);
    } catch (FileNotFoundException e) {
      String message =
          "The shared cache root directory " + location + " was not found";
      LOG.error(message);
      throw (IOException)new FileNotFoundException(message)
          .initCause(e);
    }

    // 获取缓存目录嵌套层级
    int nestedLevel = SharedCacheUtil.getCacheDepth(conf);
    // 遍历目录结构格式为 level/../<checksum>/file，构造glob匹配模式
    String pattern = SharedCacheUtil.getCacheEntryGlobPattern(nestedLevel+1);

    LOG.info("Querying for all individual cached resource files");
    // 匹配所有缓存资源文件
    FileStatus[] entries = fs.globStatus(new Path(root, pattern));
    int numEntries = entries == null ? 0 : entries.length;
    LOG.info("Found " + numEntries + " files: processing for one resource per "
        + "key");

    Map<String, String> initialCachedEntries = new HashMap<String, String>();
    if (entries != null) {
      for (FileStatus entry : entries) {
        Path file = entry.getPath();
        String fileName = file.getName();
        if (entry.isFile()) {
          // 父目录名称即为资源校验和（key）
          Path parent = file.getParent();
          if (parent != null) {
            String key = parent.getName();
            // 保证每个key只对应一个文件，冲突时保留第一个
            if (initialCachedEntries.containsKey(key)) {
              LOG.warn("Key " + key + " is already mapped to file "
                  + initialCachedEntries.get(key) + "; file " + fileName
                  + " will not be added");
            } else {
              initialCachedEntries.put(key, fileName);
            }
          }
        }
      }
    }
    LOG.info("A total of " + initialCachedEntries.size()
        + " files are now mapped");
    return initialCachedEntries;
  }

  /**
   * 将资源添加到缓存存储中。如果该key已存在资源，返回已有文件名。
   * 返回值仅代表查询时刻的存储状态，方法返回后条目可能被变更或移除，调用者需要处理该情况。
   * 
   * @return 新插入或已有资源的文件名
   */
  @Override
  public String addResource(String key, String fileName) {
    String interned = intern(key);
    synchronized (interned) {
      SharedCacheResource resource = cachedResources.get(interned);
      if (resource == null) {
        resource = new SharedCacheResource(fileName);
        cachedResources.put(interned, resource);
      }
      return resource.getFileName();
    }
  }

  /**
   * 为对应key的缓存资源添加资源引用，并更新访问时间。
   * 如果返回非null值，调用者可以安全假定：至少在引用关联的应用终止前，该资源不会被移除。
   * 
   * @return 资源文件名，如果资源不存在返回null
   */
  @Override
  public String addResourceReference(String key,
      SharedCacheResourceReference ref) {
    String interned = intern(key);
    synchronized (interned) {
      SharedCacheResource resource = cachedResources.get(interned);
      if (resource == null) { // 资源不存在
        return null;
      }
      resource.addReference(ref);
      resource.updateAccessTime();
      return resource.getFileName();
    }
  }

  /**
   * 获取当前缓存条目下注册的所有资源引用列表。如果为空返回空集合。
   * 返回集合不可修改，是查询时刻的快照，返回后状态可能变更，调用者需要处理引用失效情况。
   * 
   * @return 关联的资源引用集合，无引用则返回空集合
   */
  @Override
  public Collection<SharedCacheResourceReference> getResourceReferences(String key) {
    String interned = intern(key);
    synchronized (interned) {
      SharedCacheResource resource = cachedResources.get(interned);
      if (resource == null) {
        return Collections.emptySet();
      }
      Set<SharedCacheResourceReference> refs =
          new HashSet<SharedCacheResourceReference>(
              resource.getResourceReferences());
      return Collections.unmodifiableSet(refs);
    }
  }

  /**
   * 从资源中移除指定资源引用。如果资源不存在则不做任何操作。
   */
  @Override
  public boolean removeResourceReference(String key, SharedCacheResourceReference ref,
      boolean updateAccessTime) {
    String interned = intern(key);
    synchronized (interned) {
      boolean removed = false;
      SharedCacheResource resource = cachedResources.get(interned);
      if (resource != null) {
        Set<SharedCacheResourceReference> resourceRefs =
            resource.getResourceReferences();
        removed = resourceRefs.remove(ref);
        if (updateAccessTime) {
          resource.updateAccessTime();
        }
      }
      return removed;
    }
  }

  /**
   * 从资源中批量移除指定资源引用集合。如果资源不存在则不做任何操作。
   */
  @Override
  public void removeResourceReferences(String key,
      Collection<SharedCacheResourceReference> refs, boolean updateAccessTime) {
    String interned = intern(key);
    synchronized (interned) {
      SharedCacheResource resource = cachedResources.get(interned);
      if (resource != null) {
        Set<SharedCacheResourceReference> resourceRefs =
            resource.getResourceReferences();
        resourceRefs.removeAll(refs);
        if (updateAccessTime) {
          resource.updateAccessTime();
        }
      }
    }
  }

  /**
   * 为方法提供原子性保证。
   */
  @Override
  public void cleanResourceReferences(String key) throws YarnException {
    String interned = intern(key);
    synchronized (interned) {
      super.cleanResourceReferences(key);
    }
  }

  /**
   * 从存储中移除指定资源。如果资源不存在或成功移除，返回true；如果资源存在但引用列表非空无法移除，返回false。
   */
  @Override
  public boolean removeResource(String key) {
    String interned = intern(key);
    synchronized (interned) {
      SharedCacheResource resource = cachedResources.get(interned);
      if (resource == null) {
        return true;
      }

      if (!resource.getResourceReferences().isEmpty()) {
        return false;
      }
      // 无引用，移除资源
      cachedResources.remove(interned);
      return true;
    }
  }

  /**
   * 获取资源的最后访问时间，返回值为查询时刻的快照，之后可能会被更新。
   * 
   * @return 如果找到资源返回访问时间；否则返回-1
   */
  @VisibleForTesting
  long getAccessTime(String key) {
    String interned = intern(key);
    synchronized (interned) {
      SharedCacheResource resource = cachedResources.get(interned);
      return resource == null ? -1 : resource.getAccessTime();
    }
  }

  @Override
  public boolean isResourceEvictable(String key, FileStatus file) {
    synchronized (initialAppsLock) {
      // 初始应用列表未空，说明启动时所有应用都已完成，允许驱逐
      if (initialApps.size() > 0) {
        return false;
      }
    }

    // 计算过期时间阈值
    long staleTime =
        System.currentTimeMillis()
            - TimeUnit.MINUTES.toMillis(this.stalenessMinutes);
    long accessTime = getAccessTime(key);
    if (accessTime == -1) {
      // 内存中没有访问时间，使用文件修改时间判断
      long modTime = file.getModificationTime();
      // 如果修改时间早于存储启动时间，使用启动时间作为最后确定使用时间
      long lastUse = modTime < this.startTime ? this.startTime : modTime;
      return lastUse < staleTime;
    } else {
      // 使用内存中的访问时间判断
      return accessTime < staleTime;
    }
  }

  // 从配置读取资源过期周期，校验合法性
  private static int getStalenessPeriod(Configuration conf) {
    int stalenessMinutes =
        conf.getInt(YarnConfiguration.IN_MEMORY_STALENESS_PERIOD_MINS,
            YarnConfiguration.DEFAULT_IN_MEMORY_STALENESS_PERIOD_MINS);
    // 非正值非法，抛出异常
    if (stalenessMinutes <= 0) {
      throw new HadoopIllegalArgumentException("Non-positive staleness value: "
          + stalenessMinutes
          + ". The staleness value must be greater than zero.");
    }
    return stalenessMinutes;
  }

  // 从配置读取首次检查延迟，校验合法性
  private static int getInitialDelay(Configuration conf) {
    int initialMinutes =
        conf.getInt(YarnConfiguration.IN_MEMORY_INITIAL_DELAY_MINS,
            YarnConfiguration.DEFAULT_IN_MEMORY_INITIAL_DELAY_MINS);
    // 非正值非法，抛出异常
    if (initialMinutes <= 0) {
      throw new HadoopIllegalArgumentException(
          "Non-positive initial delay value: " + initialMinutes
              + ". The initial delay value must be greater than zero.");
    }
    return initialMinutes;
  }

  // 从配置读取检查周期，校验合法性
  private static int getCheckPeriod(Configuration conf) {
    int checkMinutes =
        conf.getInt(Y