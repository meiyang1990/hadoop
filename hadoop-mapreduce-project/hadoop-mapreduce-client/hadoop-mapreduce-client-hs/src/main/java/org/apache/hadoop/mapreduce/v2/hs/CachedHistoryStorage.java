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
import java.util.Collection;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.hadoop.thirdparty.com.google.common.cache.Cache;
import org.apache.hadoop.thirdparty.com.google.common.cache.CacheBuilder;
import org.apache.hadoop.thirdparty.com.google.common.cache.CacheLoader;
import org.apache.hadoop.thirdparty.com.google.common.cache.LoadingCache;
import org.apache.hadoop.thirdparty.com.google.common.cache.Weigher;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.UncheckedExecutionException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.JobReport;
import org.apache.hadoop.mapreduce.v2.api.records.JobState;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.hs.webapp.dao.JobsInfo;
import org.apache.hadoop.mapreduce.v2.hs.HistoryFileManager.HistoryFileInfo;
import org.apache.hadoop.mapreduce.v2.hs.webapp.dao.JobInfo;
import org.apache.hadoop.mapreduce.v2.jobhistory.JHAdminConfig;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;

import org.apache.hadoop.classification.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 文件级注释：MapReduce历史服务器缓存式历史存储实现，管理解析后的作业历史文件内存缓存，
 * 加速历史作业查询，支持基于作业数量和任务总数量两种缓存淘汰策略
 */
/**
 * Manages an in memory cache of parsed Job History files.
 */
/**
 * 类级注释：缓存式历史作业存储实现，基于Guava缓存实现已解析作业对象的内存缓存，
 * 实现HistoryStorage接口，集成服务生命周期管理，为历史服务器提供快速作业查询能力
 */
public class CachedHistoryStorage extends AbstractService implements
    HistoryStorage {
  private static final Logger LOG =
      LoggerFactory.getLogger(CachedHistoryStorage.class);

  private LoadingCache<JobId, Job> loadedJobCache = null;
  private int loadedJobCacheSize;
  private int loadedTasksCacheSize;
  private boolean useLoadedTasksCache;

  private HistoryFileManager hsManager;

  @Override
  /**
   * 注入历史文件管理器，用于后续加载作业文件
   */
  public void setHistoryFileManager(HistoryFileManager hsManager) {
    this.hsManager = hsManager;
  }

  @Override
  /**
   * 服务初始化方法，创建作业缓存，读取配置参数
   */
  public void serviceInit(Configuration conf) throws Exception {
    super.serviceInit(conf);
    LOG.info("CachedHistoryStorage Init");

    createLoadedJobCache(conf);
  }

  @SuppressWarnings("serial")
  /**
   * 根据配置创建加载作业缓存，支持按作业个数或任务总重量两种缓存策略
   */
  private void createLoadedJobCache(Configuration conf) {
    // 读取作业缓存大小配置
    loadedJobCacheSize = conf.getInt(
        JHAdminConfig.MR_HISTORY_LOADED_JOB_CACHE_SIZE,
        JHAdminConfig.DEFAULT_MR_HISTORY_LOADED_JOB_CACHE_SIZE);

    // 检查任务缓存配置合法性
    useLoadedTasksCache = false;
    try {
      String taskSizeString = conf
          .get(JHAdminConfig.MR_HISTORY_LOADED_TASKS_CACHE_SIZE);
      if (taskSizeString != null) {
        loadedTasksCacheSize = Math.max(Integer.parseInt(taskSizeString), 1);
        useLoadedTasksCache = true;
      }
    } catch (NumberFormatException nfe) {
      LOG.error("The property " +
          JHAdminConfig.MR_HISTORY_LOADED_TASKS_CACHE_SIZE +
          " is not an integer value.  Please set it to a positive" +
          " integer value.");
    }

    CacheLoader<JobId, Job> loader;
    // 缓存加载器，根据JobId加载作业对象
    loader = new CacheLoader<JobId, Job>() {
      @Override
      public Job load(JobId key) throws Exception {
        return loadJob(key);
      }
    };

    if (!useLoadedTasksCache) {
      // 不启用基于任务数的缓存，按作业个数限制缓存大小
      loadedJobCache = CacheBuilder.newBuilder()
          .maximumSize(loadedJobCacheSize)
          .initialCapacity(loadedJobCacheSize)
          .concurrencyLevel(1)
          .build(loader);
    } else {
      Weigher<JobId, Job> weightByTasks;
      // 按任务总数量计算作业权重，实现基于总任务数的缓存淘汰
      weightByTasks = new Weigher<JobId, Job>() {
        /**
         * Method for calculating Job weight by total task count.  If
         * the total task count is greater than the size of the tasks
         * cache, then cap it at the cache size.  This allows the cache
         * to always hold one large job.
         * @param key JobId object
         * @param value Job object
         * @return Weight of the job as calculated by total task count
         */
        @Override
        public int weigh(JobId key, Job value) {
          int taskCount = Math.min(loadedTasksCacheSize,
              value.getTotalMaps() + value.getTotalReduces());
          return taskCount;
        }
      };
      // Keep concurrencyLevel at 1.  Otherwise, two problems:
      // 1) The largest job that can be initially loaded is
      //    cache size / 4.
      // 2) Unit tests are not deterministic.
      // 启用基于任务总重量的缓存，总权重超过限制后淘汰旧作业
      loadedJobCache = CacheBuilder.newBuilder()
          .maximumWeight(loadedTasksCacheSize)
          .weigher(weightByTasks)
          .concurrencyLevel(1)
          .build(loader);
    }
  }
  
  /**
   * 刷新已加载作业缓存，重新读取配置创建新缓存，仅在服务已启动时生效
   */
  public void refreshLoadedJobCache() {
    if (getServiceState() == STATE.STARTED) {
      setConfig(createConf());
      createLoadedJobCache(getConfig());
    } else {
      LOG.warn("Failed to execute refreshLoadedJobCache: CachedHistoryStorage is not started");
    }
  }
  
  @VisibleForTesting
  /**
   * 创建配置对象，供测试使用
   */
  Configuration createConf() {
    return new Configuration();
  }
  
  /**
   * 构造方法，调用父类服务构造
   */
  public CachedHistoryStorage() {
    super(CachedHistoryStorage.class.getName());
  }

  /**
   * 内部异常类，封装作业历史文件加载异常
   */
  private static class HSFileRuntimeException extends RuntimeException {
    public HSFileRuntimeException(String message) {
      super(message);
    }
  }
  
  /**
   * 根据作业ID从历史文件加载作业对象
   */
  private Job loadJob(JobId jobId) throws RuntimeException, IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Looking for Job " + jobId);
    }
    HistoryFileInfo fileInfo;

    // 从历史文件管理器获取作业文件信息
    fileInfo = hsManager.getFileInfo(jobId);

    if (fileInfo == null) {
      throw new HSFileRuntimeException("Unable to find job " + jobId);
    }

    // 等待历史文件移动完成（避免读取未完成写入的文件）
    fileInfo.waitUntilMoved();

    if (fileInfo.isDeleted()) {
      throw new HSFileRuntimeException("Cannot load deleted job " + jobId);
    } else {
      // 从历史文件加载解析作业对象
      return fileInfo.loadJob();
    }
  }

  @VisibleForTesting
  /**
   * 获取已加载作业缓存对象，供测试使用
   */
  Cache<JobId, Job> getLoadedJobCache() {
    return loadedJobCache;
  }
  
  @Override
  /**
   * 获取完整作业对象，优先从缓存获取，缓存未命中则加载
   */
  public Job getFullJob(JobId jobId) {
    Job retVal = null;
    try {
      retVal = loadedJobCache.getUnchecked(jobId);
    } catch (UncheckedExecutionException e) {
      if (e.getCause() instanceof HSFileRuntimeException) {
        LOG.error(e.getCause().getMessage());
        return null;
      } else {
        throw new YarnRuntimeException(e.getCause());
      }
    }
    return retVal;
  }

  @Override
  /**
   * 获取所有不完整作业信息（仅包含索引信息，不加载完整任务数据）
   */
  public Map<JobId, Job> getAllPartialJobs() {
    LOG.debug("Called getAllPartialJobs()");
    SortedMap<JobId, Job> result = new TreeMap<JobId, Job>();
    try {
      // 遍历所有历史作业文件
      for (HistoryFileInfo mi : hsManager.getAllFileInfo()) {
        if (mi != null) {
          JobId id = mi.getJobId();
          mi.waitUntilMoved();
          // 封装为PartialJob，仅保留基本索引信息
          result.put(id, new PartialJob(mi.getJobIndexInfo(), id));
        }
      }
    } catch (IOException e) {
      LOG.warn("Error trying to scan for all FileInfos", e);
      throw new YarnRuntimeException(e);
    }
    return result;
  }

  @Override
  /**
   * 根据分页和过滤条件查询符合条件的部分作业信息
   */
  public JobsInfo getPartialJobs(Long offset, Long count, String user,
      String queue, Long sBegin, Long sEnd, Long fBegin, Long fEnd,
      JobState jobState) {
    return getPartialJobs(getAllPartialJobs().values(), offset, count, user,
        queue, sBegin, sEnd, fBegin, fEnd, jobState);
  }

  /**
   * 静态工具方法，对所有作业集合按条件过滤分页，返回查询结果
   */
  public static JobsInfo getPartialJobs(Collection<Job> jobs, Long offset,
      Long count, String user, String queue, Long sBegin, Long sEnd,
      Long fBegin, Long fEnd, JobState jobState) {
    JobsInfo allJobs = new JobsInfo();

    // 初始化过滤参数默认值
    if (sBegin == null || sBegin < 0)
      sBegin = 0l;
    if (sEnd == null)
      sEnd = Long.MAX_VALUE;
    if (fBegin == null || fBegin < 0)
      fBegin = 0l;
    if (fEnd == null)
      fEnd = Long.MAX_VALUE;
    if (offset == null || offset < 0)
      offset = 0l;
    if (count == null)
      count = Long.MAX_VALUE;

    // 偏移量超出总数，直接返回空结果
    if (offset > jobs.size()) {
      return allJobs;
    }

    long at = 0;
    long end = offset + count - 1;
    if (end < 0) { // 处理溢出情况
      end = Long.MAX_VALUE;
    }

    // 遍历所有作业依次过滤
    for (Job job : jobs) {
      if (at > end) {
        break;
      }

      // 队列过滤
      if (queue != null && !queue.isEmpty()) {
        if (!job.getQueueName().equals(queue)) {
          continue;
        }
      }

      // 提交用户过滤
      if (user != null && !user.isEmpty()) {
        if (!job.getUserName().equals(user)) {
          continue;
        }
      }

      JobReport report = job.getReport();

      // 提交开始时间过滤
      if (report.getStartTime() < sBegin || report.getStartTime() > sEnd) {
        continue;
      }
      // 完成时间过滤
      if (report.getFinishTime() < fBegin || report.getFinishTime() > fEnd) {
        continue;
      }
      // 作业状态过滤
      if (jobState != null && jobState != report.getJobState()) {
        continue;
      }

      at++;
      // 跳过偏移量之前的作业
      if ((at - 1) < offset) {
        continue;
      }

      // 构造作业信息对象添加到结果
      JobInfo jobInfo = new JobInfo(job);

      allJobs.add(jobInfo);
    }
    return allJobs;
  }

  @VisibleForTesting
  /**
   * 获取是否启用基于任务数缓存的标志，供测试使用
   */
  public boolean getUseLoadedTasksCache() {
    return useLoadedTasksCache;
  }

  @VisibleForTesting
  /**
   * 获取任务缓存总大小，供测试使用
   */
  public int getLoadedTasksCacheSize() {
    return loadedTasksCacheSize;
  }
}