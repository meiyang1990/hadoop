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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.ConnectException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableSet;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.ipc.RetriableException;
import org.apache.hadoop.mapred.JobACLsManager;
import org.apache.hadoop.mapreduce.jobhistory.JobSummary;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.jobhistory.FileNameIndexUtils;
import org.apache.hadoop.mapreduce.v2.jobhistory.JHAdminConfig;
import org.apache.hadoop.mapreduce.v2.jobhistory.JobHistoryUtils;
import org.apache.hadoop.mapreduce.v2.jobhistory.JobIndexInfo;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.util.ShutdownThreadsHelper;
import org.apache.hadoop.util.concurrent.HadoopThreadPoolExecutor;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.SystemClock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 文件级注释：作业历史文件管理器，为MapReduce历史服务器提供线程安全的历史文件访问、索引维护和生命周期管理能力
 * 负责：历史文件从中间目录移动到完成目录、索引缓存维护、过期历史文件清理、作业查询等核心功能
 */
/**
 * This class provides a way to interact with history files in a thread safe
 * manor.
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public class HistoryFileManager extends AbstractService {
  private static final Logger LOG =
      LoggerFactory.getLogger(HistoryFileManager.class);
  private static final Logger SUMMARY_LOG =
      LoggerFactory.getLogger(JobSummary.class);

  /** 历史文件状态枚举，描述作业历史文件当前所处的位置和状态 */
  private enum HistoryInfoState {
    IN_INTERMEDIATE, IN_DONE, DELETED, MOVE_FAILED
  };
  
  private static String DONE_BEFORE_SERIAL_TAIL = JobHistoryUtils
      .doneSubdirsBeforeSerialTail();

  /**
   * 序列号索引类，维护序列号到时间戳目录的映射，加速按作业ID的查询
   * 采用LRU策略淘汰最旧的索引条目，控制内存占用
   */
  /**
   * Maps between a serial number (generated based on jobId) and the timestamp
   * component(s) to which it belongs. Facilitates jobId based searches. If a
   * jobId is not found in this list - it will not be found.
   */
  private static class SerialNumberIndex {
    private SortedMap<String, Set<String>> cache;
    private int maxSize;

    public SerialNumberIndex(int maxSize) {
      this.cache = new TreeMap<String, Set<String>>();
      this.maxSize = maxSize;
    }

    /**
     * 添加序列号到时间戳目录的映射关系
     * @param serialPart 序列号部分
     * @param timestampPart 时间戳目录部分
     */
    public synchronized void add(String serialPart, String timestampPart) {
      if (!cache.containsKey(serialPart)) {
        cache.put(serialPart, new HashSet<String>());
        if (cache.size() > maxSize) {
          // 缓存超过最大容量，移除最早的条目
          String key = cache.firstKey();
          LOG.error("Dropping " + key
              + " from the SerialNumberIndex. We will no "
              + "longer be able to see jobs that are in that serial index for "
              + cache.get(key));
          cache.remove(key);
        }
      }
      Set<String> datePartSet = cache.get(serialPart);
      datePartSet.add(timestampPart);
    }

    /**
     * 移除序列号和对应时间戳目录的映射
     * @param serialPart 序列号部分
     * @param timeStampPart 时间戳目录部分
     */
    public synchronized void remove(String serialPart, String timeStampPart) {
      if (cache.containsKey(serialPart)) {
        Set<String> set = cache.get(serialPart);
        set.remove(timeStampPart);
        if (set.isEmpty()) {
          cache.remove(serialPart);
        }
      }
    }

    /**
     * 根据序列号查询对应的时间戳目录集合
     * @param serialPart 序列号部分
     * @return 时间戳目录集合，不存在则返回null
     */
    public synchronized Set<String> get(String serialPart) {
      Set<String> found = cache.get(serialPart);
      if (found != null) {
        return new HashSet<String>(found);
      }
      return null;
    }
  }

  /**
   * 带O(1)大小统计的JobId到HistoryFileInfo映射封装
   * 用于作业列表缓存，优化size()操作性能，允许轻微的大小统计不一致（不影响业务逻辑）
   */
  /**
   * Wrapper around {@link ConcurrentSkipListMap} that maintains size along
   * side for O(1) size() implementation for use in JobListCache.
   *
   * Note: The size is not updated atomically with changes additions/removals.
   * This race can lead to size() returning an incorrect size at times.
   */
  static class JobIdHistoryFileInfoMap {
    private ConcurrentSkipListMap<JobId, HistoryFileInfo> cache;
    private AtomicInteger mapSize;

    JobIdHistoryFileInfoMap() {
      cache = new ConcurrentSkipListMap<JobId, HistoryFileInfo>();
      mapSize = new AtomicInteger();
    }

    /**
     * 不存在时插入，原子更新大小统计
     * @param key 作业ID
     * @param value 历史文件信息
     * @return 已存在则返回旧值，否则返回null
     */
    public HistoryFileInfo putIfAbsent(JobId key, HistoryFileInfo value) {
      HistoryFileInfo ret = cache.putIfAbsent(key, value);
      if (ret == null) {
        mapSize.incrementAndGet();
      }
      return ret;
    }

    /**
     * 移除指定作业ID的记录，原子更新大小统计
     * @param key 作业ID
     * @return 被移除的历史文件信息，不存在则返回null
     */
    public HistoryFileInfo remove(JobId key) {
      HistoryFileInfo ret = cache.remove(key);
      if (ret != null) {
        mapSize.decrementAndGet();
      }
      return ret;
    }

    /**
     * 返回缓存记录的大小，可能与实际大小略有偏差
     * @return 记录的缓存大小
     */
    /**
     * Returns the recorded size of the internal map. Note that this could be out
     * of sync with the actual size of the map
     * @return "recorded" size
     */
    public int size() {
      return mapSize.get();
    }

    /**
     * 根据作业ID获取历史文件信息
     * @param key 作业ID
     * @return 历史文件信息
     */
    public HistoryFileInfo get(JobId key) {
      return cache.get(key);
    }

    /**
     * 获取可导航的作业ID集合
     * @return 可导航的作业ID集合
     */
    public NavigableSet<JobId> navigableKeySet() {
      return cache.navigableKeySet();
    }

    /**
     * 获取所有历史文件信息集合
     * @return 所有历史文件信息集合
     */
    public Collection<HistoryFileInfo> values() {
      return cache.values();
    }
  }

  /**
   * 作业列表缓存类，维护已发现作业的缓存，基于容量和时间清理旧条目
   * 控制内存占用，避免缓存无限增长
   */
  static class JobListCache {
    private JobIdHistoryFileInfoMap cache;
    private int maxSize;
    private long maxAge;

    public JobListCache(int maxSize, long maxAge) {
      this.maxSize = maxSize;
      this.maxAge = maxAge;
      this.cache = new JobIdHistoryFileInfoMap();
    }

    /**
     * 如果不存在则添加作业到缓存，缓存超容时清理旧条目
     * @param fileInfo 历史文件信息
     * @return 已存在则返回旧值，否则返回null
     */
    public HistoryFileInfo addIfAbsent(HistoryFileInfo fileInfo) {
      JobId jobId = fileInfo.getJobId();
      if (LOG.isDebugEnabled()) {
        LOG.debug("Adding " + jobId + " to job list cache with "
            + fileInfo.getJobIndexInfo());
      }
      HistoryFileInfo old = cache.putIfAbsent(jobId, fileInfo);
      if (cache.size() > maxSize) {
        //There is a race here, where more then one thread could be trying to
        // remove entries.  This could result in too many entries being removed
        // from the cache.  This is considered OK as the size of the cache
        // should be rather large, and we would rather have performance over
        // keeping the cache size exactly at the maximum.
        Iterator<JobId> keys = cache.navigableKeySet().iterator();
        // 过期时间截止点，超过该时间的作业会被清理
        long cutoff = System.currentTimeMillis() - maxAge;

        // MAPREDUCE-6436: 减少移动中历史文件的日志输出，仅记录第一个和总数
        // 统计待清理但保留的移动中状态历史数量
        JobId firstInIntermediateKey = null;
        int inIntermediateCount = 0;
        JobId firstMoveFailedKey = null;
        int moveFailedCount = 0;

        while (cache.size() > maxSize && keys.hasNext()) {
          JobId key = keys.next();
          HistoryFileInfo firstValue = cache.get(key);
          if (firstValue != null) {
            if (firstValue.isMovePending()) {
              if (firstValue.didMoveFail() &&
                  firstValue.jobIndexInfo.getFinishTime() <= cutoff) {
                // 移动失败且已过期，清理缓存并删除文件
                cache.remove(key);
                // Now lets try to delete it
                try {
                  firstValue.delete();
                } catch (IOException e) {
                  LOG.error("Error while trying to delete history files" +
                      " that could not be moved to done.", e);
                }
              } else {
                // 移动未完成，暂不清理，统计数量用于日志
                if (firstValue.didMoveFail()) {
                  if (moveFailedCount == 0) {
                    firstMoveFailedKey = key;
                  }
                  moveFailedCount += 1;
                } else {
                  if (inIntermediateCount == 0) {
                    firstInIntermediateKey = key;
                  }
                  inIntermediateCount += 1;
                }
              }
            } else {
              // 已完成且缓存超容，清理最旧的条目
              cache.remove(key);
            }
          }
        }
        // 仅输出汇总日志，避免日志爆炸
        if (inIntermediateCount > 0) {
          LOG.warn("Waiting to remove IN_INTERMEDIATE state histories " +
                  "(e.g. " + firstInIntermediateKey + ") from JobListCache " +
                  "because it is not in done yet. Total count is " +
                  inIntermediateCount + ".");
        }
        if (moveFailedCount > 0) {
          LOG.warn("Waiting to remove MOVE_FAILED state histories " +
                  "(e.g. " + firstMoveFailedKey + ") from JobListCache " +
                  "because it is not in done yet. Total count is " +
                  moveFailedCount + ".");
        }
      }
      return old;
    }

    /**
     * 从缓存中删除指定作业
     * @param fileInfo 历史文件信息
     */
    public void delete(HistoryFileInfo fileInfo) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Removing from cache " + fileInfo);
      }
      cache.remove(fileInfo.getJobId());
    }

    /**
     * 获取所有缓存的历史文件信息
     * @return 所有历史文件信息集合
     */
    public Collection<HistoryFileInfo> values() {
      return new ArrayList<HistoryFileInfo>(cache.values());
    }

    /**
     * 根据作业ID获取缓存的历史文件信息
     * @param jobId 作业ID
     * @return 历史文件信息
     */
    public HistoryFileInfo get(JobId jobId) {
      return cache.get(jobId);
    }

    /**
     * 判断缓存是否已满
     * @return true 缓存大小达到或超过最大值，否则false
     */
    public boolean isFull() {
      return cache.size() >= maxSize;
    }

    /**
     * 获取当前缓存大小
     * @return 缓存记录数
     */
    public int size() {
      return cache.size();
    }
  }

  /**
   * 中间目录下用户目录包装类，用于目录修改时间检测和懒扫描
   * 仅当目录修改时间变化时才重新扫描，提升性能
   */
  /**
   * This class represents a user dir in the intermediate done directory.  This
   * is mostly for locking purposes. 
   */
  private class UserLogDir {
    long modTime = 0;
    private long scanTime = 0;

    /**
     * 如果目录有修改则执行扫描，发现新的历史文件
     * 适配云存储修改时间截断的特性，增加额外检测逻辑
     * @param fs 用户目录文件状态
     */
    public synchronized void scanIfNeeded(FileStatus fs) {
      long newModTime = fs.getModificationTime();
      // MAPREDUCE-6680: 云存储修改时间通常截断到秒级，需要额外处理
      // MAPREDUCE-7101: 部分云存储不更新目录修改时间，支持强制每次扫描
      boolean alwaysScan = conf.getBoolean(
          JHAdminConfig.MR_HISTORY_ALWAYS_SCAN_USER_DIR,
          JHAdminConfig.DEFAULT_MR_HISTORY_ALWAYS_SCAN_USER_DIR);
      if (alwaysScan || modTime != newModTime
          || (scanTime/1000) == (modTime/1000)
          || (scanTime/1000 + 1) == (modTime/1000)) {
        // 扫描前重置扫描时间
        scanTime = System.currentTimeMillis();
        Path p = fs.getPath();
        try {
          scanIntermediateDirectory(p);
          //If scanning fails, we will scan again.  We assume the failure is
          // temporary.
          modTime = newModTime;
        } catch (IOException e) {
          LOG.error("Error while trying to scan the directory " + p, e);
        }
      } else {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Scan not needed of " + fs.getPath());
        }
        // 重置扫描时间
        scanTime = System.currentTimeMillis();
      }
    }
  }

  /**
   * 单个作业历史文件信息封装，维护文件路径、索引信息和当前状态
   * 提供移动作业到完成目录、加载作业