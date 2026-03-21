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
import java.util.concurrent.locks.Lock;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.sharedcache.SharedCacheUtil;
import org.apache.hadoop.yarn.server.sharedcachemanager.metrics.CleanerMetrics;
import org.apache.hadoop.yarn.server.sharedcachemanager.store.SCMStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 共享缓存过期清理任务，负责清理共享缓存区域中过期条目和孤儿文件。
 * 同一时间点只允许运行一个清理任务。
 */
@Private
@Evolving
class CleanerTask implements Runnable {
  private static final String RENAMED_SUFFIX = "-renamed";
  private static final Logger LOG =
      LoggerFactory.getLogger(CleanerTask.class);

  private final String location;
  private final long sleepTime;
  private final int nestedLevel;
  private final Path root;
  private final FileSystem fs;
  private final SCMStore store;
  private final CleanerMetrics metrics;
  private final Lock cleanerTaskLock;

  /**
   * 根据配置创建清理任务工厂方法。
   *
   * @param conf Yarn配置对象
   * @param store 共享缓存存储对象
   * @param metrics 清理任务指标统计对象
   * @param cleanerTaskLock 保证清理任务串行执行的锁
   * @return 清理任务实例
   */
  public static CleanerTask create(Configuration conf, SCMStore store,
      CleanerMetrics metrics, Lock cleanerTaskLock) {
    try {
      // 获取共享缓存根目录路径
      String location =
          conf.get(YarnConfiguration.SHARED_CACHE_ROOT,
              YarnConfiguration.DEFAULT_SHARED_CACHE_ROOT);

      // 获取资源清理间隔休眠时间
      long sleepTime =
          conf.getLong(YarnConfiguration.SCM_CLEANER_RESOURCE_SLEEP_MS,
              YarnConfiguration.DEFAULT_SCM_CLEANER_RESOURCE_SLEEP_MS);
      // 获取缓存目录嵌套层级
      int nestedLevel = SharedCacheUtil.getCacheDepth(conf);
      // 获取共享缓存所在文件系统实例
      FileSystem fs = FileSystem.get(conf);

      return new CleanerTask(location, sleepTime, nestedLevel, fs, store,
          metrics, cleanerTaskLock);
    } catch (IOException e) {
      LOG.error("Unable to obtain the filesystem for the cleaner service", e);
      throw new ExceptionInInitializerError(e);
    }
  }

  /**
   * 创建清理任务构造方法。
   */
  CleanerTask(String location, long sleepTime, int nestedLevel, FileSystem fs,
      SCMStore store, CleanerMetrics metrics, Lock cleanerTaskLock) {
    this.location = location;
    this.sleepTime = sleepTime;
    this.nestedLevel = nestedLevel;
    this.root = new Path(location);
    this.fs = fs;
    this.store = store;
    this.metrics = metrics;
    this.cleanerTaskLock = cleanerTaskLock;
  }

  @Override
  public void run() {
    // 尝试获取任务锁，保证串行执行
    if (!this.cleanerTaskLock.tryLock()) {
      // 已有另一个清理任务正在运行
      LOG.warn("A cleaner task is already running. "
          + "This scheduled cleaner task will do nothing.");
      return;
    }

    try {
      // 检查共享缓存根目录是否存在
      if (!fs.exists(root)) {
        LOG.error("The shared cache root " + location + " was not found. "
            + "The cleaner task will do nothing.");
        return;
      }

      // 开始遍历清理共享缓存
      process();
    } catch (Throwable e) {
      LOG.error("Unexpected exception while initializing the cleaner task. "
          + "This task will do nothing,", e);
    } finally {
      // 无论执行结果如何，最终都释放锁
      this.cleanerTaskLock.unlock();
    }
  }

  /**
   * 遍历共享缓存区域，清理过期和孤儿文件。
   */
  void process() {
    // 上报清理任务开始指标
    metrics.reportCleaningStart();
    try {
      // 根据嵌套层级生成资源路径匹配模式
      String pattern = SharedCacheUtil.getCacheEntryGlobPattern(nestedLevel);
      // 匹配得到所有缓存资源目录
      FileStatus[] resources =
          fs.globStatus(new Path(root, pattern));
      // 计算资源总数
      int numResources = resources == null ? 0 : resources.length;
      LOG.info("Processing " + numResources + " resources in the shared cache");
      // 记录开始时间
      long beginMs = System.currentTimeMillis();
      if (resources != null) {
        // 遍历每个缓存资源
        for (FileStatus resource : resources) {
          // 检查线程中断信号，支持快速中止
          if (Thread.currentThread().isInterrupted()) {
            LOG.warn("The cleaner task was interrupted. Aborting.");
            break;
          }

          // 如果是目录则处理该资源，否则记录警告
          if (resource.isDirectory()) {
            processSingleResource(resource);
          } else {
            LOG.warn("Invalid file at path " + resource.getPath().toString()
                +
                " when a directory was expected");
          }
          // 如果配置了休眠时间，清理每个资源后休眠
          if (sleepTime > 0) {
            Thread.sleep(sleepTime);
          }
        }
      }
      // 计算清理耗时
      long endMs = System.currentTimeMillis();
      long durationMs = endMs - beginMs;
      LOG.info("Processed " + numResources + " resource(s) in " + durationMs +
          " ms.");
    } catch (IOException e1) {
      LOG.error("Unable to complete the cleaner task", e1);
    } catch (InterruptedException e2) {
      // 恢复中断状态
      Thread.currentThread().interrupt();
    }
  }

  /**
   * 获取共享缓存根目录路径。
   */
  Path getRootPath() {
    return root;
  }

  /**
   * 处理单个共享缓存资源目录。
   */
  void processSingleResource(FileStatus resource) {
    Path path = resource.getPath();
    // 初始化资源处理状态
    ResourceStatus resourceStatus = ResourceStatus.INIT;

    // 如果路径以重命名后缀结尾，说明这是之前标记为过期但未成功删除的目录，直接删除
    if (path.toString().endsWith(RENAMED_SUFFIX)) {
      LOG.info("Found a renamed directory that was left undeleted at " +
          path.toString() + ". Deleting.");
      try {
        if (fs.delete(path, true)) {
          resourceStatus = ResourceStatus.DELETED;
        }
      } catch (IOException e) {
        LOG.error("Error while processing a shared cache resource: " + path, e);
      }
    } else {
      // 目录名即为资源key，是资源的唯一标识
      String key = path.getName();

      try {
        // 清理存储中已失效的应用引用
        store.cleanResourceReferences(key);
      } catch (YarnException e) {
        LOG.error("Exception thrown while removing dead appIds.", e);
      }

      // 检查资源是否符合淘汰条件，需要清理
      if (store.isResourceEvictable(key, resource)) {
        try {
          /*
           * TODO See YARN-2663: There is a race condition between
           * store.removeResource(key) and
           * removeResourceFromCacheFileSystem(path) operations because they do
           * not happen atomically and resources can be uploaded with different
           * file names by the node managers.
           */
          // 从存储中移除该资源，会再次检查是否还有有效应用引用
          if (store.removeResource(key)) {
            // 从文件系统删除该资源目录
            boolean deleted = removeResourceFromCacheFileSystem(path);
            if (deleted) {
              resourceStatus = ResourceStatus.DELETED;
            } else {
              LOG.error("Failed to remove path from the file system."
                  + " Skipping this resource: " + path);
              resourceStatus = ResourceStatus.ERROR;
            }
          } else {
            // 资源仍存在有效引用，不删除
            resourceStatus = ResourceStatus.PROCESSED;
          }
        } catch (IOException e) {
          LOG.error(
              "Failed to remove path from the file system. Skipping this resource: "
                  + path, e);
          resourceStatus = ResourceStatus.ERROR;
        }
      } else {
        // 资源不符合淘汰条件，不删除
        resourceStatus = ResourceStatus.PROCESSED;
      }
    }

    // 根据处理结果更新指标
    switch (resourceStatus) {
    case DELETED:
      metrics.reportAFileDelete();
      break;
    case PROCESSED:
      metrics.reportAFileProcess();
      break;
    case ERROR:
      metrics.reportAFileError();
      break;
    default:
      LOG.error("Cleaner encountered an invalid status (" + resourceStatus
          + ") while processing resource: " + path.getName());
    }
  }

  /**
   * 从共享缓存文件系统删除单个资源，先重命名再删除保证删除操作原子性。
   * @param path 待删除资源路径
   * @return 删除成功返回true，否则返回false
   * @throws IOException 文件操作IO异常
   */
  private boolean removeResourceFromCacheFileSystem(Path path)
      throws IOException {
    // 生成重命名后的路径
    Path renamedPath = new Path(path.toString() + RENAMED_SUFFIX);
    if (fs.rename(path, renamedPath)) {
      // 重命名成功后，安全删除目录
      LOG.info("Deleting " + path.toString());
      return fs.delete(renamedPath, true);
    } else {
      // 重命名失败，保留原目录
      LOG.error("We were not able to rename the directory to "
          + renamedPath.toString() + ". We will leave it intact.");
    }
    return false;
  }

  /**
   * 缓存资源处理状态枚举，标识单个资源的处理结果。
   */
  private enum ResourceStatus {
    /** 初始状态 */
    INIT,
    /** 已完成处理，未删除 */
    PROCESSED,
    /** 已成功删除 */
    DELETED,
    /** 处理过程发生错误 */
    ERROR
  }
}