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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer;

import java.io.Serializable;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.hadoop.yarn.server.nodemanager.DeletionService;

/**
 * 节点管理器本地缓存清理器，负责清理PUBLIC和PRIVATE类型的本地资源缓存，控制缓存总大小不超过阈值。
 * 采用LRU策略清理最近最少使用的未使用资源。
 */
class LocalCacheCleaner {

  private long currentSize;
  private final long targetSize;
  private final DeletionService delService;
  private final SortedMap<LocalizedResource, LocalResourcesTracker> resourceMap;

  /**
   * 构造本地缓存清理器，使用默认LRU排序策略。
   * @param delService 删除服务，用于提交删除任务
   * @param targetSize 缓存目标最大大小，超过该大小触发清理
   */
  LocalCacheCleaner(DeletionService delService, long targetSize) {
    this(delService, targetSize, new LRUComparator());
  }

  /**
   * 构造本地缓存清理器，使用自定义排序比较器。
   * @param delService 删除服务，用于提交删除任务
   * @param targetSize 缓存目标最大大小，超过该大小触发清理
   * @param cmp 资源排序比较器，决定清理顺序
   */
  LocalCacheCleaner(DeletionService delService, long targetSize,
      Comparator<? super LocalizedResource> cmp) {
    this(delService, targetSize,
        new TreeMap<LocalizedResource, LocalResourcesTracker>(cmp));
  }

  /**
   * 构造本地缓存清理器，使用预定义的资源有序集合。
   * @param delService 删除服务，用于提交删除任务
   * @param targetSize 缓存目标最大大小，超过该大小触发清理
   * @param resourceMap 预填充的待清理资源有序集合
   */
  LocalCacheCleaner(DeletionService delService, long targetSize,
      SortedMap<LocalizedResource, LocalResourcesTracker> resourceMap) {
    this.resourceMap = resourceMap;
    this.delService = delService;
    this.targetSize = targetSize;
  }

  /**
   * 将指定资源跟踪器中的所有可清理资源添加到清理候选列表。
   * @param newTracker 资源跟踪器，需要从中提取待清理资源
   */
  public void addResources(LocalResourcesTracker newTracker) {
    for (LocalizedResource resource : newTracker) {
      currentSize += resource.getSize();
      if (resource.getRefCount() > 0) {
        // 跳过仍被使用的资源，不加入清理候选
        continue;
      }
      resourceMap.put(resource, newTracker);
    }
  }

  /**
   * 按照排序顺序清理缓存，直到缓存总大小低于目标阈值。
   * @return 本次清理的统计结果
   */
  public LocalCacheCleanerStats cleanCache() {
    // 初始化清理统计，记录清理前缓存总大小
    LocalCacheCleanerStats stats = new LocalCacheCleanerStats(currentSize);
    // 遍历排序后的资源，持续清理直到缓存大小低于目标值
    for (Iterator<Map.Entry<LocalizedResource, LocalResourcesTracker>> i =
        resourceMap.entrySet().iterator();
        currentSize - stats.totalDelSize > targetSize && i.hasNext();) {
      Map.Entry<LocalizedResource, LocalResourcesTracker> rsrc = i.next();
      LocalizedResource resource = rsrc.getKey();
      LocalResourcesTracker tracker = rsrc.getValue();
      // 尝试从跟踪器删除该资源，成功则统计删除大小
      if (tracker.remove(resource, delService)) {
        stats.incDelSize(tracker.getUser(), resource.getSize());
      }
    }
    // 清空候选资源列表
    this.resourceMap.clear();
    return stats;
  }

  /**
   * 本地缓存清理结果统计类，记录本次清理删除的各类资源大小信息。
   */
  static class LocalCacheCleanerStats {
    private final Map<String, Long> userDelSizes = new TreeMap<String, Long>();
    private final long cacheSizeBeforeClean;
    private long totalDelSize;
    private long publicDelSize;
    private long privateDelSize;

    /**
     * 构造清理统计对象。
     * @param cacheSizeBeforeClean 清理前缓存总大小
     */
    LocalCacheCleanerStats(long cacheSizeBeforeClean) {
      this.cacheSizeBeforeClean = cacheSizeBeforeClean;
    }

    /**
     * 增加删除大小统计，区分公共/私有和用户维度。
     * @param user 资源对应用户，null表示公共资源
     * @param delSize 本次删除的资源大小
     */
    void incDelSize(String user, long delSize) {
      totalDelSize += delSize;
      if (user == null) {
        publicDelSize += delSize;
      } else {
        privateDelSize += delSize;
        Long userDel = userDelSizes.get(user);
        if (userDel != null) {
          userDel += delSize;
          userDelSizes.put(user, userDel);
        } else {
          userDelSizes.put(user, delSize);
        }
      }
    }

    Map<String, Long> getUserDelSizes() {
      return Collections.unmodifiableMap(userDelSizes);
    }

    long getCacheSizeBeforeClean() {
      return cacheSizeBeforeClean;
    }

    long getTotalDelSize() {
      return totalDelSize;
    }

    long getPublicDelSize() {
      return publicDelSize;
    }

    long getPrivateDelSize() {
      return privateDelSize;
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("Cache Size Before Clean: ").append(cacheSizeBeforeClean)
          .append(", ");
      sb.append("Total Deleted: ").append(totalDelSize).append(", ");
      sb.append("Public Deleted: ").append(publicDelSize).append(", ");
      sb.append("Private Deleted: ").append(privateDelSize);
      return sb.toString();
    }

    /**
     * 生成包含每个用户删除详情的详细字符串。
     * @return 详细统计信息字符串
     */
    public String toStringDetailed() {
      StringBuilder sb = new StringBuilder();
      sb.append(this.toString());
      sb.append(", Private Deleted Detail: {");
      for (Map.Entry<String, Long> e : userDelSizes.entrySet()) {
        sb.append(" ").append(e.getKey()).append(":").append(e.getValue());
      }
      sb.append(" }");
      return sb.toString();
    }
  }

  /**
   * LRU排序比较器，按照资源最后访问时间戳排序，越早访问的排在前面先清理。
   */
  private static class LRUComparator implements Comparator<LocalizedResource>,
      Serializable {

    private static final long serialVersionUID = 7034380228434701668L;

    public int compare(LocalizedResource r1, LocalizedResource r2) {
      long ret = r1.getTimestamp() - r2.getTimestamp();
      if (0 == ret) {
        // 时间戳相同时，用身份哈希码保证排序稳定唯一
        return System.identityHashCode(r1) - System.identityHashCode(r2);
      }
      return ret > 0 ? 1 : -1;
    }
  }
}