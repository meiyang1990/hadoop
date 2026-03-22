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

package org.apache.hadoop.hdfs.server.datanode.fsdataset.impl;

import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.io.nativeio.NativeIO;

import org.apache.hadoop.classification.VisibleForTesting;

/**
 * 文件路径：hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/datanode/fsdataset/impl/CacheStats.java
 * <p>
 * HDFS DataNode内存缓存统计管理器，负责维护DataNode内存缓存容量和使用量的统计信息，
 * 支持线程安全的容量预留和释放，符合操作系统页面对齐要求，为NameNode分配缓存块提供容量依据。
 */
class CacheStats {

  /**
   * 已使用缓存字节统计器，采用乐观估算策略：只统计未完成的缓存预留，不扣除未完成的缓存释放，
   * 保证统计值始终高估实际用量，避免NameNode给当前节点分配超出容量的缓存任务。
   */
  private final UsedBytesCount usedBytesCount;

  /**
   * 缓存总容量，单位：字节，配置后保持不变。
   */
  private final long maxBytes;

  /**
   * 构造缓存统计对象，初始化已使用字节统计器。
   * @param maxBytes 缓存总容量，单位字节
   */
  CacheStats(long maxBytes) {
    this.usedBytesCount = new UsedBytesCount();
    this.maxBytes = maxBytes;
  }

  /**
   * 操作系统页面对齐工具类，根据当前系统页面大小对字节数进行向上/向下取整，
   * 满足操作系统内存缓存对齐要求。
   */
  @VisibleForTesting
  static class PageRounder {
    /** 当前操作系统的页面大小 */
    private final long osPageSize = NativeIO.POSIX.getCacheManipulator()
        .getOperatingSystemPageSize();

    /**
     * 将输入字节数向上取整到操作系统页面大小的整数倍。
     * @param count 原始字节数
     * @return 页面对齐后的字节数
     */
    public long roundUp(long count) {
      return (count + osPageSize - 1) & (~(osPageSize - 1));
    }

    /**
     * 将输入字节数向下取整到操作系统页面大小的整数倍。
     * @param count 原始字节数
     * @return 页面对齐后的字节数
     */
    public long roundDown(long count) {
      return count & (~(osPageSize - 1));
    }
  }

  /**
   * 线程安全的已使用缓存字节计数器，支持原子性的容量预留和释放，自动处理页面对齐。
   */
  private class UsedBytesCount {
    /** 原子存储当前已使用的字节数，支持并发无锁更新 */
    private final AtomicLong usedBytes = new AtomicLong(0);

    /** 页面对齐工具实例 */
    private CacheStats.PageRounder rounder = new PageRounder();

    /**
     * 尝试预留指定大小的缓存空间，自动向上页面对齐。
     * @param count 需要预留的原始字节数
     * @return 预留成功返回新的已使用字节数，超出容量返回-1
     */
    long reserve(long count) {
      count = rounder.roundUp(count);
      while (true) {
        long cur = usedBytes.get();
        long next = cur + count;
        if (next > getCacheCapacity()) {
          return -1;
        }
        if (usedBytes.compareAndSet(cur, next)) {
          return next;
        }
      }
    }

    /**
     * 释放指定大小的缓存空间，自动向上页面对齐。
     * @param count 需要释放的原始字节数
     * @return 释放后的新已使用字节数
     */
    long release(long count) {
      count = rounder.roundUp(count);
      return usedBytes.addAndGet(-count);
    }

    /**
     * 释放指定大小的缓存空间，自动向下页面对齐。
     * @param count 需要释放的原始字节数
     * @return 释放后的新已使用字节数
     */
    long releaseRoundDown(long count) {
      count = rounder.roundDown(count);
      return usedBytes.addAndGet(-count);
    }

    /**
     * 获取当前已使用的字节数。
     * @return 当前已使用字节数
     */
    long get() {
      return usedBytes.get();
    }
  }

  // Stats related methods for FSDatasetMBean

  /**
   * 获取当前近似已使用缓存容量，供JMX监控和NameNode查询使用。
   * @return 近似已使用缓存字节数
   */
  public long getCacheUsed() {
    return usedBytesCount.get();
  }

  /**
   * 获取缓存总容量，供JMX监控和NameNode查询使用。
   * @return 缓存总容量，单位字节
   */
  public long getCacheCapacity() {
    return maxBytes;
  }

  /**
   * 尝试预留指定大小的缓存空间。
   * @param count 需要预留的原始字节数
   * @return 预留成功返回新的已使用字节数，超出容量返回-1
   */
  long reserve(long count) {
    return usedBytesCount.reserve(count);
  }

  /**
   * 释放指定大小的缓存空间，向上页面对齐。
   * @param count 需要释放的原始字节数
   * @return 释放后的新已使用字节数
   */
  long release(long count) {
    return usedBytesCount.release(count);
  }

  /**
   * 释放指定大小的缓存空间，向下页面对齐。
   * @param count 需要释放的原始字节数
   * @return 释放后的新已使用字节数
   */
  long releaseRoundDown(long count) {
    return usedBytesCount.releaseRoundDown(count);
  }

  /**
   * 获取当前操作系统的页面大小，供外部模块页面对齐计算使用。
   * @return 操作系统页面大小，单位字节
   */
  long getPageSize() {
    return usedBytesCount.rounder.osPageSize;
  }

  /**
   * 将输入字节数向上对齐到操作系统页面大小。
   * @param count 原始字节数
   * @return 页面对齐后的字节数
   */
  long roundUpPageSize(long count) {
    return usedBytesCount.rounder.roundUp(count);
  }
}