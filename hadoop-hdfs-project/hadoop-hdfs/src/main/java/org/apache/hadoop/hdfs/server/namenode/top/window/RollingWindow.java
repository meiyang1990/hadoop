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
package org.apache.hadoop.hdfs.server.namenode.top.window;

import java.util.Date;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.classification.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 滚动窗口实现，用于统计指定时间窗口内的事件累积量，为NameNode指标 Top 统计提供时间窗口支持。
 * 支持并发事件上报，通过分桶存储在准确性和空间占用之间做平衡，可获取最近一个窗口周期内的总事件数。
 * <p>
 *
 * 设计假设：
 * <p>
 *
 * (1) {@link #incAt}方法支持并发调用
 * <p>
 *
 * (2) 两次连续调用{@link #incAt}的时间参数可以是任意顺序
 * <p>
 *
 * (3) 事件上报的缓冲延迟不超过窗口长度，即两次连续调用时间满足 time1 &lt; time2 或者 time1 - time2 &lt; 窗口长度。
 * 该假设用于避免不必要的同步操作。
 * <p>
 *
 * 线程安全由{@link RollingWindow.Bucket}的原子变量和同步机制保证
 */
@InterfaceAudience.Private
public class RollingWindow {
  private static final Logger LOG = LoggerFactory.getLogger(RollingWindow.class);

  /**
   * 滚动窗口由多个桶组成，通过分桶在准确性和空间复杂度之间做权衡：
   * 分桶数量越少，滚动窗口占用内存越少，但计算窗口总值时可能产生更大误差。
   */
  Bucket[] buckets;
  final int windowLenMs;
  final int bucketSize;

  /**
   * 构造滚动窗口，初始化所有分桶
   * @param windowLenMs 滚动窗口覆盖的时间周期，必须大于最大缓冲延迟
   * @param numBuckets 窗口包含的分桶数量
   */
  RollingWindow(int windowLenMs, int numBuckets) {
    buckets = new Bucket[numBuckets];
    for (int i = 0; i < numBuckets; i++) {
      buckets[i] = new Bucket();
    }
    this.windowLenMs = windowLenMs;
    this.bucketSize = windowLenMs / numBuckets;
    if (this.bucketSize % bucketSize != 0) {
      throw new IllegalArgumentException(
          "The bucket size in the rolling window is not integer: windowLenMs= "
              + windowLenMs + " numBuckets= " + numBuckets);
    }
  }

  /**
   * 在指定时间点发生事件，将增量更新到滚动窗口对应分桶中
   * <p>
   *
   * @param time 事件发生的时间戳
   * @param delta 需要累加的增量值
   */
  public void incAt(long time, long delta) {
    // 计算当前事件所在的分桶索引
    int bi = computeBucketIndex(time);
    Bucket bucket = buckets[bi];
    // 如果当前分桶的上次更新时间已经超出滚动窗口范围，重置分桶
    if (bucket.isStaleNow(time)) {
      bucket.safeReset(time);
    }
    // 累加增量到当前分桶
    bucket.inc(delta);
  }

  /**
   * 根据事件时间计算对应分桶索引
   * @param time 事件时间戳
   * @return 对应分桶索引
   */
  private int computeBucketIndex(long time) {
    int positionOnWindow = (int) (time % windowLenMs);
    int bucketIndex = positionOnWindow * buckets.length / windowLenMs;
    return bucketIndex;
  }

  /**
   * 滚动窗口的单个分桶，存储分桶内的总增量和上次更新时间，保证并发操作线程安全
   */
  private class Bucket {
    private AtomicLong value = new AtomicLong(0);
    private AtomicLong updateTime = new AtomicLong(-1); // -1 表示从未更新

    /**
     * 检查当前分桶的状态是否已经过期（超出滚动窗口范围）
     *
     * @param time 当前时间
     * @return true 表示分桶状态已过期，需要重置
     */
    boolean isStaleNow(long time) {
      long utime = updateTime.get();
      return (utime == -1) || (time - utime >= windowLenMs);
    }

    /**
     * 安全重置分桶状态，处理并发更新和重置冲突
     *
     * @param time 当前时间
     */
    void safeReset(long time) {
      // 同一时间只允许一个线程重置分桶
      synchronized (this) {
        if (isStaleNow(time)) {
          // 先重置值，再更新时间，保证其他线程看到非过期时间时能拿到正确的更新值
          value.set(0);
          updateTime.set(time);
        }
        // 否则已经有并发线程完成重置，无需操作
      }
    }

    /**
     * 累加增量到当前分桶，调用前需要已经完成过期检查，不需要更新更新时间：只要更新时间还在窗口内，算法就能正常工作
     * @param delta 要累加的增量
     */
    void inc(long delta) {
      value.addAndGet(delta);
    }
  }

  /**
   * 获取指定时间点滚动窗口内的总事件量
   * <p>
   *
   * 如果时间落后于最新更新时间，新的更新仍会被计入总和
   *
   * @param time 当前时间戳
   * @return 过去窗口周期内发生的总事件量
   */
  public long getSum(long time) {
    long sum = 0;
    // 遍历所有分桶累加未过期分桶的值
    for (Bucket bucket : buckets) {
      boolean stale = bucket.isStaleNow(time);
      if (!stale) {
        sum += bucket.value.get();
      }
      // 调试日志输出每个分桶的状态信息
      if (LOG.isDebugEnabled()) {
        long bucketTime = bucket.updateTime.get();
        String timeStr = new Date(bucketTime).toString();
        LOG.debug("Sum: + " + sum + " Bucket: updateTime: " + timeStr + " ("
            + bucketTime + ") isStale " + stale + " at " + time);
      }
    }
    return sum;
  }

}