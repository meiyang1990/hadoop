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
 * Unless required by applicable law or agreed to writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.yarn.server.resourcemanager.scheduler;

import java.util.HashMap;

/**
 * YARN资源调度器时间分桶统计工具，按时间差区间统计对象数量。
 * 维护键-时间对集合，查询时按预定义的时间区间分桶，返回每个区间内的对象数量。
 * 常用于统计调度队列中等待不同时长的任务/资源数量。
 */
class TimeBucketMetrics<OBJ> {

  // 存储对象及其对应的时间戳
  private final HashMap<OBJ, Long> map = new HashMap<OBJ, Long>();
  // 每个分桶的对象计数数组
  private final int[] counts;
  // 分桶时间阈值数组，按升序排列，用于划分不同时间区间
  private final long[] cuts;

  /**
   * 基于时间阈值构造分桶统计器，分桶数量为阈值数量+1。
   * @param cuts 分桶时间阈值数组，按升序排列
   */
  TimeBucketMetrics(long[] cuts) {
    this.cuts = cuts;
    counts = new int[cuts.length + 1];
  }

  /**
   * 添加对象到统计集合
   * @param key 待统计对象
   * @param time 对象记录的时间戳
   */
  synchronized void add(OBJ key, long time) {
    map.put(key, time);
  }

  /**
   * 从统计集合中移除对象
   * @param key 待移除对象
   */
  synchronized void remove(OBJ key) {
    map.remove(key);
  }

  /**
   * 根据时间差查找对应分桶索引
   * @param val 输入时间差
   * @return 对应分桶的索引
   */
  private int findBucket(long val) {
    for(int i=0; i < cuts.length; ++i) {
      if (val < cuts[i]) {
	return i;
      }
    }
    return cuts.length;
  }

  /**
   * 计算当前每个分桶的对象数量，基于当前时间计算时间差后统计。
   * 注意：每次调用都会复用同一个计数数组，返回结果会被下一次调用覆盖。
   * @param now 当前时间戳
   * @return 每个分桶的对象计数数组
   */
  synchronized int[] getBucketCounts(long now) {
    // 重置所有分桶计数为0
    for(int i=0; i < counts.length; ++i) {
      counts[i] = 0;
    }
    // 遍历所有对象，按时间差累加分桶计数
    for(Long time: map.values()) {
      counts[findBucket(now - time)] += 1;
    }
    return counts;
  }
}