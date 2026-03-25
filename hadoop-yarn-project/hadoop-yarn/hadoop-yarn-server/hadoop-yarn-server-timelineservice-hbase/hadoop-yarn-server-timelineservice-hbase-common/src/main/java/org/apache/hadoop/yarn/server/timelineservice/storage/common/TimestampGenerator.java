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

package org.apache.hadoop.yarn.server.timelineservice.storage.common;

import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.yarn.api.records.ApplicationId;

/**
 * YARN时间线服务HBase存储时间戳生成工具类，为HBase协处理器提供唯一时间戳生成能力。
 * 用于解决HBase同一列多个版本写入时的时间戳冲突问题。
 */
public class TimestampGenerator {

  /*
   * if this is changed, then reading cell timestamps written with older
   * multiplier value will not work
   */
  /** 时间戳放大系数，提供百万级的精度扩展，预留低位空间存储额外信息 */
  public static final long TS_MULTIPLIER = 1000000L;

  /** 记录上一次生成的唯一时间戳，用于CAS原子操作保证唯一性 */
  private final AtomicLong lastTimestamp = new AtomicLong();

  /**
   * 获取按精度放大后的当前系统时间戳。
   *
   * @return 放大后的当前时间戳
   */
  public long currentTime() {
    // We want to align cell timestamps with current time.
    // cell timestamps are not be less than
    // System.currentTimeMillis() * TS_MULTIPLIER.
    return System.currentTimeMillis() * TS_MULTIPLIER;
  }

  /**
   * 生成当前TimestampGenerator实例范围内唯一的时间戳。
   * 在HBase RegionObserver协处理器场景下，保证同一Region内的唯一性。
   * 通过CAS原子操作解决并发生成冲突，确保不会重复。
   *
   * @return 唯一时间戳
   */
  public long getUniqueTimestamp() {
    long lastTs;
    long nextTs;
    // CAS循环保证生成唯一递增时间戳
    do {
      lastTs = lastTimestamp.get();
      // 新时间戳不小于上一个+1，也不小于当前系统时间
      nextTs = Math.max(lastTs + 1, currentTime());
    } while (!lastTimestamp.compareAndSet(lastTs, nextTs));
    return nextTs;
  }

  /**
   * 生成带有应用ID后缀的补充时间戳，将应用ID低位嵌入时间戳低位，
   * 用于区分不同应用同时写入同一列的场景，避免冲突。
   *
   * @param incomingTS 原始时间戳
   * @param appId 应用ID字符串
   * @return 嵌入应用ID后缀的时间戳
   */
  public static long getSupplementedTimestamp(long incomingTS, String appId) {
    long suffix = getAppIdSuffix(appId);
    long outgoingTS = incomingTS * TS_MULTIPLIER + suffix;
    return outgoingTS;

  }

  /**
   * 从应用ID字符串提取低位后缀，用于嵌入时间戳。
   *
   * @param appIdStr 应用ID字符串
   * @return 应用ID对放大系数取模后的低位后缀
   */
  private static long getAppIdSuffix(String appIdStr) {
    if (appIdStr == null) {
      return 0L;
    }
    ApplicationId appId = ApplicationId.fromString(appIdStr);
    long id = appId.getId() % TS_MULTIPLIER;
    return id;
  }

  /**
   * 从补充时间戳中截去低位后缀，还原出原始毫秒级时间戳。
   *
   * @param incomingTS 补充后的时间戳
   * @return 截断低位后缀后的原始时间戳
   */
  public static long getTruncatedTimestamp(long incomingTS) {
    return incomingTS / TS_MULTIPLIER;
  }
}