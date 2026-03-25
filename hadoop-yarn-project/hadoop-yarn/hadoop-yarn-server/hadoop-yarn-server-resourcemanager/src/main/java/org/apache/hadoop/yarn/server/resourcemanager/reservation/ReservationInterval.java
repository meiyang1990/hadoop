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

package org.apache.hadoop.yarn.server.resourcemanager.reservation;

/**
 * 表示YARN资源预留的时间区间，存储并管理预留的起止时间，支持时间重叠检查和区间比较
 */
public class ReservationInterval implements Comparable<ReservationInterval> {

  // 预留开始时间戳（毫秒）
  private final long startTime;

  // 预留结束时间戳（毫秒）
  private final long endTime;

  /**
   * 构造指定起止时间的预留时间区间
   * @param startTime 预留开始时间戳
   * @param endTime 预留结束时间戳
   */
  public ReservationInterval(long startTime, long endTime) {
    this.startTime = startTime;
    this.endTime = endTime;
  }

  /**
   * 获取预留区间的开始时间
   * 
   * @return 开始时间戳
   */
  public long getStartTime() {
    return startTime;
  }

  /**
   * 获取预留区间的结束时间
   * 
   * @return 结束时间戳
   */
  public long getEndTime() {
    return endTime;
  }

  /**
   * 检查指定时间点是否落在当前预留区间内
   * 
   * @param tick 待检查的时间戳
   * @return true如果时间点在区间内（包含边界），否则false
   */
  public boolean isOverlap(long tick) {
    return (startTime <= tick && tick <= endTime);
  }

  @Override
  /**
   * 按先开始时间、后结束时间的顺序比较两个预留区间
   * @param anotherInterval 待比较的另一个预留区间
   * @return 负整数表示当前区间更小，0表示相等，正整数表示当前区间更大
   */
  public int compareTo(ReservationInterval anotherInterval) {
    long diff = 0;
    // 开始时间相等则比较结束时间
    if (startTime == anotherInterval.getStartTime()) {
      diff = endTime - anotherInterval.getEndTime();
    } else {
      // 否则比较开始时间
      diff = startTime - anotherInterval.getStartTime();
    }
    // 根据差值返回比较结果
    if (diff < 0) {
      return -1;
    } else if (diff > 0) {
      return 1;
    } else {
      return 0;
    }
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + (int) (endTime ^ (endTime >>> 32));
    result = prime * result + (int) (startTime ^ (startTime >>> 32));
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (!(obj instanceof ReservationInterval)) {
      return false;
    }
    ReservationInterval other = (ReservationInterval) obj;
    if (endTime != other.endTime) {
      return false;
    }
    if (startTime != other.startTime) {
      return false;
    }
    return true;
  }

  @Override
  public String toString() {
    return "[" + startTime + ", " + endTime + "]";
  }

}