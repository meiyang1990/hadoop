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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.VisibleForTesting;

/**
 * 周期性游程编码稀疏资源分配数据结构，基于周期性重复模式存储预约资源分配信息，默认周期为1天。
 * 用于支持周期性重复预约，避免存储重复周期的冗余数据。
 */
public class PeriodicRLESparseResourceAllocation
    extends RLESparseResourceAllocation {

  // 日志实例
  private static final Logger LOG =
      LoggerFactory.getLogger(PeriodicRLESparseResourceAllocation.class);

  // 周期长度，单位毫秒
  private long timePeriod;

  /**
   * 构造函数，指定资源计算器和周期长度。
   *
   * @param resourceCalculator 资源计算器，用于资源数值计算比较
   * @param timePeriod 周期长度，单位毫秒
   */
  public PeriodicRLESparseResourceAllocation(
      ResourceCalculator resourceCalculator, Long timePeriod) {
    super(resourceCalculator);
    this.timePeriod = timePeriod;
  }

  /**
   * 构造函数，使用默认周期长度（1天）。
   *
   * @param resourceCalculator 资源计算器，用于资源数值计算比较
   */
  public PeriodicRLESparseResourceAllocation(
      ResourceCalculator resourceCalculator) {
    this(resourceCalculator,
        YarnConfiguration.DEFAULT_RM_RESERVATION_SYSTEM_MAX_PERIODICITY);
  }

  /**
   * 构造函数，基于已有的游程编码分配数据构造周期性结构，仅用于测试。
   *
   * @param rleVector 已有的游程编码资源分配数据
   * @param timePeriod 周期长度，单位毫秒
   */
  @VisibleForTesting
  public PeriodicRLESparseResourceAllocation(
      RLESparseResourceAllocation rleVector, Long timePeriod) {
    super(rleVector.getCumulative(), rleVector.getResourceCalculator());
    this.timePeriod = timePeriod;

    // 调整偏移量，确保起始时间对齐到0点，处理周期环绕
    long delta = (getEarliestStartTime() % timePeriod - getEarliestStartTime());
    shift(delta);

    List<Long> toRemove = new ArrayList<>();
    Map<Long, Resource> toAdd = new TreeMap<>();

    // 遍历所有时间点，移除超出周期范围的点，并合并资源到单个周期内
    for (Map.Entry<Long, Resource> entry : cumulativeCapacity.entrySet()) {
      if (entry.getKey() > timePeriod) {
        toRemove.add(entry.getKey());
        if (entry.getValue() != null) {
          toAdd.put(timePeriod, entry.getValue());
          long prev = entry.getKey() % timePeriod;
          toAdd.put(prev, this.getCapacityAtTime(prev));
          toAdd.put(0L, entry.getValue());
        }
      }
    }
    // 移除原始超出周期的点
    for (Long l : toRemove) {
      cumulativeCapacity.remove(l);
    }
    // 添加合并后新的时间点资源
    cumulativeCapacity.putAll(toAdd);
  }

  /**
   * 根据周期性重复规则，获取指定时刻的已分配资源量。
   *
   * @param tick 需要查询的UTC时间戳
   * @return 指定时刻的已分配资源
   */
  public Resource getCapacityAtTime(long tick) {
    long convertedTime = (tick % timePeriod);
    return super.getCapacityAtTime(convertedTime);
  }

  /**
   * 在单个周期内的指定区间添加资源分配，仅允许操作[0, timePeriod]范围内的区间。
   * 由InMemoryPlan在预约 placement 阶段调用。
   *
   * @param interval 需要添加资源的时间区间
   * @param resource 需要添加的资源量
   * @return 添加成功返回true，区间超出范围返回false
   */
  public boolean addInterval(ReservationInterval interval, Resource resource) {
    long startTime = interval.getStartTime();
    long endTime = interval.getEndTime();

    if (startTime >= 0 && endTime > startTime && endTime <= timePeriod) {
      return super.addInterval(interval, resource);
    } else {
      LOG.info("Cannot set capacity beyond end time: " + timePeriod + " was ("
          + interval.toString() + ")");
      return false;
    }
  }

  /**
   * 在单个周期内的指定区间移除资源分配，仅允许操作[0, timePeriod]范围内的区间。
   *
   * @param interval 需要移除资源的时间区间
   * @param resource 需要移除的资源量
   * @return 移除成功返回true，区间超出范围或资源不足返回false
   */
  public boolean removeInterval(ReservationInterval interval,
      Resource resource) {
    long startTime = interval.getStartTime();
    long endTime = interval.getEndTime();
    // 如果待移除资源大于区间内最小可用资源，中止移除避免出现负容量
    // TODO 重新处理结束时间的递减操作
    if (!Resources.fitsIn(resource, getMinimumCapacityInInterval(
        new ReservationInterval(startTime, endTime - 1)))) {
      LOG.info("Request to remove more resources than what is available");
      return false;
    }
    if (startTime >= 0 && endTime > startTime && endTime <= timePeriod) {
      return super.removeInterval(interval, resource);
    } else {
      LOG.info("Interval extends beyond the end time " + timePeriod);
      return false;
    }
  }

  /**
   * 从指定基准时间开始，按给定周期步长计算所有时间点的最大资源量。
   *
   * @param tick 基准UTC时间，偏移量计算起点
   * @param period 周期步长，单位毫秒
   * @return 所有采样时间点中的最大资源量
   */
  public Resource getMaximumPeriodicCapacity(long tick, long period) {
    Resource maxResource;
    if (period < timePeriod) {
      maxResource = super.getMaximumPeriodicCapacity(tick % timePeriod, period);
    } else {
      // 如果步长大于等于本周期长度，整个区间内只有一个采样点，直接返回该点资源
      maxResource = super.getCapacityAtTime(tick % timePeriod);
    }
    return maxResource;
  }

  /**
   * 获取当前周期长度。
   *
   * @return 周期长度，单位毫秒
   */
  public long getTimePeriod() {
    return this.timePeriod;
  }

  @Override
  public String toString() {
    StringBuilder ret = new StringBuilder();
    ret.append("Period: ").append(timePeriod).append("\n")
        .append(super.toString());
    if (super.isEmpty()) {
      ret.append(" no allocations\n");
    }
    return ret.toString();
  }

  @Override
  public RLESparseResourceAllocation getRangeOverlapping(long start, long end) {
    NavigableMap<Long, Resource> unrolledMap = new TreeMap<>();
    readLock.lock();
    try {
      // 计算查询起始点在本周期内的相对位置
      long relativeStart = (start >= 0) ? start % timePeriod : 0;
      NavigableMap<Long, Resource> cumulativeMap = this.getCumulative();
      Long previous = cumulativeMap.floorKey(relativeStart);
      previous = (previous != null) ? previous : 0;
      // 展开所有覆盖查询范围的周期，确保覆盖到查询结束点
      for (long i = 0; i <= 1 + (end - start) / timePeriod; i++) {
        // 将当前周期内所有时间点展开到绝对时间坐标
        for (Map.Entry<Long, Resource> e : cumulativeMap.entrySet()) {
          long curKey = e.getKey() + (i * timePeriod);
          if (curKey >= previous && (start + curKey - relativeStart) <= end) {
            unrolledMap.put(curKey, e.getValue());
          }
        }
      }
      // 构造展开后的非周期性游程编码资源分配对象
      RLESparseResourceAllocation rle =
          new RLESparseResourceAllocation(unrolledMap, getResourceCalculator());
      // 平移坐标对齐查询起始点
      rle.shift(start - relativeStart);
      return rle;
    } finally {
      readLock.unlock();
    }
  }

}