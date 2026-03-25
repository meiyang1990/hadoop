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

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.exceptions.PlanningException;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;

/**
 * 文件说明：YARN资源预留模块的行程编码稀疏资源分配数据结构
 * 核心功能：基于行程编码实现稀疏存储，维护时间维度上的资源分配信息，节省内存占用
 */
public class RLESparseResourceAllocation {

  // toString输出最大条目阈值，超过阈值仅输出概览信息
  private static final int THRESHOLD = 100;
  // 零资源常量，表示不分配任何资源
  private static final Resource ZERO_RESOURCE = Resources.none();

  @SuppressWarnings("checkstyle:visibilitymodifier")
  // 存储行程编码后的累计容量，key为时间戳，value为该时间点后的累计资源量
  protected NavigableMap<Long, Resource> cumulativeCapacity =
      new TreeMap<Long, Resource>();

  // 读写锁，保障并发访问安全性，读读不互斥，读写互斥
  private final ReentrantReadWriteLock readWriteLock =
      new ReentrantReadWriteLock();
  @SuppressWarnings("checkstyle:visibilitymodifier")
  protected final Lock readLock = readWriteLock.readLock();
  private final Lock writeLock = readWriteLock.writeLock();

  // 资源计算器，用于资源比较和计算
  private final ResourceCalculator resourceCalculator;

  /**
   * 构造函数，初始化稀疏资源分配实例
   * @param resourceCalculator 资源计算器
   */
  public RLESparseResourceAllocation(ResourceCalculator resourceCalculator) {
    this.resourceCalculator = resourceCalculator;
  }

  /**
   * 构造函数，使用已有的行程编码数据初始化稀疏资源分配实例
   * @param out 已有的行程编码累计容量数据
   * @param resourceCalculator 资源计算器
   */
  public RLESparseResourceAllocation(NavigableMap<Long, Resource> out,
      ResourceCalculator resourceCalculator) {
    // miss check for repeated entries
    this.cumulativeCapacity = out;
    this.resourceCalculator = resourceCalculator;
  }

  /**
   * 在指定时间区间内增加资源分配
   *
   * @param reservationInterval 资源要添加的时间区间
   * @param totCap 要添加的资源总量
   * @return 增加成功返回true，当前实现总是返回true
   */
  public boolean addInterval(ReservationInterval reservationInterval,
      Resource totCap) {
    if (totCap.equals(ZERO_RESOURCE)) {
      return true;
    }
    writeLock.lock();
    try {
      // 把要添加的区间转换为行程编码格式：起点加资源量，终点加零资源
      NavigableMap<Long, Resource> addInt = new TreeMap<Long, Resource>();
      addInt.put(reservationInterval.getStartTime(), totCap);
      addInt.put(reservationInterval.getEndTime(), ZERO_RESOURCE);
      try {
        // 合并新分配到现有资源分配中
        cumulativeCapacity =
            merge(resourceCalculator, totCap, cumulativeCapacity, addInt,
                Long.MIN_VALUE, Long.MAX_VALUE, RLEOperator.add);
      } catch (PlanningException e) {
        // add操作不会抛出异常，此处仅捕获签名要求
      }
      return true;
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * 在指定时间区间内移除资源分配
   *
   * @param reservationInterval 资源要移除的时间区间
   * @param totCap 要移除的资源总量
   * @return 移除成功返回true，当前实现总是返回true
   */
  public boolean removeInterval(ReservationInterval reservationInterval,
      Resource totCap) {
    if (totCap.equals(ZERO_RESOURCE)) {
      return true;
    }
    writeLock.lock();
    try {

      // 把要移除的区间转换为行程编码格式：起点加资源量，终点加零资源
      NavigableMap<Long, Resource> removeInt = new TreeMap<Long, Resource>();
      removeInt.put(reservationInterval.getStartTime(), totCap);
      removeInt.put(reservationInterval.getEndTime(), ZERO_RESOURCE);
      try {
        // 合并移除操作到现有资源分配中
        cumulativeCapacity =
            merge(resourceCalculator, totCap, cumulativeCapacity, removeInt,
                Long.MIN_VALUE, Long.MAX_VALUE, RLEOperator.subtract);
      } catch (PlanningException e) {
        // subtract操作不会抛出异常，此处仅捕获签名要求
      }
      return true;
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * 获取指定时间点的累计资源分配量
   *
   * @param tick 查询的时间戳
   * @return 指定时间点的资源分配总量
   */
  public Resource getCapacityAtTime(long tick) {
    readLock.lock();
    try {
      // 找到不大于查询时间的最大时间点，其值就是查询时间点的累计资源量
      Entry<Long, Resource> closestStep = cumulativeCapacity.floorEntry(tick);
      if (closestStep != null) {
        return Resources.clone(closestStep.getValue());
      }
      // 早于第一个时间点，返回零资源
      return Resources.clone(ZERO_RESOURCE);
    } finally {
      readLock.unlock();
    }
  }

  /**
   * 获取最早资源分配的开始时间戳
   *
   * @return 最早分配的时间戳，无分配则返回-1
   */
  public long getEarliestStartTime() {
    readLock.lock();
    try {
      if (cumulativeCapacity.isEmpty()) {
        return -1;
      } else {
        return cumulativeCapacity.firstKey();
      }
    } finally {
      readLock.unlock();
    }
  }

  /**
   * 获取最新非空资源分配的时间戳
   *
   * @return 最新非空分配的时间戳，无分配则返回-1
   */
  public long getLatestNonNullTime() {
    readLock.lock();
    try {
      if (cumulativeCapacity.isEmpty()) {
        return -1;
      } else {
        // 最后一个条目可能是空值（用于终止序列），返回前一个条目
        Entry<Long, Resource> last = cumulativeCapacity.lastEntry();
        if (last.getValue() == null) {
          return cumulativeCapacity.floorKey(last.getKey() - 1);
        } else {
          return last.getKey();
        }
      }
    } finally {
      readLock.unlock();
    }
  }

  /**
   * 检查当前是否没有任何非零资源分配
   *
   * @return 无任何有效分配返回true，否则返回false
   */
  public boolean isEmpty() {
    readLock.lock();
    try {
      if (cumulativeCapacity.isEmpty()) {
        return true;
      }
      // 删除操作后可能只剩一个零条目和末尾null，需要检查这种情况
      if (cumulativeCapacity.size() == 2) {
        return cumulativeCapacity.firstEntry().getValue().equals(ZERO_RESOURCE)
            && cumulativeCapacity.lastEntry().getValue() == null;
      }
      return false;
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public String toString() {
    StringBuilder ret = new StringBuilder();
    readLock.lock();
    try {
      // 条目超过阈值只输出概览，否则输出所有条目
      if (cumulativeCapacity.size() > THRESHOLD) {
        ret.append("Number of steps: ").append(cumulativeCapacity.size())
            .append(" earliest entry: ").append(cumulativeCapacity.firstKey())
            .append(" latest entry: ").append(cumulativeCapacity.lastKey());
      } else {
        for (Map.Entry<Long, Resource> r : cumulativeCapacity.entrySet()) {
          ret.append(r.getKey()).append(": ").append(r.getValue())
              .append("\n ");
        }
      }
      return ret.toString();
    } finally {
      readLock.unlock();
    }
  }

  /**
   * 将当前行程编码的资源分配转换为区间映射，方便遍历所有有效分配区间
   *
   * @return 区间到资源量的映射表
   */
  public Map<ReservationInterval, Resource> toIntervalMap() {

    readLock.lock();
    try {
      Map<ReservationInterval, Resource> allocations =
          new TreeMap<ReservationInterval, Resource>();

      // 空分配直接返回空映射
      if (isEmpty()) {
        return allocations;
      }

      Map.Entry<Long, Resource> lastEntry = null;
      // 遍历行程编码条目，连续两个时间点构成一个区间
      for (Map.Entry<Long, Resource> entry : cumulativeCapacity.entrySet()) {

        if (lastEntry != null && entry.getValue() != null) {
          ReservationInterval interval =
              new ReservationInterval(lastEntry.getKey(), entry.getKey());
          Resource resource = lastEntry.getValue();

          allocations.put(interval, resource);
        }

        lastEntry = entry;
      }
      return allocations;
    } finally {
      readLock.unlock();
    }
  }

  /**
   * 获取累计容量的行程编码映射
   * @return 累计容量映射表
   */
  public NavigableMap<Long, Resource> getCumulative() {
    readLock.lock();
    try {
      return cumulativeCapacity;
    } finally {
      readLock.unlock();
    }
  }

  /**
   * 获取资源计算器实例
   * @return 资源计算器
   */
  public ResourceCalculator getResourceCalculator() {
    return resourceCalculator;
  }

  /**
   * 对两个RLESparseResourceAllocation在指定时间范围内执行指定操作合并
   *
   * @param resCalc 资源计算器
   * @param clusterResource 集群总资源量（用于DRF调度计算）
   * @param a 左操作数
   * @param b 右操作数
   * @param operator 合并操作类型
   * @param start 合并时间范围起点
   * @param end 合并时间范围终点
   * @return 合并后的新RLESparseResourceAllocation实例
   * @throws PlanningException 如果操作要求结果非负但结果出现负值则抛出异常
   */
  public static RLESparseResourceAllocation merge(ResourceCalculator resCalc,
      Resource clusterResource, RLESparseResourceAllocation a,
      RLESparseResourceAllocation b, RLEOperator operator, long start, long end)
      throws PlanningException {
    // 截取两个输入在指定范围内的子区间
    NavigableMap<Long, Resource> cumA =
        a.getRangeOverlapping(start, end).getCumulative();
    NavigableMap<Long, Resource> cumB =
        b.getRangeOverlapping(start, end).getCumulative();
    // 执行合并
    NavigableMap<Long, Resource> out =
        merge(resCalc, clusterResource, cumA, cumB, start, end, operator);
    // 封装为新实例返回
    return new RLESparseResourceAllocation(out, resCalc);
  }

  /**
   * 对两个行程编码映射在指定时间范围内执行指定操作合并，内部实现方法
   */
  private static NavigableMap<Long, Resource> merge(ResourceCalculator resCalc,
      Resource clusterResource, NavigableMap<Long, Resource> a,
      NavigableMap<Long, Resource> b, long start, long end,
      RLEOperator operator) throws PlanningException {

    // 处理其中一个输入为空的特殊情况
    if (a == null || a.isEmpty()) {
      if (operator == RLEOperator.subtract
          || operator == RLEOperator.subtractTestNonNegative) {
        return negate(operator, b);
      } else {
        return b;
      }
    }
    if (b == null || b.isEmpty()) {
      return a;
    }

    // 初始化双指针遍历两个有序映射
    Iterator<Entry<Long, Resource>> aIt = a.entrySet().iterator();
    Iterator<Entry<Long, Resource>> bIt = b.entrySet().iterator();
    Entry<Long, Resource> curA = aIt.next();
    Entry<Long, Resource> curB = bIt.next();
    Entry<Long, Resource> lastA = null;
    Entry<Long, Resource> lastB = null;
    boolean aIsDone = false;
    boolean bIsDone = false;

    TreeMap<Long, Resource> out = new TreeMap<Long, Resource>();

    // 双指针合并有序时间线
    while (!(curA.equals(lastA) && curB.equals(lastB))) {

      Resource outRes;
      long time = -1;

      // 当前A时间点小于B时间点，处理A点
      if (bIsDone || (curA.getKey() < curB.getKey() && !aIsDone)) {
        outRes = combineValue(operator, resCalc, clusterResource, curA, lastB);
        // 小于起点的时间点截断到起点
        time = (curA.getKey() < start) ? start : curA.getKey();
        lastA = curA;
        // 移动A指针
        if (aIt.hasNext()) {
          curA = aIt.next();
        } else {
          aIsDone = true;
        }

      } else {
        // 当前B时间点小于A时间点，处理B点
        if (aIsDone || (curA.getKey() > curB.getKey() && !bIsDone)) {
          outRes =
              combineValue(operator, resCalc, clusterResource, lastA, curB);
          // 小于起点的时间点截断到起点
          time = (curB.getKey() < start) ? start : curB.getKey();
          lastB = curB;
          // 移动B指针
          if (bIt.hasNext()) {
            curB = bIt.next();
          } else {
            bIsDone = true;
          }

        } else {
          // A和B时间点相同，合并处理
          outRes = combineValue(operator, resCalc, clusterResource, curA, curB);
          // 小于起点的时间点截断到起点
          time = (curA.getKey() < start) ? start : curA.getKey();
          // 同时移动两个指针
          lastA = curA;
          if (aIt.hasNext()) {
            curA = aIt.next();
          } else {
            aIsDone = true;
          }
          lastB = curB;
          if (bIt.hasNext()) {
            curB = bIt.next();
          } else {
            bIsDone = true;
          }
        }
      }

      // 只有和前一个值不同才添加，压缩冗余数据
      addIfNeeded(out, time, outRes);
    }
    // 添加终点标记
    addIfNeeded(out, end, null);

    return out;
  }

  /**
   * 对输入映射所有资源值取反，用于减法操作
   */
  private static NavigableMap<Long, Resource> negate(RLEOperator operator,
      NavigableMap<Long, Resource> a) throws PlanningException {

    TreeMap<Long, Resource> out = new TreeMap<Long, Resource>();
    for (Entry<Long, Resource> e : a.entrySet()) {
      Resource val = Resources.negate(e.getValue());
      // 如果要求结果非负，检查是否出现负值
      if (operator == RLEOperator.subtractTestNonNegative
          && (Resources.fitsIn(val, ZERO_RESOURCE)
              && !Resources.equals(val, ZERO_RESOURCE))) {
        throw new PlanningException(
            "RLESparseResourceAllocation: merge failed as the "
                + "resulting RLESparseResourceAllocation would be negative");
      }
      out.put(e.getKey(), val);
    }

    return out;
  }

  /**
   * 仅当输出结果与上一个值不同时才添加，压缩冗余条目
   */
  private static void addIfNeeded(TreeMap<Long, Resource> out, long time,
      Resource outRes) {

    if (out.isEmpty() || (out.lastEntry() != null && outRes == null)
        || (out.lastEntry().getValue() != null
            && !Resources.equals(out.lastEntry().getValue(), outRes))) {
      out.put(time, outRes);
    }

  }

  /**
   * 根据操作类型计算两个资源值的