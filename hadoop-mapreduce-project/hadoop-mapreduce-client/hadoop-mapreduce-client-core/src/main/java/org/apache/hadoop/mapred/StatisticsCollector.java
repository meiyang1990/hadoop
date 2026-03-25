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
package org.apache.hadoop.mapred;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.hadoop.mapred.StatisticsCollector.Stat.TimeStat;

/**
 * 文件：多时间窗口统计数据收集器
 * 功能：为MapReduce任务监控提供分时间窗口的指标统计能力，支持滚动窗口和累计窗口两种统计模式
 */
/**
 * Collects the statistics in time windows.
 */
/**
 * 多时间窗口统计收集器核心类，负责管理多个统计指标和时间窗口，定时更新统计数据
 */
class StatisticsCollector {

  private static final int DEFAULT_PERIOD = 5;

  static final TimeWindow 
    SINCE_START = new TimeWindow("Since Start", -1, -1);
  
  static final TimeWindow 
    LAST_WEEK = new TimeWindow("Last Week", 7 * 24 * 60 * 60, 60 * 60);
  
  static final TimeWindow 
    LAST_DAY = new TimeWindow("Last Day", 24 * 60 * 60, 60 * 60);
  
  static final TimeWindow 
    LAST_HOUR = new TimeWindow("Last Hour", 60 * 60, 60);
  
  static final TimeWindow 
    LAST_MINUTE = new TimeWindow("Last Minute", 60, 10);

  /** 默认开启的统计时间窗口集合 */
  static final TimeWindow[] DEFAULT_COLLECT_WINDOWS = {
    StatisticsCollector.SINCE_START,
    StatisticsCollector.LAST_DAY,
    StatisticsCollector.LAST_HOUR
    };

  private final int period;
  private boolean started;
  
  /** 时间窗口对应的统计更新器映射表 */
  private final Map<TimeWindow, StatUpdater> updaters = 
    new LinkedHashMap<TimeWindow, StatUpdater>();
  /** 按名称存储的所有统计指标映射表 */
  private final Map<String, Stat> statistics = new HashMap<String, Stat>();

  /**
   * 使用默认更新周期（5秒）构造统计收集器
   */
  StatisticsCollector() {
    this(DEFAULT_PERIOD);
  }

  /**
   * 使用指定更新周期构造统计收集器
   * @param period 统计更新周期，单位为秒
   */
  StatisticsCollector(int period) {
    this.period = period;
  }

  /**
   * 启动统计收集器，启动后台定时线程定期更新所有统计数据
   */
  synchronized void start() {
    if (started) {
      return;
    }
    // 创建后台监控定时器线程，设为守护线程
    Timer timer = new Timer("Timer thread for monitoring ", true);
    TimerTask task = new TimerTask() {
      public void run() {
        update();
      }
    };
    long millis = period * 1000;
    // 按固定周期调度更新任务
    timer.scheduleAtFixedRate(task, millis, millis);
    started = true;
  }

  /**
   * 更新所有时间窗口的统计数据，由定时线程调用
   */
  protected synchronized void update() {
    for (StatUpdater c : updaters.values()) {
      c.update();
    }
  }

  /**
   * 获取所有时间窗口更新器的只读视图
   * @return 不可修改的更新器映射表
   */
  Map<TimeWindow, StatUpdater> getUpdaters() {
    return Collections.unmodifiableMap(updaters);
  }

  /**
   * 获取所有统计指标的只读视图
   * @return 不可修改的统计指标映射表
   */
  Map<String, Stat> getStatistics() {
    return Collections.unmodifiableMap(statistics);
  }

  /**
   * 在默认时间窗口集合上创建新的统计指标
   * @param name 统计指标名称
   * @return 创建完成的统计指标对象
   */
  synchronized Stat createStat(String name) {
    return createStat(name, DEFAULT_COLLECT_WINDOWS);
  }

  /**
   * 在指定时间窗口集合上创建新的统计指标
   * @param name 统计指标名称
   * @param windows 需要统计该指标的时间窗口数组
   * @return 创建完成的统计指标对象
   */
  synchronized Stat createStat(String name, TimeWindow[] windows) {
    // 指标名称不能重复
    if (statistics.get(name) != null) {
      throw new RuntimeException("Stat with name "+ name + 
          " is already defined");
    }
    // 构建每个时间窗口对应的统计数据对象
    Map<TimeWindow, TimeStat> timeStats = 
      new LinkedHashMap<TimeWindow, TimeStat>();
    for (TimeWindow window : windows) {
      StatUpdater collector = updaters.get(window);
      // 若该时间窗口还没有更新器，创建对应更新器
      if (collector == null) {
        if(SINCE_START.equals(window)) {
          // 从启动开始的累计窗口使用基础更新器
          collector = new StatUpdater();
        } else {
          // 滚动时间窗口使用专用更新器
          collector = new TimeWindowStatUpdater(window, period);
        }
        updaters.put(window, collector);
      }
      TimeStat timeStat = new TimeStat();
      collector.addTimeStat(name, timeStat);
      timeStats.put(window, timeStat);
    }

    Stat stat = new Stat(name, timeStats);
    statistics.put(name, stat);
    return stat;
  }

  /**
   * 移除指定名称的统计指标
   * @param name 要移除的统计指标名称
   * @return 被移除的统计指标对象，不存在则返回null
   */
  synchronized Stat removeStat(String name) {
    Stat stat = statistics.remove(name);
    if (stat != null) {
      // 从所有时间窗口更新器中移除该指标
      for (StatUpdater collector : updaters.values()) {
        collector.removeTimeStat(name);
      }
    }
    return stat;
  }

  /**
   * 时间窗口定义类，描述一个统计窗口的名称、大小和更新粒度
   */
  static class TimeWindow {
    final String name;
    final int windowSize;
    final int updateGranularity;
    /**
     * 构造时间窗口定义
     * @param name 窗口名称
     * @param windowSize 窗口总大小，单位为秒
     * @param updateGranularity 窗口内每个桶的粒度，单位为秒
     */
    TimeWindow(String name, int windowSize, int updateGranularity) {
      if (updateGranularity > windowSize) {
        throw new RuntimeException(
            "Invalid TimeWindow: updateGranularity > windowSize");
      }
      this.name = name;
      this.windowSize = windowSize;
      this.updateGranularity = updateGranularity;
    }

    public int hashCode() {
      return name.hashCode() + updateGranularity + windowSize;
    }

    public boolean equals(Object obj) {
      if (this == obj)
        return true;
      if (obj == null)
        return false;
      if (getClass() != obj.getClass())
        return false;
      final TimeWindow other = (TimeWindow) obj;
      if (name == null) {
        if (other.name != null)
          return false;
      } else if (!name.equals(other.name))
        return false;
      if (updateGranularity != other.updateGranularity)
        return false;
      if (windowSize != other.windowSize)
        return false;
      return true;
    }
  }

  /**
   * 单个统计指标，维护该指标在所有时间窗口下的统计数据
   */
  static class Stat {
    final String name;
    private Map<TimeWindow, TimeStat> timeStats;

    /**
     * 构造统计指标
     * @param name 指标名称
     * @param timeStats 各时间窗口对应的统计数据
     */
    private Stat(String name, Map<TimeWindow, TimeStat> timeStats) {
      this.name = name;
      this.timeStats = timeStats;
    }

    /**
     * 给该指标在所有时间窗口上增加指定增量
     * @param incr 增量值
     */
    public synchronized void inc(int incr) {
      for (TimeStat ts : timeStats.values()) {
        ts.inc(incr);
      }
    }

    /**
     * 给该指标增加1个增量
     */
    public synchronized void inc() {
      inc(1);
    }

    /**
     * 获取该指标在所有时间窗口下的统计数据只读视图
     * @return 不可修改的时间窗口统计数据映射
     */
    public synchronized Map<TimeWindow, TimeStat> getValues() {
      return Collections.unmodifiableMap(timeStats);
    }

    /**
     * 单个时间窗口内的统计数据存储类，基于桶结构存储计数值
     */
    static class TimeStat {
      /** 存储历史桶的计数值链表，滚动窗口会移除过期桶 */
      private final LinkedList<Integer> buckets = new LinkedList<Integer>();
      /** 当前窗口总统计值 */
      private int value;
      /** 当前未提交到桶的增量值 */
      private int currentValue;

      /**
       * 获取当前窗口总统计值
       * @return 总统计值
       */
      public synchronized int getValue() {
        return value;
      }

      /**
       * 增加当前周期的增量值
       * @param i 增量值
       */
      private synchronized void inc(int i) {
        currentValue += i;
      }

      /**
       * 将当前周期增量打包为新桶加入链表
       */
      private synchronized void addBucket() {
        buckets.addLast(currentValue);
        setValueToCurrent();
      }

      /**
       * 将当前增量合并到总统计值，重置当前增量
       */
      private synchronized void setValueToCurrent() {
        value += currentValue;
        currentValue = 0;
      }

      /**
       * 移除最早的过期桶，并从总统计值中减去该桶的值
       */
      private synchronized void removeBucket() {
        int removed = buckets.removeFirst();
        value -= removed;
      }
    }
  }

  /**
   * 基础统计更新器，用于累计窗口（从启动开始）的统计更新
   */
  private static class StatUpdater {

    /** 需要更新的统计指标映射表，键为指标名称 */
    protected final Map<String, TimeStat> statToCollect = 
      new HashMap<String, TimeStat>();

    /**
     * 添加一个需要更新的统计指标
     * @param name 指标名称
     * @param s 时间窗口统计数据对象
     */
    synchronized void addTimeStat(String name, TimeStat s) {
      statToCollect.put(name, s);
    }

    /**
     * 移除指定名称的统计指标
     * @param name 指标名称
     * @return 被移除的统计数据对象
     */
    synchronized TimeStat removeTimeStat(String name) {
      return statToCollect.remove(name);
    }

    /**
     * 更新所有管理的统计指标，将当前增量合并到总统计值
     */
    synchronized void update() {
      for (TimeStat stat : statToCollect.values()) {
        stat.setValueToCurrent();
      }
    }
  }

  /**
   * Updates TimeWindow statistics in buckets.
   *
   */
  /**
   * 滚动时间窗口统计更新器，继承基础更新器，实现按桶滚动更新逻辑
   */
  private static class TimeWindowStatUpdater extends StatUpdater{

    /** 时间窗口需要保留的桶总数 */
    final int collectBuckets;
    /** 每个桶需要多少次更新周期才能填满 */
    final int updatesPerBucket;
    
    /** 当前桶已经累积的更新次数 */
    private int updates;
    /** 当前窗口已经创建的桶总数 */
    private int buckets;

    /**
     * 构造滚动时间窗口更新器
     * @param w 时间窗口定义
     * @param updatePeriod 收集器更新周期，单位秒
     */
    TimeWindowStatUpdater(TimeWindow w, int updatePeriod) {
      if (updatePeriod > w.updateGranularity) {
        throw new RuntimeException(
            "Invalid conf: updatePeriod > updateGranularity");
      }
      // 计算总桶数 = 窗口大小 / 桶粒度
      collectBuckets = w.windowSize / w.updateGranularity;
      // 计算每个桶需要多少次更新 = 桶粒度 / 更新周期
      updatesPerBucket = w.updateGranularity / updatePeriod;
    }

    /**
     * 执行滚动时间窗口的更新逻辑，填满桶后生成新桶，超过窗口大小后移除过期桶
     */
    synchronized void update() {
      updates++;
      // 当累积次数达到每个桶要求的次数，生成新桶
      if (updates == updatesPerBucket) {
        for(TimeStat stat : statToCollect.values()) {
          stat.addBucket();
        }
        updates = 0;
        buckets++;
        // 当桶数超过窗口要求，移除最早的过期桶
        if (buckets > collectBuckets) {
          for (TimeStat stat : statToCollect.values()) {
            stat.removeBucket();
          }
          buckets--;
        }
      }
    }
  }

}