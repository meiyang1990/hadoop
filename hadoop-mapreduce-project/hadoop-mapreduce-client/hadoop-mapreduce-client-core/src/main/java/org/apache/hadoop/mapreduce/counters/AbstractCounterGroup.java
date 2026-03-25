// 这个文件已经全部加上中文注释
/*
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

package org.apache.hadoop.mapreduce.counters;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.util.ResourceBundles;
import org.apache.hadoop.util.StringInterner;

import org.apache.hadoop.thirdparty.com.google.common.collect.Iterators;

/**
 * 文件：AbstractCounterGroup.java
 * 所属模块：MapReduce客户端核心
 * 功能：提供MapReduce计数器组的通用抽象实现，同时兼容mapred和mapreduce两个包的使用，
 * 封装了计数器组的公共管理逻辑，具体计数器创建由子类实现。
 *
 * 抽象计数器组基类，实现了计数器组的通用逻辑，为不同类型的计数器实现提供统一抽象。
 * 核心职责包括：管理同一分组下的多个计数器、处理计数器的查找/添加、提供序列化读写、
 * 支持计数器增量聚合，同时通过Limits控制计数器数量防止溢出。
 *
 * @param <T> 分组内计数器的具体类型
 */
@InterfaceAudience.Private
public abstract class AbstractCounterGroup<T extends Counter>
    implements CounterGroupBase<T> {

  private final String name;
  private String displayName;
  // 存储分组内所有计数器，按键（计数器名）有序排列，支持并发访问
  private final ConcurrentMap<String, T> counters =
      new ConcurrentSkipListMap<String, T>();
  private final Limits limits;

  /**
   * 构造抽象计数器组，初始化基本属性
   * @param name 计数器组名称
   * @param displayName 计数器组显示名称（用于界面展示）
   * @param limits 计数器数量限制器，防止计数器过多溢出
   */
  public AbstractCounterGroup(String name, String displayName,
                              Limits limits) {
    this.name = name;
    this.displayName = displayName;
    this.limits = limits;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public synchronized String getDisplayName() {
    return displayName;
  }

  @Override
  public synchronized void setDisplayName(String displayName) {
    this.displayName = displayName;
  }

  @Override
  public synchronized void addCounter(T counter) {
    // 将计数器按名称存入分组
    counters.put(counter.getName(), counter);
    // 计数器计数加一，检查是否超过数量限制
    limits.incrCounters();
  }

  @Override
  public synchronized T addCounter(String counterName, String displayName,
                                   long value) {
    // 过滤计数器名称，移除非法字符并截断过长名称
    String saveName = Limits.filterCounterName(counterName);
    // 查找已存在的计数器，不自动创建
    T counter = findCounterImpl(saveName, false);
    if (counter == null) {
      // 不存在则创建新计数器并添加到分组
      return addCounterImpl(saveName, displayName, value);
    }
    // 存在则更新计数器值
    counter.setValue(value);
    return counter;
  }

  private T addCounterImpl(String name, String displayName, long value) {
    // 通过抽象工厂方法创建新计数器实例
    T counter = newCounter(name, displayName, value);
    // 将新计数器添加到分组
    addCounter(counter);
    return counter;
  }

  @Override
  public synchronized T findCounter(String counterName, String displayName) {
    // 加锁避免多线程同时创建相同计数器的问题
    // 过滤计数器名称
    String saveName = Limits.filterCounterName(counterName);
    // 查找已存在的计数器，不自动创建
    T counter = findCounterImpl(saveName, false);
    if (counter == null) {
      // 不存在则创建初始值为0的新计数器
      return addCounterImpl(saveName, displayName, 0);
    }
    return counter;
  }

  @Override
  public T findCounter(String counterName, boolean create) {
    // 过滤名称后查找计数器
    return findCounterImpl(Limits.filterCounterName(counterName), create);
  }

  // 需要加锁，仅使用并发数据结构无法保证本地化、限制检查等逻辑的原子性
  private synchronized T findCounterImpl(String counterName, boolean create) {
    // 从分组中按名称获取计数器
    T counter = counters.get(counterName);
    if (counter == null && create) {
      // 不存在且需要创建时，从资源包获取本地化显示名称
      String localized =
          ResourceBundles.getCounterName(getName(), counterName, counterName);
      // 创建初始值为0的新计数器
      return addCounterImpl(counterName, localized, 0);
    }
    return counter;
  }

  @Override
  public T findCounter(String counterName) {
    // 默认自动创建不存在的计数器
    return findCounter(counterName, true);
  }

  /**
   * 抽象工厂方法，创建指定属性的新计数器实例，由具体子类实现
   * @param counterName 计数器名称
   * @param displayName 计数器显示名称
   * @param value 计数器初始值
   * @return 新创建的计数器实例
   */
  protected abstract T newCounter(String counterName, String displayName,
                                  long value);

  /**
   * 抽象工厂方法，创建空的新计数器实例，用于反序列化，由具体子类实现
   * @return 新创建的空计数器实例
   */
  protected abstract T newCounter();

  @Override
  public Iterator<T> iterator() {
    // 返回分组内所有计数器的迭代器
    return counters.values().iterator();
  }

  /**
   * 序列化格式：displayName | 计数器数量 | 多个计数器序列化结果
   */
  @Override
  public synchronized void write(DataOutput out) throws IOException {
    // 写入显示名称
    Text.writeString(out, displayName);
    // 写入计数器数量，使用可变长度整数压缩存储
    WritableUtils.writeVInt(out, counters.size());
    // 依次序列化每个计数器
    for(Counter counter: counters.values()) {
      counter.write(out);
    }
  }

  @Override
  public synchronized void readFields(DataInput in) throws IOException {
    // 读取显示名称并字符串驻留，节省内存
    displayName = StringInterner.weakIntern(Text.readString(in));
    // 清空原有计数器
    counters.clear();
    // 读取计数器数量
    int size = WritableUtils.readVInt(in);
    for (int i = 0; i < size; i++) {
      // 创建空计数器实例
      T counter = newCounter();
      // 反序列化计数器
      counter.readFields(in);
      // 将计数器存入分组
      counters.put(counter.getName(), counter);
      // 计数器计数加一，检查限制
      limits.incrCounters();
    }
  }

  @Override
  public synchronized int size() {
    // 返回分组内计数器数量
    return counters.size();
  }

  @Override
  public synchronized boolean equals(Object genericRight) {
    if (genericRight instanceof CounterGroupBase<?>) {
      @SuppressWarnings("unchecked")
      // 强转为计数器组类型
      CounterGroupBase<T> right = (CounterGroupBase<T>) genericRight;
      // 逐个比较所有计数器是否相等
      return Iterators.elementsEqual(iterator(), right.iterator());
    }
    // 类型不同直接返回不相等
    return false;
  }

  @Override
  public synchronized int hashCode() {
    // 使用内部计数器Map的哈希值作为分组哈希
    return counters.hashCode();
  }

  @Override
  public void incrAllCounters(CounterGroupBase<T> rightGroup) {
    try {
      // 遍历另一个分组的所有计数器
      for (Counter right : rightGroup) {
        // 在当前分组查找对应计数器，不存在则创建
        Counter left = findCounter(right.getName(), right.getDisplayName());
        // 将另一个分组的计数器值增量累加到当前计数器
        left.increment(right.getValue());
      }
    } catch (LimitExceededException e) {
      // 超过计数器数量限制时清空所有计数器并抛出异常
      counters.clear();
      throw e;
    }
  }
}