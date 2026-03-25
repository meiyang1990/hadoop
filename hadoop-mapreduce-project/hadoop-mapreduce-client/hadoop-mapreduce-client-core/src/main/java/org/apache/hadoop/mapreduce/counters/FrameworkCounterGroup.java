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

import static org.apache.hadoop.util.Preconditions.checkNotNull;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.util.ResourceBundles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.thirdparty.com.google.common.collect.AbstractIterator;
import org.apache.hadoop.thirdparty.com.google.common.collect.Iterators;

/**
 * 文件说明：MapReduce框架内置计数器组抽象基类，为mapred和mapreduce两个包提供框架计数器组的通用实现，
 * 基于枚举类型定义框架计数器，统一管理不同类型框架计数器的查找、序列化、累加等公共逻辑。
 *
 * @param <T> 计数器枚举类型
 * @param <C> 具体计数器类型
 */
@InterfaceAudience.Private
public abstract class FrameworkCounterGroup<T extends Enum<T>,
    C extends Counter> implements CounterGroupBase<C> {
  private static final Logger LOG =
      LoggerFactory.getLogger(FrameworkCounterGroup.class);
  
  private final Class<T> enumClass; // 存储计数器枚举类，用于Enum.valueOf查找枚举实例
  private final Object[] counters;  // 按枚举序号存储计数器实例，缓存已创建的计数器
  private String displayName = null; // 计数器组显示名称，用于Web UI展示

  /**
   * 框架计数器实现类，为Hadoop框架内置计数器提供门面实现，通过兼容新旧接口简化兼容性处理。
   *
   * @param <T> 对应枚举类型
   */
  @InterfaceAudience.Private
  public static class FrameworkCounter<T extends Enum<T>> extends AbstractCounter {
    final T key; // 对应的枚举常量
    final String groupName; // 所属计数器组名称
    private long value; // 当前计数器值

    /**
     * 构造函数，基于枚举常量和所属组名称创建框架计数器实例。
     * @param ref 对应的枚举常量
     * @param groupName 所属计数器组名称
     */
    public FrameworkCounter(T ref, String groupName) {
      key = ref;
      this.groupName = groupName;
    }
    
    /**
     * 获取计数器对应的枚举键。
     * @return 枚举键
     */
    @Private
    public T getKey() {
      return key;
    }

    /**
     * 获取计数器所属组名称。
     * @return 计数器组名称
     */
    @Private
    public String getGroupName() {
      return groupName;
    }
    
    @Override
    public String getName() {
      return key.name();
    }

    @Override
    public String getDisplayName() {
      // 从资源包获取国际化展示名称，不存在则返回原名称
      return ResourceBundles.getCounterName(groupName, getName(), getName());
    }

    @Override
    public long getValue() {
      return value;
    }

    @Override
    public void setValue(long value) {
      this.value = value;
    }

    @Override
    public void increment(long incr) {
      // 对_MAX结尾的计数器特殊处理，只保留最大值，实现最大值统计逻辑
      if (key.name().endsWith("_MAX")) {
        value = value > incr ? value : incr;
      } else {
        value += incr;
      }
    }

    @Override
    public void write(DataOutput out) throws IOException {
      assert false : "shouldn't be called";
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      assert false : "shouldn't be called";
    }

    @Override
    public Counter getUnderlyingCounter() {
      return this;
    }
  }

  /**
   * 构造函数，基于枚举类创建框架计数器组。
   * @param enumClass 定义计数器的枚举类
   */
  @SuppressWarnings("unchecked")
  public FrameworkCounterGroup(Class<T> enumClass) {
    this.enumClass = enumClass;
    T[] enums = enumClass.getEnumConstants();
    counters = new Object[enums.length];
  }

  @Override
  public String getName() {
    return enumClass.getName();
  }

  @Override
  public String getDisplayName() {
    if (displayName == null) {
      // 从资源包获取国际化展示名称，不存在则返回类名
      displayName = ResourceBundles.getCounterGroupName(getName(), getName());
    }
    return displayName;
  }

  @Override
  public void setDisplayName(String displayName) {
    this.displayName = displayName;
  }

  /**
   * 根据名称查找对应枚举常量。
   * @param name 枚举常量名称
   * @return 对应枚举实例
   */
  private T valueOf(String name) {
    return Enum.valueOf(enumClass, name);
  }

  @Override
  public void addCounter(C counter) {
    C ours = findCounter(counter.getName());
    if (ours != null) {
      ours.setValue(counter.getValue());
    } else {
      LOG.warn(counter.getName() + "is not a known counter.");
    }
  }

  @Override
  public C addCounter(String name, String displayName, long value) {
    C counter = findCounter(name);
    if (counter != null) {
      counter.setValue(value);
    } else {
      LOG.warn(name + "is not a known counter.");
    }
    return counter;
  }

  @Override
  public C findCounter(String counterName, String displayName) {
    return findCounter(counterName);
  }

  @Override
  public C findCounter(String counterName, boolean create) {
    try {
      return findCounter(valueOf(counterName));
    }
    catch (Exception e) {
      if (create) throw new IllegalArgumentException(e);
      return null;
    }
  }

  @Override
  public C findCounter(String counterName) {
    try {
      T enumValue = valueOf(counterName);
      return findCounter(enumValue);
    } catch (IllegalArgumentException e) {
      LOG.warn(counterName + " is not a recognized counter.");
      return null;
    }
  }

  /**
   * 根据枚举键查找对应计数器，懒加载创建计数器实例。
   * @param key 枚举键
   * @return 对应计数器实例
   */
  @SuppressWarnings("unchecked")
  private C findCounter(T key) {
    int i = key.ordinal();
    if (counters[i] == null) {
      counters[i] = newCounter(key);
    }
    return (C) counters[i];
  }

  /**
   * 抽象工厂方法，由子类实现创建具体类型的计数器实例。
   * @param key 计数器对应的枚举键
   * @return 新创建的计数器实例
   */
  protected abstract C newCounter(T key);

  @Override
  public int size() {
    int n = 0;
    // 统计已初始化的计数器数量
    for (int i = 0; i < counters.length; ++i) {
      if (counters[i] != null) ++n;
    }
    return n;
  }

  @Override
  @SuppressWarnings("rawtypes")
  public void incrAllCounters(CounterGroupBase<C> other) {
    // 将另一个计数器组的所有计数器值累加至当前组
    if (checkNotNull(other, "other counter group")
        instanceof FrameworkCounterGroup<?, ?>) {
      for (Counter counter : other) {
        C c = findCounter(((FrameworkCounter) counter).key.name());
        if (c != null) {
          c.increment(counter.getValue());
        }
      }
    }
  }

  /**
   * 序列化框架计数器组到输出流，格式: 计数器数量 (枚举序号 计数器值)*
   */
  @Override
  @SuppressWarnings("unchecked")
  public void write(DataOutput out) throws IOException {
    WritableUtils.writeVInt(out, size());
    for (int i = 0; i < counters.length; ++i) {
      Counter counter = (C) counters[i];
      if (counter != null) {
        // 只序列化已初始化的计数器
        WritableUtils.writeVInt(out, i);
        WritableUtils.writeVLong(out, counter.getValue());
      }
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    // 清空原有计数器数据
    clear();
    int len = WritableUtils.readVInt(in);
    T[] enums = enumClass.getEnumConstants();
    // 反序列化每个计数器
    for (int i = 0; i < len; ++i) {
      int ord = WritableUtils.readVInt(in);
      Counter counter = newCounter(enums[ord]);
      counter.setValue(WritableUtils.readVLong(in));
      counters[ord] = counter;
    }
  }

  /**
   * 清空所有计数器实例。
   */
  private void clear() {
    for (int i = 0; i < counters.length; ++i) {
      counters[i] = null;
    }
  }

  @Override
  public Iterator<C> iterator() {
    // 返回跳空null的迭代器，只遍历已初始化的计数器
    return new AbstractIterator<C>() {
      int i = 0;
      @Override
      protected C computeNext() {
        while (i < counters.length) {
          @SuppressWarnings("unchecked")
          C counter = (C) counters[i++];
          if (counter != null) return counter;
        }
        return endOfData();
      }
    };
  }

  @Override
  public boolean equals(Object genericRight) {
    if (genericRight instanceof CounterGroupBase<?>) {
      @SuppressWarnings("unchecked")
      CounterGroupBase<C> right = (CounterGroupBase<C>) genericRight;
      // 逐元素迭代比较计数器内容
      return Iterators.elementsEqual(iterator(), right.iterator());
    }
    return false;
  }

  @Override
  public synchronized int hashCode() {
    // 深度计算哈希，包含枚举类、计数器数组和展示名称
    return Arrays.deepHashCode(new Object[]{enumClass, counters, displayName});
  }
}