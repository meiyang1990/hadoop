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
package org.apache.hadoop.hdfs.util;

import org.apache.hadoop.util.Preconditions;
import org.apache.commons.lang3.ArrayUtils;

import java.util.Arrays;

/**
 * 文件级注释：基于枚举类型的通用计数器工具类，为每个枚举常量维护一个独立的长整型计数，
 * 支持增删改查、批量重置、数值比较等操作，常用于HDFS中按枚举分类的统计场景。
 *
 * 示例使用说明：若存在枚举类型
 * <pre>
 * enum Fruit { APPLE, ORANGE, GRAPE }
 * </pre>
 * 可创建{@link EnumCounters}对象分别对APPLE、ORANGE和GRAPE进行计数统计。
 *
 * @param <E> 要统计的枚举类型
 */
public class EnumCounters<E extends Enum<E>> {
  /** 枚举的Class对象 */
  private final Class<E> enumClass;
  /** 对应枚举每个常量的计数数组，下标使用枚举的ordinal值 */
  private final long[] counters;

  /**
   * 构造方法：使用指定枚举类型创建计数器，所有计数器初始化为0。
   * @param enumClass 要统计的枚举Class对象
   */
  public EnumCounters(final Class<E> enumClass) {
    final E[] enumConstants = enumClass.getEnumConstants();
    Preconditions.checkNotNull(enumConstants);
    this.enumClass = enumClass;
    this.counters = new long[enumConstants.length];
  }

  /**
   * 构造方法：使用指定枚举类型创建计数器，所有计数器初始化为指定默认值。
   * @param enumClass 要统计的枚举Class对象
   * @param defaultVal 所有计数器的初始值
   */
  public EnumCounters(final Class<E> enumClass, long defaultVal) {
    final E[] enumConstants = enumClass.getEnumConstants();
    Preconditions.checkNotNull(enumConstants);
    this.enumClass = enumClass;
    this.counters = new long[enumConstants.length];
    reset(defaultVal);
  }
  
  /**
   * 获取指定枚举项的计数值。
   * @param e 目标枚举项
   * @return 指定枚举项当前的计数值
   */
  public final long get(final E e) {
    return counters[e.ordinal()];
  }

  /**
   * 获取所有计数值的数组快照（深拷贝）。
   * @return 所有计数器当前值的拷贝数组
   */
  public long[] asArray() {
    return ArrayUtils.clone(counters);
  }

  /**
   * 对所有计数器执行取反操作（所有计数值变为自身的负值）。
   */
  public void negation() {
    for(int i = 0; i < counters.length; i++) {
      counters[i] = -counters[i];
    }
  }
  
  /**
   * 将指定枚举项的计数设置为给定值。
   * @param e 目标枚举项
   * @param value 要设置的目标值
   */
  public void set(final E e, final long value) {
    counters[e.ordinal()] = value;
  }

  /**
   * 将当前所有计数器的值设置为另一个EnumCounters的对应值。
   * @param that 要复制的源计数器对象
   */
  public void set(final EnumCounters<E> that) {
    for(int i = 0; i < counters.length; i++) {
      this.counters[i] = that.counters[i];
    }
  }

  /**
   * 将所有计数器重置为0。
   */
  public void reset() {
    reset(0L);
  }

  /**
   * 给指定枚举项的计数加上给定增量值。
   * @param e 目标枚举项
   * @param value 要增加的数值（可为负数）
   */
  public void add(final E e, final long value) {
    counters[e.ordinal()] += value;
  }

  /**
   * 将另一个计数器的所有值对应加到当前计数器中。
   * @param that 要相加的源计数器对象
   */
  public void add(final EnumCounters<E> that) {
    for(int i = 0; i < counters.length; i++) {
      this.counters[i] += that.counters[i];
    }
  }

  /**
   * 给指定枚举项的计数减去给定值。
   * @param e 目标枚举项
   * @param value 要减去的数值
   */
  public void subtract(final E e, final long value) {
    counters[e.ordinal()] -= value;
  }

  /**
   * 将当前计数器的所有值减去另一个计数器的对应值。
   * @param that 要减去的源计数器对象
   */
  public void subtract(final EnumCounters<E> that) {
    for(int i = 0; i < counters.length; i++) {
      this.counters[i] -= that.counters[i];
    }
  }
  
  /**
   * 计算所有计数器的总和。
   * @return 所有计数值相加得到的总和
   */
  public long sum() {
    long sum = 0;
    for(int i = 0; i < counters.length; i++) {
      sum += counters[i];
    }
    return sum;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    } else if (!(obj instanceof EnumCounters)) {
      return false;
    }
    final EnumCounters<?> that = (EnumCounters<?>)obj;
    return this.enumClass == that.enumClass
        && Arrays.equals(this.counters, that.counters);
  }

  /**
   * 创建当前计数器对象的深拷贝。
   * @return 当前EnumCounters对象的深度拷贝
   */
  public EnumCounters<E> deepCopyEnumCounter() {
    EnumCounters<E> newCounter = new EnumCounters<>(enumClass);
    newCounter.set(this);
    return newCounter;
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(counters);
  }

  @Override
  public String toString() {
    final E[] enumConstants = enumClass.getEnumConstants();
    final StringBuilder b = new StringBuilder();
    for(int i = 0; i < counters.length; i++) {
      final String name = enumConstants[i].name();
      b.append(name).append("=").append(counters[i]).append(", ");
    }
    return b.substring(0, b.length() - 2);
  }

  /**
   * 将所有计数器重置为指定值。
   * @param val 重置后的目标值
   */
  public void reset(long val) {
    for(int i = 0; i < counters.length; i++) {
      this.counters[i] = val;
    }
  }

  /**
   * 检查所有计数器的值是否都小于等于给定值。
   * @param val 待比较的目标值
   * @return 若所有计数器都<=val返回true，否则返回false
   */
  public boolean allLessOrEqual(long val) {
    for (long c : counters) {
      if (c > val) {
        return false;
      }
    }
    return true;
  }

  /**
   * 检查是否存在任意计数器的值大于等于给定值。
   * @param val 待比较的目标值
   * @return 若至少有一个计数器>=val返回true，否则返回false
   */
  public boolean anyGreaterOrEqual(long val) {
    for (long c: counters) {
      if (c >= val) {
        return true;
      }
    }
    return false;
  }
}