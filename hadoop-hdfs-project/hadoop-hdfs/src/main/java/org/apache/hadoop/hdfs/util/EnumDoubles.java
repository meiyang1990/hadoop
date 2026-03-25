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

import java.util.Arrays;

import org.apache.hadoop.util.Preconditions;

/**
 * 文件级注释：该类是HDFS工具类，实现了基于枚举类型的double值存储容器，
 * 类似EnumCounters，但值类型为double，用于按枚举类型分组存储和操作浮点型统计数据。
 *
 * @param <E> the enum type 用于索引的枚举类型
 */
/**
 * 基于枚举索引的double数组容器，为每个枚举常量维护一个double类型值，
 * 提供了批量算术运算和修改操作，方便对枚举分组的浮点统计数据进行管理。
 *
 * @param <E> 作为索引的枚举类型
 */
public class EnumDoubles<E extends Enum<E>> {
  /** 存储枚举类型的Class对象 */
  private final Class<E> enumClass;
  /** 存储double值数组，索引对应枚举的ordinal值 */
  private final double[] doubles;

  /**
   * 构造方法，根据给定枚举类型创建存储容器，初始化所有值为0。
   * @param enumClass 索引使用的枚举类
   */
  public EnumDoubles(final Class<E> enumClass) {
    final E[] enumConstants = enumClass.getEnumConstants();
    Preconditions.checkNotNull(enumConstants);
    this.enumClass = enumClass;
    this.doubles = new double[enumConstants.length];
  }
  
  /**
   * 获取指定枚举对应的double值。
   * @param e 目标枚举实例
   * @return 对应枚举存储的double值
   */
  public final double get(final E e) {
    return doubles[e.ordinal()];
  }

  /**
   * 对所有存储的值取反。
   */
  public final void negation() {
    for(int i = 0; i < doubles.length; i++) {
      doubles[i] = -doubles[i];
    }
  }
  
  /**
   * 设置指定枚举对应的double值。
   * @param e 目标枚举实例
   * @param value 需要设置的值
   */
  public final void set(final E e, final double value) {
    doubles[e.ordinal()] = value;
  }

  /**
   * 将当前对象所有值设置为另一个EnumDoubles对应的值。
   * @param that 源EnumDoubles对象
   */
  public final void set(final EnumDoubles<E> that) {
    for(int i = 0; i < doubles.length; i++) {
      this.doubles[i] = that.doubles[i];
    }
  }

  /**
   * 将所有值重置为0.0。
   */
  public final void reset() {
    for(int i = 0; i < doubles.length; i++) {
      this.doubles[i] = 0.0;
    }
  }

  /**
   * 给指定枚举对应的值增加给定增量。
   * @param e 目标枚举实例
   * @param value 需要增加的值
   */
  public final void add(final E e, final double value) {
    doubles[e.ordinal()] += value;
  }

  /**
   * 将另一个EnumDoubles的所有值加到当前对象对应位置。
   * @param that 需要相加的源EnumDoubles对象
   */
  public final void add(final EnumDoubles<E> that) {
    for(int i = 0; i < doubles.length; i++) {
      this.doubles[i] += that.doubles[i];
    }
  }

  /**
   * 给指定枚举对应的值减去给定值。
   * @param e 目标枚举实例
   * @param value 需要减去的值
   */
  public final void subtract(final E e, final double value) {
    doubles[e.ordinal()] -= value;
  }

  /**
   * 将当前对象所有值减去另一个EnumDoubles对应位置的值。
   * @param that 减数EnumDoubles对象
   */
  public final void subtract(final EnumDoubles<E> that) {
    for(int i = 0; i < doubles.length; i++) {
      this.doubles[i] -= that.doubles[i];
    }
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    } else if (!(obj instanceof EnumDoubles)) {
      return false;
    }
    final EnumDoubles<?> that = (EnumDoubles<?>)obj;
    return this.enumClass == that.enumClass
        && Arrays.equals(this.doubles, that.doubles);
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(doubles);
  }

  @Override
  public String toString() {
    final E[] enumConstants = enumClass.getEnumConstants();
    final StringBuilder b = new StringBuilder();
    for(int i = 0; i < doubles.length; i++) {
      final String name = enumConstants[i].name();
      b.append(name).append("=").append(doubles[i]).append(", ");
    }
    return b.substring(0, b.length() - 2);
  }
}