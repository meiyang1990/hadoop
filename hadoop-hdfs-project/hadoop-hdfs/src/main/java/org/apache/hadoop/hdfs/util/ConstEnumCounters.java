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

/**
 * 文件功能说明：基于枚举类型的常量计数器，实现不可修改的枚举计数器功能，所有修改操作都会抛出异常
 *
 * 这是EnumCounters的只读常量版本，任何修改操作都会触发ConstEnumException异常，
 * 用于需要保证计数器数据不可被修改的场景。
 *
 * @see org.apache.hadoop.hdfs.util.EnumCounters
 */
public class ConstEnumCounters<E extends Enum<E>> extends EnumCounters<E> {

  /**
   * 异常类定义：当尝试修改常量计数器时抛出的异常
   */
  public static final class ConstEnumException extends RuntimeException {
    private ConstEnumException(String msg) {
      super(msg);
    }
  }

  /** 单例异常实例，所有修改操作都抛出该异常 */
  private static final ConstEnumException CONST_ENUM_EXCEPTION =
      new ConstEnumException("modification on const.");

  /**
   * 构造函数：构造一个指定枚举类型、设置默认值的常量计数器
   *
   * @param enumClass 枚举类型的Class对象
   * @param defaultVal 所有计数器的默认初始值
   */
  public ConstEnumCounters(Class<E> enumClass, long defaultVal) {
    super(enumClass);
    forceReset(defaultVal);
  }

  @Override
  /** 覆盖父类取反方法，禁止修改，抛出异常 */
  public final void negation() {
    throw CONST_ENUM_EXCEPTION;
  }

  @Override
  /** 覆盖父类设置指定枚举计数器方法，禁止修改，抛出异常 */
  public final void set(final E e, final long value) {
    throw CONST_ENUM_EXCEPTION;
  }

  @Override
  /** 覆盖父类批量设置方法，禁止修改，抛出异常 */
  public final void set(final EnumCounters<E> that) {
    throw CONST_ENUM_EXCEPTION;
  }

  @Override
  /** 覆盖父类重置方法，禁止修改，抛出异常 */
  public final void reset() {
    throw CONST_ENUM_EXCEPTION;
  }

  @Override
  /** 覆盖父类加法方法，禁止修改，抛出异常 */
  public final void add(final E e, final long value) {
    throw CONST_ENUM_EXCEPTION;
  }

  @Override
  /** 覆盖父类批量加法方法，禁止修改，抛出异常 */
  public final void add(final EnumCounters<E> that) {
    throw CONST_ENUM_EXCEPTION;
  }

  @Override
  /** 覆盖父类减法方法，禁止修改，抛出异常 */
  public final void subtract(final E e, final long value) {
    throw CONST_ENUM_EXCEPTION;
  }

  @Override
  /** 覆盖父类批量减法方法，禁止修改，抛出异常 */
  public final void subtract(final EnumCounters<E> that) {
    throw CONST_ENUM_EXCEPTION;
  }

  @Override
  /** 覆盖父类重置指定值方法，禁止修改，抛出异常 */
  public final void reset(long val) {
    throw CONST_ENUM_EXCEPTION;
  }

  /** 强制重置计数器值，仅在构造时内部使用，初始化所有计数器为指定值 */
  private void forceReset(long val) {
    super.reset(val);
  }
}