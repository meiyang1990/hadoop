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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.util.StringInterner;

/**
 * MapReduce通用计数器实现，提供基础的计数功能，支持序列化反序列化
 * 用于存储MapReduce作业中各类统计指标，可被所有类型的任务使用
 */
@InterfaceAudience.Private
public class GenericCounter extends AbstractCounter {

  private String name;
  private String displayName;
  private long value = 0;

  /**
   * 默认构造函数，主要用于反序列化时创建空对象
   */
  public GenericCounter() {
    // mostly for readFields
  }

  /**
   * 构造指定名称和显示名称的计数器，初始值为0
   * @param name 计数器内部名称
   * @param displayName 计数器展示名称，用于UI显示
   */
  public GenericCounter(String name, String displayName) {
    this.name = name;
    this.displayName = displayName;
  }

  /**
   * 构造指定名称、显示名称和初始值的计数器
   * @param name 计数器内部名称
   * @param displayName 计数器展示名称，用于UI显示
   * @param value 计数器初始值
   */
  public GenericCounter(String name, String displayName, long value) {
    this.name = name;
    this.displayName = displayName;
    this.value = value;
  }

  @Override @Deprecated
  public synchronized void setDisplayName(String displayName) {
    this.displayName = displayName;
  }

  @Override
  public synchronized void readFields(DataInput in) throws IOException {
    // 从输入流读取计数器名称，使用弱引用字符串 intern 减少内存占用
    name = StringInterner.weakIntern(Text.readString(in));
    // 读取是否有独立显示名称，有则读取，否则复用名称
    displayName = in.readBoolean() ? 
        StringInterner.weakIntern(Text.readString(in)) : name;
    // 读取可变长编码的计数器当前值
    value = WritableUtils.readVLong(in);
  }

  /**
   * GenericCounter ::= keyName isDistinctDisplayName [displayName] value
   */
  @Override
  public synchronized void write(DataOutput out) throws IOException {
    // 写入计数器名称
    Text.writeString(out, name);
    // 判断显示名称是否和内部名称不同，写入标记位
    boolean distinctDisplayName = ! name.equals(displayName);
    out.writeBoolean(distinctDisplayName);
    // 如果不同，写入显示名称
    if (distinctDisplayName) {
      Text.writeString(out, displayName);
    }
    // 写入可变长编码的计数器当前值
    WritableUtils.writeVLong(out, value);
  }

  @Override
  public synchronized String getName() {
    return name;
  }

  @Override
  public synchronized String getDisplayName() {
    return displayName;
  }

  @Override
  public synchronized long getValue() {
    return value;
  }

  @Override
  public synchronized void setValue(long value) {
    this.value = value;
  }

  @Override
  public synchronized void increment(long incr) {
    value += incr;
  }

  @Override
  public Counter getUnderlyingCounter() {
    return this;
  }
}