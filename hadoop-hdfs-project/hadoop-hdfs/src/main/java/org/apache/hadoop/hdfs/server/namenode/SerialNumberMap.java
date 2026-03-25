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
package org.apache.hadoop.hdfs.server.namenode;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.classification.InterfaceAudience;

/**
 * 文件级注释：HDFS NameNode中用于给对象分配唯一递增序列号的双向映射表，支持双向查询
 *
 * 实现对象到序列号的双向映射功能。
 * 
 * <p>可以通过对象查询对应的序列号，如果对象不存在则自动生成一个自增1的新序列号进行映射。
 * 也支持通过序列号反向查询对应的原始对象。
 * 
 * <p>该映射表是线程安全的，支持并发访问。
 */
@InterfaceAudience.Private
public class SerialNumberMap<T> {
  /** 映射表名称，用于日志和错误信息标识 */
  private String name;
  /** 允许分配的最大序列号 */
  private final int max;
  /** 当前下一个可分配序列号，使用原子类保证并发安全 */
  private final AtomicInteger current = new AtomicInteger(1);
  /** 对象到序列号的正向映射 */
  private final ConcurrentMap<T, Integer> t2i =
      new ConcurrentHashMap<T, Integer>();
  /** 序列号到对象的反向映射 */
  private final ConcurrentMap<Integer, T> i2t =
      new ConcurrentHashMap<Integer, T>();

  /**
   * 从序列号管理器构造映射表
   * @param snm 序列号管理器，提供名称和位长配置
   */
  SerialNumberMap(SerialNumberManager snm) {
    this(snm.name(), snm.getLength());
  }

  /**
   * 构造指定名称和位长的序列号映射表
   * @param name 映射表名称
   * @param bitLength 序列号占用的比特位数，用于计算最大序列号
   */
  SerialNumberMap(String name, int bitLength) {
    this.name = name;
    this.max = (1 << bitLength) - 1;
  }

  /**
   * 获取指定对象对应的序列号，不存在则自动分配新序列号
   * @param t 待获取序列号的对象
   * @return 对象对应的序列号
   */
  public int get(T t) {
    if (t == null) {
      return 0;
    }
    Integer sn = t2i.get(t);
    if (sn == null) {
      synchronized (this) {
        // 双重检查，避免重复分配
        sn = t2i.get(t);
        if (sn == null) {
          // 获取下一个可用序列号并自增
          sn = current.getAndIncrement();
          // 检查是否超过最大序列号限制
          if (sn > max) {
            current.getAndDecrement();
            throw new IllegalStateException(name + ": serial number map is full");
          }
          // 原子插入，处理并发竞争
          Integer old = t2i.putIfAbsent(t, sn);
          if (old != null) {
            // 竞争失败，回退序列号计数，返回已存在的序列号
            current.getAndDecrement();
            return old;
          }
          // 建立反向映射
          i2t.put(sn, t);
        }
      }
    }
    return sn;
  }

  /**
   * 根据序列号获取对应的原始对象
   * @param i 序列号
   * @return 序列号对应的原始对象
   */
  public T get(int i) {
    if (i == 0) {
      return null;
    }
    T t = i2t.get(i);
    if (t == null) {
      throw new IllegalStateException(
          name + ": serial number " + i + " does not exist");
    }
    return t;
  }

  /**
   * 获取当前映射表允许分配的最大序列号
   * @return 最大序列号值
   */
  int getMax() {
    return max;
  }

  /**
   * 获取所有序列号-对象映射条目的拷贝
   * @return 所有映射条目的新HashSet实例
   */
  Set<Map.Entry<Integer, T>> entrySet() {
    return new HashSet<>(i2t.entrySet());
  }

  /**
   * 获取当前映射表中已分配的序列号数量
   * @return 已分配的映射条目数量
   */
  public int size() {
    return i2t.size();
  }

  @Override
  public String toString() {
    return "current=" + current + ",\n" +
           "max=" + max + ",\n  t2i=" + t2i + ",\n  i2t=" + i2t;
  }
}