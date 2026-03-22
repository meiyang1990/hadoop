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
import java.util.Arrays;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.thirdparty.com.google.common.base.Joiner;
import static org.apache.hadoop.util.Preconditions.checkNotNull;
import org.apache.hadoop.thirdparty.com.google.common.collect.AbstractIterator;
import org.apache.hadoop.thirdparty.com.google.common.collect.Iterators;
import org.apache.hadoop.thirdparty.com.google.common.collect.Maps;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.FileSystemCounter;
import org.apache.hadoop.mapreduce.util.ResourceBundles;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 文件系统计数器组的抽象基类，同时兼容mapred和mapreduce包的公共实现，
 * 用于按文件系统scheme分类统计各文件系统的IO操作指标。
 * 
 * @param <C> 组内计数器的具体类型
 */
@InterfaceAudience.Private
public abstract class FileSystemCounterGroup<C extends Counter>
    implements CounterGroupBase<C> {

  // 允许的最大文件系统scheme数量，用于边界检查
  static final int MAX_NUM_SCHEMES = 100; // intern/sanity check
  // 存储全局唯一化后的scheme字符串，减少内存占用
  static final ConcurrentMap<String, String> schemes = Maps.newConcurrentMap();
  
  private static final Logger LOG =
      LoggerFactory.getLogger(FileSystemCounterGroup.class);

  // 延迟初始化计数器存储，避免空组占用过多内存
  // key：文件系统scheme，value：对应scheme的所有计数器数组，按枚举ordinal索引存储
  private Map<String, Object[]> map;
  // 计数器组的显示名称
  private String displayName;

  private static final Joiner NAME_JOINER = Joiner.on('_');
  private static final Joiner DISP_JOINER = Joiner.on(": ");

  /**
   * 文件系统计数器具体实现，存储单个scheme下单个指标的计数值。
   */
  @InterfaceAudience.Private
  public static class FSCounter extends AbstractCounter {
    // 所属文件系统scheme
    final String scheme;
    // 计数器对应的指标类型
    final FileSystemCounter key;
    // 当前计数值
    private long value;

    /**
     * 构造文件系统计数器实例。
     * @param scheme 文件系统scheme
     * @param ref 计数器指标类型
     */
    public FSCounter(String scheme, FileSystemCounter ref) {
      this.scheme = scheme;
      key = ref;
    }
    
    /**
     * 获取计数器所属文件系统scheme。
     * @return 文件系统scheme
     */
    @Private
    public String getScheme() {
      return scheme;
    }
    
    /**
     * 获取计数器对应的文件系统指标类型。
     * @return 文件系统计数器枚举
     */
    @Private
    public FileSystemCounter getFileSystemCounter() {
      return key;
    }

    @Override
    public String getName() {
      return NAME_JOINER.join(scheme, key.name());
    }

    @Override
    public String getDisplayName() {
      return DISP_JOINER.join(scheme, localizeCounterName(key.name()));
    }

    /**
     * 从资源包获取本地化的计数器名称。
     * @param counterName 计数器原始名称
     * @return 本地化后的显示名称
     */
    protected String localizeCounterName(String counterName) {
      return ResourceBundles.getCounterName(FileSystemCounter.class.getName(),
                                            counterName, counterName);
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
      value += incr;
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

  @Override
  public String getName() {
    return FileSystemCounter.class.getName();
  }

  @Override
  public String getDisplayName() {
    if (displayName == null) {
      displayName = ResourceBundles.getCounterGroupName(getName(),
          "File System Counters");
    }
    return displayName;
  }

  @Override
  public void setDisplayName(String displayName) {
    this.displayName = displayName;
  }

  @Override
  public void addCounter(C counter) {
    C ours;
    if (counter instanceof FileSystemCounterGroup.FSCounter) {
      FSCounter c = (FSCounter) counter;
      ours = findCounter(c.scheme, c.key);
    }
    else {
      ours = findCounter(counter.getName());
    }
    if (ours != null) {
      ours.setValue(counter.getValue());
    }
  }

  @Override
  public C addCounter(String name, String displayName, long value) {
    C counter = findCounter(name);
    if (counter != null) {
      counter.setValue(value);
    }
    return counter;
  }

  /**
   * 解析通用计数器名称，拆分为scheme和计数器key两部分。
   * @param counterName 格式为 scheme_counterName 的计数器全名
   * @return 拆分后的数组，[0]为scheme，[1]为计数器key
   */
  private String[] parseCounterName(String counterName) {
    int schemeEnd = counterName.indexOf('_');
    if (schemeEnd < 0) {
      throw new IllegalArgumentException("bad fs counter name");
    }
    return new String[]{counterName.substring(0, schemeEnd),
                        counterName.substring(schemeEnd + 1)};
  }

  @Override
  public C findCounter(String counterName, String displayName) {
    return findCounter(counterName);
  }

  @Override
  public C findCounter(String counterName, boolean create) {
    try {
      String[] pair = parseCounterName(counterName);
      return findCounter(pair[0], FileSystemCounter.valueOf(pair[1]));
    }
    catch (Exception e) {
      if (create) throw new IllegalArgumentException(e);
      LOG.warn(counterName + " is not a recognized counter.");
      return null;
    }
  }

  @Override
  public C findCounter(String counterName) {
    return findCounter(counterName, false);
  }

  /**
   * 根据文件系统scheme和计数器key查找或创建计数器。
   * @param scheme 文件系统scheme
   * @param key 文件系统计数器枚举
   * @return 对应计数器实例
   */
  @SuppressWarnings("unchecked")
  public synchronized C findCounter(String scheme, FileSystemCounter key) {
    final String canonicalScheme = checkScheme(scheme);
    if (map == null) {
      map = new ConcurrentSkipListMap<>();
    }
    Object[] counters = map.get(canonicalScheme);
    int ord = key.ordinal();
    if (counters == null) {
      counters = new Object[FileSystemCounter.values().length];
      map.put(canonicalScheme, counters);
      counters[ord] = newCounter(canonicalScheme, key);
    }
    else if (counters[ord] == null) {
      counters[ord] = newCounter(canonicalScheme, key);
    }
    return (C) counters[ord];
  }

  /**
   * 校验并标准化文件系统scheme，限制最大scheme数量。
   * @param scheme 原始scheme字符串
   * @return 标准化后的全局唯一scheme
   */
  private String checkScheme(String scheme) {
    String fixed = StringUtils.toUpperCase(scheme);
    String interned = schemes.putIfAbsent(fixed, fixed);
    if (schemes.size() > MAX_NUM_SCHEMES) {
      // mistakes or abuses
      throw new IllegalArgumentException("too many schemes? "+ schemes.size() +
                                         " when process scheme: "+ scheme);
    }
    return interned == null ? fixed : interned;
  }

  /**
   * 抽象工厂方法，由子类实现创建具体类型的计数器。
   * @param scheme 文件系统scheme
   * @param key 计数器枚举key
   * @return 新的计数器实例
   */
  protected abstract C newCounter(String scheme, FileSystemCounter key);

  @Override
  public synchronized int size() {
    int n = 0;
    if (map != null) {
      for (Object[] counters : map.values()) {
        n += numSetCounters(counters);
      }
    }
    return n;
  }

  @Override
  @SuppressWarnings("unchecked")
  public void incrAllCounters(CounterGroupBase<C> other) {
    if (checkNotNull(other.getUnderlyingGroup(), "other group")
        instanceof FileSystemCounterGroup<?>) {
      for (Counter counter : other) {
        FSCounter c = (FSCounter) ((Counter)counter).getUnderlyingCounter();
        findCounter(c.scheme, c.key) .increment(counter.getValue());
      }
    }
  }

  /**
   * 序列化文件系统计数器组到输出流。
   * 格式：#scheme (scheme #counter (key value)*)*
   */
  @Override
  public synchronized void write(DataOutput out) throws IOException {
    if (map != null) {
      WritableUtils.writeVInt(out, map.size()); // 写入scheme数量
      for (Map.Entry<String, Object[]> entry : map.entrySet()) {
        WritableUtils.writeString(out, entry.getKey()); // 写入scheme名称
        // 写入当前scheme已设置的计数器数量
        WritableUtils.writeVInt(out, numSetCounters(entry.getValue()));
        for (Object counter : entry.getValue()) {
          if (counter == null) continue;
          @SuppressWarnings("unchecked")
          FSCounter c = (FSCounter) ((Counter) counter).getUnderlyingCounter();
          WritableUtils.writeVInt(out, c.key.ordinal());  // 写入计数器key序号
          WritableUtils.writeVLong(out, c.getValue());    // 写入计数器值
        }
      }
    } else {
      WritableUtils.writeVInt(out, 0);
    }
  }

  /**
   * 统计数组中非空计数器的数量。
   * @param counters 计数器数组
   * @return 非空计数器数量
   */
  private int numSetCounters(Object[] counters) {
    int n = 0;
    for (Object counter : counters) if (counter != null) ++n;
    return n;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    int numSchemes = WritableUtils.readVInt(in);    // 读取scheme数量
    FileSystemCounter[] enums = FileSystemCounter.values();
    for (int i = 0; i < numSchemes; ++i) {
      String scheme = WritableUtils.readString(in); // 读取scheme名称
      int numCounters = WritableUtils.readVInt(in); // 读取计数器数量
      for (int j = 0; j < numCounters; ++j) {
        findCounter(scheme, enums[WritableUtils.readVInt(in)])  // 根据序号获取counter
            .setValue(WritableUtils.readVLong(in)); // 设置计数值
      }
    }
  }

  @Override
  public Iterator<C> iterator() {
    return new AbstractIterator<C>() {
      Iterator<Object[]> it = map != null ? map.values().iterator() : null;
      Object[] counters = (it != null && it.hasNext()) ? it.next() : null;
      int i = 0;
      @Override
      protected C computeNext() {
        while (counters != null) {
          while (i < counters.length) {
            @SuppressWarnings("unchecked")
            C counter = (C) counters[i++];
            if (counter != null) return counter;
          }
          i = 0;
          counters = (it != null && it.hasNext()) ? it.next() : null;
        }
        return endOfData();
      }
    };
  }

  @Override
  public synchronized boolean equals(Object genericRight) {
    if (genericRight instanceof CounterGroupBase<?>) {
      @SuppressWarnings("unchecked")
      CounterGroupBase<C> right = (CounterGroupBase<C>) genericRight;
      return Iterators.elementsEqual(iterator(), right.iterator());
    }
    return false;
  }

  @Override
  public synchronized int hashCode() {
    // 深度计算哈希，因为计数器存储在数组中
    int hash = FileSystemCounter.class.hashCode();
    if (map != null) {
      for (Object[] counters : map.values()) {
        if (counters != null) hash ^= Arrays.hashCode(counters);
      }
    }
    return hash;
  }
}