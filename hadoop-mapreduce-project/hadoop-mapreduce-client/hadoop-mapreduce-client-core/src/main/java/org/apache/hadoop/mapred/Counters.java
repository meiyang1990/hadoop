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

import static org.apache.hadoop.mapreduce.util.CountersStrings.parseEscapedCompactString;
import static org.apache.hadoop.mapreduce.util.CountersStrings.toEscapedCompactString;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.ParseException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.commons.collections4.IteratorUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.mapreduce.FileSystemCounter;
import org.apache.hadoop.mapreduce.counters.AbstractCounterGroup;
import org.apache.hadoop.mapreduce.counters.AbstractCounters;
import org.apache.hadoop.mapreduce.counters.CounterGroupBase;
import org.apache.hadoop.mapreduce.counters.CounterGroupFactory;
import org.apache.hadoop.mapreduce.counters.FileSystemCounterGroup;
import org.apache.hadoop.mapreduce.counters.FrameworkCounterGroup;
import org.apache.hadoop.mapreduce.counters.GenericCounter;
import org.apache.hadoop.mapreduce.counters.Limits;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormatCounter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormatCounter;
import org.slf4j.Logger;

import org.apache.hadoop.thirdparty.com.google.common.collect.Iterators;

/**
 * 文件级注释：MapReduce旧API的计数器集合容器，管理全局计数器，计数器可由MapReduce框架或应用程序自定义，按分组组织
 * A set of named counters.
 *
 * <p><code>Counters</code> represent global counters, defined either by the
 * Map-Reduce framework or applications. Each <code>Counter</code> can be of
 * any {@link Enum} type.</p>
 *
 * <p><code>Counters</code> are bunched into {@link Group}s, each comprising of
 * counters from a particular <code>Enum</code> class.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class Counters
    extends AbstractCounters<Counters.Counter, Counters.Group> {
  
  public static final int MAX_COUNTER_LIMIT = Limits.getCountersMax();
  public static final int MAX_GROUP_LIMIT = Limits.getGroupsMax();
  // 存储已废弃计数器分组到新分组的映射
  private static final HashMap<String, String> depricatedCounterMap =
      new HashMap<String, String>();
  
  static {
    initDepricatedMap();
  }
  
  /**
   * 构造空计数器集合，使用默认分组工厂
   */
  public Counters() {
    super(groupFactory);
  }

  /**
   * 从新API的Counters构造旧API的Counters实例
   * @param newCounters 新API的计数器集合
   */
  public Counters(org.apache.hadoop.mapreduce.Counters newCounters) {
    super(newCounters, groupFactory);
  }

  @SuppressWarnings({ "deprecation" })
  // 初始化废弃分组到新分组的映射表
  private static void initDepricatedMap() {
    depricatedCounterMap.put(FileInputFormat.Counter.class.getName(),
      FileInputFormatCounter.class.getName());
    depricatedCounterMap.put(FileOutputFormat.Counter.class.getName(),
      FileOutputFormatCounter.class.getName());
    depricatedCounterMap.put(
      org.apache.hadoop.mapreduce.lib.input.FileInputFormat.Counter.class
        .getName(), FileInputFormatCounter.class.getName());
    depricatedCounterMap.put(
      org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.Counter.class
        .getName(), FileOutputFormatCounter.class.getName());
  }

  /**
   * 根据旧分组名获取对应的新分组名
   * @param oldGroup 旧分组名
   * @return 新分组名，如果不存在映射返回null
   */
  private static String getNewGroupKey(String oldGroup) {
    if (depricatedCounterMap.containsKey(oldGroup)) {
      return depricatedCounterMap.get(oldGroup);
    }
    return null;
  }
  
  /**
   * 将新API的Counters转换为旧API的Counters实例，用于兼容旧API
   * @param newCounters 新API的计数器集合
   * @return 转换后的旧API计数器集合
   */
  static Counters downgrade(org.apache.hadoop.mapreduce.Counters newCounters) {
    return new Counters(newCounters);
  }

  /**
   * 根据分组名获取计数器分组
   * @param groupName 分组名称
   * @return 对应的计数器分组
   */
  public synchronized Group getGroup(String groupName) {
    return super.getGroup(groupName);
  }

  /**
   * 获取所有分组名称集合
   * @return 分组名称列表
   */
  @SuppressWarnings("unchecked")
  public synchronized Collection<String> getGroupNames() {
    return IteratorUtils.toList(super.getGroupNames().iterator());
  }

  /**
   * 生成计数器的简洁字符串表示，用于显示
   * @return 所有计数器的简洁拼接字符串
   */
  public synchronized String makeCompactString() {
    StringBuilder builder = new StringBuilder();
    boolean first = true;
    // 遍历所有分组
    for(Group group: this){
      // 遍历分组内所有计数器
      for(Counter counter: group) {
        // 添加分隔符
        if (first) {
          first = false;
        } else {
          builder.append(',');
        }
        // 拼接分组名.计数器名:值格式
        builder.append(group.getDisplayName());
        builder.append('.');
        builder.append(counter.getDisplayName());
        builder.append(':');
        builder.append(counter.getCounter());
      }
    }
    return builder.toString();
  }
  
  /**
   * 单个计数器实现，保存计数器名称和计数值，兼容旧API，包装新API计数器实现
   * A counter record, comprising its name and value.
   */
  @InterfaceAudience.Public
  @InterfaceStability.Stable
  public static class Counter implements org.apache.hadoop.mapreduce.Counter {
    // 底层实际使用的新API计数器实例
    org.apache.hadoop.mapreduce.Counter realCounter;

    /**
     * 构造包装指定新API计数器的旧API计数器实例
     * @param counter 新API计数器实例
     */
    Counter(org.apache.hadoop.mapreduce.Counter counter) {
      this.realCounter = counter;
    }

    /**
     * 构造空计数器实例，使用GenericCounter作为底层实现
     */
    public Counter() {
      this(new GenericCounter());
    }

    @SuppressWarnings("deprecation")
    @Override
    public void setDisplayName(String displayName) {
      realCounter.setDisplayName(displayName);
    }

    @Override
    public String getName() {
      return realCounter.getName();
    }

    @Override
    public String getDisplayName() {
      return realCounter.getDisplayName();
    }

    @Override
    public long getValue() {
      return realCounter.getValue();
    }

    @Override
    public void setValue(long value) {
      realCounter.setValue(value);
    }

    @Override
    public void increment(long incr) {
      realCounter.increment(incr);
    }

    @Override
    public void write(DataOutput out) throws IOException {
      realCounter.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      realCounter.readFields(in);
    }

    /**
     * Returns the compact stringified version of the counter in the format
     * [(actual-name)(display-name)(value)]
     * @return the stringified result
     */
    public String makeEscapedCompactString() {
      return toEscapedCompactString(realCounter);
    }

    /**
     * Checks for (content) equality of two (basic) counters
     * @param counter to compare
     * @return true if content equals
     * @deprecated
     */
    @Deprecated
    public boolean contentEquals(Counter counter) {
      return realCounter.equals(counter.getUnderlyingCounter());
    }

    /**
     * 获取计数器当前值，旧API兼容方法
     * @return 计数器当前值
     */
    public long getCounter() {
      return realCounter.getValue();
    }

    @Override
    public org.apache.hadoop.mapreduce.Counter getUnderlyingCounter() {
      return realCounter;
    }
    
    @Override
    public synchronized boolean equals(Object genericRight) {
      if (genericRight instanceof Counter) {
        synchronized (genericRight) {
          Counter right = (Counter) genericRight;
          return getName().equals(right.getName()) &&
                 getDisplayName().equals(right.getDisplayName()) &&
                 getValue() == right.getValue();
        }
      }
      return false;
    }
    
    @Override
    public int hashCode() {
      return realCounter.hashCode();
    }
  }


  /**
   * 计数器分组，同一枚举类的计数器归属同一个分组，兼容旧API，包装新API分组实现
   *  <code>Group</code> of counters, comprising of counters from a particular
   *  counter {@link Enum} class.
   *
   *  <p><code>Group</code>handles localization of the class name and the
   *  counter names.</p>
   */
  @InterfaceAudience.Public
  @InterfaceStability.Stable
  public static class Group implements CounterGroupBase<Counter> {
    // 底层实际使用的新API分组实例
    private CounterGroupBase<Counter> realGroup;
    
    protected Group() {
      realGroup = null;
    }
    
    Group(GenericGroup group) {
      this.realGroup = group;
    }
    Group(FSGroupImpl group) {
      this.realGroup = group;
    }
    
    @SuppressWarnings({ "unchecked", "rawtypes" })
    Group(FrameworkGroupImpl group) {
      this.realGroup = group;
    }
    
    /**
     * 获取分组内指定名称计数器的值，不存在则返回0
     * @param counterName 计数器名称
     * @return 计数器值，不存在返回0
     */
    public long getCounter(String counterName)  {
      return getCounterValue(realGroup, counterName);
    }

    /**
     * 生成分组的转义紧凑字符串表示
     * @return 分组转义紧凑字符串
     */
    public String makeEscapedCompactString() {
      return toEscapedCompactString(realGroup);
    }

    /**
     * Get the counter for the given id and create it if it doesn't exist.
     * @param id the numeric id of the counter within the group
     * @param name the internal counter name
     * @return the counter
     * @deprecated use {@link #findCounter(String)} instead
     */
    @Deprecated
    public Counter getCounter(int id, String name) {
      return findCounter(name);
    }

    /**
     * Get the counter for the given name and create it if it doesn't exist.
     * @param name the internal counter name
     * @return the counter
     */
    public Counter getCounterForName(String name) {
      return findCounter(name);
    }

    @Override
    public void write(DataOutput out) throws IOException {
     realGroup.write(out); 
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      realGroup.readFields(in);
    }

    @Override
    public Iterator<Counter> iterator() {
      return realGroup.iterator();
    }

    @Override
    public String getName() {
      return realGroup.getName();
    }

    @Override
    public String getDisplayName() {
      return realGroup.getDisplayName();
    }

    @Override
    public void setDisplayName(String displayName) {
      realGroup.setDisplayName(displayName);
    }

    @Override
    public void addCounter(Counter counter) {
      realGroup.addCounter(counter);
    }

    @Override
    public Counter addCounter(String name, String displayName, long value) {
      return realGroup.addCounter(name, displayName, value);
    }

    @Override
    public Counter findCounter(String counterName, String displayName) {
      return realGroup.findCounter(counterName, displayName);
    }

    @Override
    public Counter findCounter(String counterName, boolean create) {
      return realGroup.findCounter(counterName, create);
    }

    @Override
    public Counter findCounter(String counterName) {
      return realGroup.findCounter(counterName);
    }

    @Override
    public int size() {
      return realGroup.size();
    }

    @Override
    public void incrAllCounters(CounterGroupBase<Counter> rightGroup) {
      realGroup.incrAllCounters(rightGroup);
    }
    
    @Override
    public CounterGroupBase<Counter> getUnderlyingGroup() {
      return realGroup;
    }

    @Override
    public synchronized boolean equals(Object genericRight) {
      if (genericRight instanceof CounterGroupBase<?>) {
        @SuppressWarnings("unchecked")
        CounterGroupBase<Counter> right = ((CounterGroupBase<Counter>) 
        genericRight).getUnderlyingGroup();
        return Iterators.elementsEqual(iterator(), right.iterator());
      }
      return false;
    }

    @Override
    public int hashCode() {
      return realGroup.hashCode();
    }
  }

  // 获取指定分组中指定名称计数器的值，不存在返回0，供旧分组接口使用
  static long getCounterValue(CounterGroupBase<Counter> group, String counterName) {
    Counter counter = group.findCounter(counterName, false);
    if (counter != null) return counter.getValue();
    return 0L;
  }

  /**
   * 通用计数器分组实现，适配旧API分组接口
   */
  private static class GenericGroup extends AbstractCounterGroup<Counter> {

    GenericGroup(String name, String displayName, Limits limits) {
      super(name, displayName, limits);
    }

    @Override
    protected Counter newCounter(String counterName, String displayName,
                                 long value) {
      return new Counter(new GenericCounter(counterName, displayName, value));
    }

    @Override
    protected Counter newCounter() {
      return new Counter();
    }
    
    @Override
    public CounterGroupBase<Counter> getUnderlyingGroup() {
     return this;
    }
  }

  /**
   * 框架枚举计数器分组实现，适配旧API分组接口
   */
  private static class FrameworkGroupImpl<T extends Enum<T>>
      extends FrameworkCounterGroup<T, Counter> {

    FrameworkGroupImpl(Class<T> cls) {
      super(cls);
    }

    @Override
    protected Counter newCounter(T key) {
      return new Counter(new FrameworkCounter<T>(key, getName()));
    }

    @Override
    public CounterGroupBase<Counter> getUnderlyingGroup() {
      return this;
    }
  }

  /**
   * 文件系统计数器分组实现，适配旧API分组接口
   */
  private static class FSGroupImpl extends FileSystemCounterGroup<Counter> {

    @Override
    protected Counter newCounter(String scheme, FileSystemCounter key) {
      return new Counter(new FSCounter(scheme, key));
    }

    @Override
    public CounterGroupBase<Counter> getUnderlyingGroup() {
      return this;
    }
  }

  /**
   * 根据分组名和计数器名查找计数器，处理废弃分组名的兼容
   * @param group 分组名
   * @param name 计数器名
   * @return 找到的计数器，不存在则新建返回
   */
  public synchronized Counter findCounter(String group, String name) {
    // 处理废弃的MAP_INPUT_BYTES计数器名兼容
    if (name.equals("MAP_INPUT_BYTES")) {
      LOG.warn("Counter name MAP_INPUT_BYTES is deprecated. " +
               "Use FileInputFormatCounters as group name and " +
               " BYTES_READ as counter name instead");
      return findCounter(FileInputFormatCounter.BYTES_READ);
    }
    // 转换废弃分组名到新分组名
    String newGroupKey = getNewGroupKey(group);
    if (newGroupKey != null) {
      group = newGroupKey;
    }
    return getGroup(group).getCounterForName(name);
  }

  /**
   * 计数器分组工厂，为旧API创建对应类型的分组实例
   * Provide factory methods for counter group factory implementation.
   * See also the GroupFactory in
   *  {@link org.apache.hadoop.mapreduce.Counters mapreduce.Counters}
   */
  static class GroupFactory extends CounterGroupFactory<Counter, Group> {

    @Override
    protected <T extends Enum<T>>
    FrameworkGroupFactory<Group> newFrameworkGroupFactory(final Class<T> cls) {
      return new FrameworkGroupFactory<Group>() {
        @Override public Group newGroup(String name) {
          return new Group(new FrameworkGroupImpl<T>(