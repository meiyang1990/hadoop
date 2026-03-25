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

import static org.apache.hadoop.mapreduce.counters.CounterGroupFactory.getFrameworkGroupId;
import static org.apache.hadoop.mapreduce.counters.CounterGroupFactory.isFrameworkGroup;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.FileSystemCounter;
import org.apache.hadoop.mapreduce.JobCounter;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.apache.hadoop.util.StringInterner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.thirdparty.com.google.common.collect.Iterables;
import org.apache.hadoop.thirdparty.com.google.common.collect.Iterators;
import org.apache.hadoop.thirdparty.com.google.common.collect.Maps;

/**
 * 计数器容器抽象基类，为mapred和mapreduce包提供通用的计数器实现
 * 
 * 负责对不同类型的计数器分组进行统一管理，支持框架内置计数器、文件系统计数器和用户自定义计数器
 *
 * @param <C> 容器内计数器类型
 * @param <G> 容器内计数器分组类型
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public abstract class AbstractCounters<C extends Counter,
                                       G extends CounterGroupBase<C>>
    implements Writable, Iterable<G> {

  /** 日志对象，打印计数器相关日志 */
  protected static final Logger LOG =
      LoggerFactory.getLogger("mapreduce.Counters");

  /** 枚举类型到对应计数器的缓存，避免重复查找 */
  private final Map<Enum<?>, C> cache = Maps.newIdentityHashMap();
  /** 框架和文件系统计数器分组存储，按名称排序 */
  private final Map<String, G> fgroups = new ConcurrentSkipListMap<String, G>();
  /** 用户自定义计数器分组存储，按名称排序 */
  private final Map<String, G> groups = new ConcurrentSkipListMap<String, G>();
  /** 计数器分组工厂，用于创建不同类型的分组实例 */
  private final CounterGroupFactory<C, G> groupFactory;

  /** 分组类型枚举，用于框架计数器序列化，避免序列化全名字符串 */
  // For framework counter serialization without strings
  enum GroupType { FRAMEWORK, FILESYSTEM };

  /** 是否序列化所有计数器，false时仅序列化框架和文件系统计数器 */
  // Writes only framework and fs counters if false.
  private boolean writeAllCounters = true;

  /** 旧分组名到新分组名的映射，用于兼容旧版本API */
  private static final Map<String, String> legacyMap = Maps.newHashMap();
  static {
    // 兼容旧版Task计数器枚举命名
    legacyMap.put("org.apache.hadoop.mapred.Task$Counter",
                  TaskCounter.class.getName());
    // 兼容旧版Job计数器枚举命名
    legacyMap.put("org.apache.hadoop.mapred.JobInProgress$Counter",
                  JobCounter.class.getName());
    // 兼容旧版文件系统计数器分组命名
    legacyMap.put("FileSystemCounters", FileSystemCounter.class.getName());
  }

  /** 计数器数量限制对象，防止恶意创建过多计数器占用内存 */
  private final Limits limits = new Limits();

  /**
   * 构造方法，使用指定的分组工厂创建计数器容器
   * @param gf 计数器分组工厂
   */
  @InterfaceAudience.Private
  public AbstractCounters(CounterGroupFactory<C, G> gf) {
    groupFactory = gf;
  }

  /**
   * 拷贝构造方法，从另一个计数器对象复制所有分组和计数器
   * @param <C1> 源对象计数器类型
   * @param <G1> 源对象分组类型
   * @param counters 源计数器对象，用于复制
   * @param groupFactory 当前容器使用的分组工厂
   */
  @InterfaceAudience.Private
  public <C1 extends Counter, G1 extends CounterGroupBase<C1>>
  AbstractCounters(AbstractCounters<C1, G1> counters,
                   CounterGroupFactory<C, G> groupFactory) {
    this.groupFactory = groupFactory;
    for(G1 group: counters) {
      String name = group.getName();
      G newGroup = groupFactory.newGroup(name, group.getDisplayName(), limits);
      (isFrameworkGroup(name) ? fgroups : groups).put(name, newGroup);
      for(Counter counter: group) {
        newGroup.addCounter(counter.getName(), counter.getDisplayName(),
                            counter.getValue());
      }
    }
  }

  /**
   * 添加一个已构造的分组到容器
   * @param group 要添加的分组对象
   * @return 添加后的分组对象
   */
  @InterfaceAudience.Private
  public synchronized G addGroup(G group) {
    String name = group.getName();
    if (isFrameworkGroup(name)) {
      fgroups.put(name, group);
    } else {
      limits.checkGroups(groups.size() + 1);
      groups.put(name, group);
    }
    return group;
  }

  /**
   * 根据名称和显示名称创建并添加新分组
   * @param name 分组名称
   * @param displayName 分组显示名称
   * @return 新建的分组对象
   */
  @InterfaceAudience.Private
  public G addGroup(String name, String displayName) {
    return addGroup(groupFactory.newGroup(name, displayName, limits));
  }

  /**
   * 根据分组名和计数器名查找计数器，不存在则创建新计数器
   * @param groupName 计数器所属分组名称
   * @param counterName 计数器名称
   * @return 匹配的计数器对象
   */
  public C findCounter(String groupName, String counterName) {
    G grp = getGroup(groupName);
    return grp.findCounter(counterName);
  }

  /**
   * 根据枚举查找计数器，同一个枚举始终返回同一个计数器，结果缓存
   * @param key 计数器对应的枚举键
   * @return 匹配的计数器对象
   */
  public synchronized C findCounter(Enum<?> key) {
    C counter = cache.get(key);
    if (counter == null) {
      counter = findCounter(key.getDeclaringClass().getName(), key.name());
      cache.put(key, counter);
    }
    return counter;
  }

  /**
   * 根据文件系统scheme和文件系统计数器枚举查找对应计数器
   * @param scheme 文件系统scheme（如hdfs、s3等）
   * @param key 文件系统计数器枚举
   * @return 对应文件系统的计数器对象
   */
  @InterfaceAudience.Private
  public synchronized C findCounter(String scheme, FileSystemCounter key) {
    return ((FileSystemCounterGroup<C>) getGroup(
        FileSystemCounter.class.getName()).getUnderlyingGroup()).
        findCounter(scheme, key);
  }

  /**
   * 获取所有分组名称的可迭代对象，包含兼容旧版本的废弃分组名
   * @return 所有分组名称的可迭代对象
   */
  public synchronized Iterable<String> getGroupNames() {
    HashSet<String> deprecated = new HashSet<String>();
    // 检查所有旧分组名，对应新分组存在的话，添加旧名到结果保持兼容性
    for(Map.Entry<String, String> entry : legacyMap.entrySet()) {
      String newGroup = entry.getValue();
      boolean isFGroup = isFrameworkGroup(newGroup);
      if(isFGroup ? fgroups.containsKey(newGroup) : groups.containsKey(newGroup)) {
        deprecated.add(entry.getKey());
      }
    }
    return Iterables.concat(fgroups.keySet(), groups.keySet(), deprecated);
  }

  @Override
  public Iterator<G> iterator() {
    // 拼接框架分组和自定义分组的迭代器
    return Iterators.concat(fgroups.values().iterator(),
                            groups.values().iterator());
  }

  /**
   * 根据分组名称获取分组对象，不存在则创建新的空分组
   * @param groupName 分组名称
   * @return 对应分组对象
   */
  public synchronized G getGroup(String groupName) {

    // 处理旧分组名兼容
    boolean groupNameInLegacyMap = true;
    String newGroupName = legacyMap.get(groupName);
    if (newGroupName == null) {
      groupNameInLegacyMap = false;
      newGroupName = Limits.filterGroupName(groupName);
    }

    // 从对应存储获取分组
    boolean isFGroup = isFrameworkGroup(newGroupName);
    G group = isFGroup ? fgroups.get(newGroupName) : groups.get(newGroupName);
    if (group == null) {
      // 分组不存在，新建分组并添加到对应存储
      group = groupFactory.newGroup(newGroupName, limits);
      if (isFGroup) {
        fgroups.put(newGroupName, group);
      } else {
        limits.checkGroups(groups.size() + 1);
        groups.put(newGroupName, group);
      }
      // 如果是旧分组名，打印弃用警告
      if (groupNameInLegacyMap) {
        LOG.warn("Group " + groupName + " is deprecated. Use " + newGroupName
            + " instead");
      }
    }
    return group;
  }

  /**
   * 统计所有分组中计数器总数
   * @return 所有分组的计数器总数量
   */
  public synchronized int countCounters() {
    int result = 0;
    for (G group : this) {
      result += group.size();
    }
    return result;
  }

  /**
   * 将所有计数器序列化输出到DataOutput
   * 格式：版本号 -> 框架分组数量 -> (分组类型 分组信息 分组数据)* -> 自定义分组数量 -> (分组名 分组数据)*
   */
  @Override
  public synchronized void write(DataOutput out) throws IOException {
    WritableUtils.writeVInt(out, groupFactory.version());
    // 先输出框架计数器分组
    WritableUtils.writeVInt(out, fgroups.size());
    for (G group : fgroups.values()) {
      // 按分组类型序列化
      if (group.getUnderlyingGroup() instanceof FrameworkCounterGroup<?, ?>) {
        WritableUtils.writeVInt(out, GroupType.FRAMEWORK.ordinal());
        WritableUtils.writeVInt(out, getFrameworkGroupId(group.getName()));
        group.write(out);
      } else if (group.getUnderlyingGroup() instanceof FileSystemCounterGroup<?>) {
        WritableUtils.writeVInt(out, GroupType.FILESYSTEM.ordinal());
        group.write(out);
      }
    }
    if (writeAllCounters) {
      // 输出用户自定义分组
      WritableUtils.writeVInt(out, groups.size());
      for (G group : groups.values()) {
        Text.writeString(out, group.getName());
        group.write(out);
      }
    } else {
      // 不输出自定义分组，写0个
      WritableUtils.writeVInt(out, 0);
    }
  }

  @Override
  public synchronized void readFields(DataInput in) throws IOException {
    int version = WritableUtils.readVInt(in);
    // 校验版本一致性
    if (version != groupFactory.version()) {
      throw new IOException("Counters version mismatch, expected "+
          groupFactory.version() +" got "+ version);
    }
    int numFGroups = WritableUtils.readVInt(in);
    fgroups.clear();
    GroupType[] groupTypes = GroupType.values();
    // 反序列化框架分组
    while (numFGroups-- > 0) {
      // 根据分组类型创建对应分组实例
      GroupType groupType = groupTypes[WritableUtils.readVInt(in)];
      G group;
      switch (groupType) {
        case FILESYSTEM: // 文件系统分组，不需要额外id
          group = groupFactory.newFileSystemGroup();
          break;
        case FRAMEWORK:  // 框架分组，需要读取分组id
          group = groupFactory.newFrameworkGroup(WritableUtils.readVInt(in));
          break;
        default: // 处理未知类型，抛出异常
          throw new IOException("Unexpected counter group type: "+ groupType);
      }
      // 反序列化分组内容并存入存储
      group.readFields(in);
      fgroups.put(group.getName(), group);
    }
    // 反序列化用户自定义分组
    int numGroups = WritableUtils.readVInt(in);
    while (numGroups-- > 0) {
      limits.checkGroups(groups.size() + 1);
      // 使用字符串驻留节省内存
      G group = groupFactory.newGenericGroup(
          StringInterner.weakIntern(Text.readString(in)), null, limits);
      group.readFields(in);
      groups.put(group.getName(), group);
    }
  }

  /**
   * 将所有计数器转换为可读文本格式
   * @return 计数器的文本描述字符串
   */
  @Override
  public synchronized String toString() {
    StringBuilder sb = new StringBuilder("Counters: " + countCounters());
    for (G group: this) {
      sb.append("\n\t").append(group.getDisplayName());
      for (Counter counter: group) {
        sb.append("\n\t\t").append(counter.getDisplayName()).append("=")
          .append(counter.getValue());
      }
    }
    return sb.toString();
  }

  /**
   * 将另一个计数器容器中的所有计数器值增量累加到当前容器中
   * @param other 提供增量值的另一个计数器容器
   */
  public synchronized void incrAllCounters(AbstractCounters<C, G> other) {
    for(G right : other) {
      String groupName = right.getName();
      G left = (isFrameworkGroup(groupName) ? fgroups : groups).get(groupName);
      if (left == null) {
        left = addGroup(groupName, right.getDisplayName());
      }
      left.incrAllCounters(right);
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public boolean equals(Object genericRight) {
    if (genericRight instanceof AbstractCounters<?, ?>) {
      // 通过迭代器逐个比较所有分组内容是否相等
      return Iterators.elementsEqual(iterator(),
          ((AbstractCounters<C, G>)genericRight).iterator());
    }
    return false;
  }

  @Override
  public int hashCode() {
    return groups.hashCode();
  }

  /**
   * 设置是否序列化所有计数器标志
   * @param send true序列化所有，false仅序列化框架和文件系统计数器
   */
  @InterfaceAudience.Private
  public void setWriteAllCounters(boolean send) {
    writeAllCounters = send;
  }

  /**
   * 获取是否序列化所有计数器标志
   * @return true需要序列化所有计数器，false仅序列化框架计数器
   */
  @InterfaceAudience.Private
  public boolean getWriteAllCounters() {
    return writeAllCounters;
  }

  /**
   * 获取当前容器的计数器数量限制对象
   * @return 限制对象实例
   */
  @InterfaceAudience.Private
  public Limits limits() {
    return limits;
  }
}