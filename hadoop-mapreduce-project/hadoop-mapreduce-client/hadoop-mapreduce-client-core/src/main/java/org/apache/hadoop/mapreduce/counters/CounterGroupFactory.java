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

import java.util.List;
import java.util.Map;

import org.apache.hadoop.thirdparty.com.google.common.collect.Maps;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.FileSystemCounter;
import org.apache.hadoop.mapreduce.JobCounter;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.apache.hadoop.mapreduce.util.ResourceBundles;
import org.apache.hadoop.util.Lists;

/**
 * 文件级注释：MapReduce计数器分组工厂抽象基类，为mapred和mapreduce包提供统一的计数器分组创建逻辑，
 * 支持框架内置计数器分组、文件系统计数器分组和用户自定义通用计数器分组的创建，同时提供序列化用的分组ID映射。
 * 
 * 抽象工厂类，定义计数器分组创建的通用逻辑和扩展接口
 * @param <C> 计数器类型
 * @param <G> 计数器分组类型
 */
@InterfaceAudience.Private
public abstract class CounterGroupFactory<C extends Counter,
                                          G extends CounterGroupBase<C>> {

  /**
   * 框架计数器分组工厂接口，定义创建分组的统一方法
   * @param <F> 分组类型
   */
  public interface FrameworkGroupFactory<F> {
    /**
     * 创建指定名称的计数器分组
     * @param name 分组名称
     * @return 新建的计数器分组实例
     */
    F newGroup(String name);
  }

  // 分组名称到序列化ID的映射，用于序列化转换
  private static final Map<String, Integer> s2i = Maps.newHashMap();
  // 序列化ID到分组名称的反向映射，用于反序列化转换
  private static final List<String> i2s = Lists.newArrayList();
  // 工厂版本号，用于序列化兼容性检查
  private static final int VERSION = 1;
  // 文件系统计数器分组的固定名称
  private static final String FS_GROUP_NAME = FileSystemCounter.class.getName();

  // 框架分组名称对应工厂实例的缓存映射
  private final Map<String, FrameworkGroupFactory<G>> fmap = Maps.newHashMap();
  {
    // 注册MapReduce内置框架计数器分组
    addFrameworkGroup(TaskCounter.class);
    addFrameworkGroup(JobCounter.class);
  }

  /**
   * 添加框架计数器分组，更新映射关系并注册分组工厂
   * @param <T> 计数器枚举类型
   * @param cls 计数器枚举类
   */
  private synchronized <T extends Enum<T>>
  void addFrameworkGroup(final Class<T> cls) {
    updateFrameworkGroupMapping(cls);
    fmap.put(cls.getName(), newFrameworkGroupFactory(cls));
  }

  /**
   * 更新框架分组的静态名称-ID映射关系
   * @param cls 计数器枚举类
   */
  private static synchronized void updateFrameworkGroupMapping(Class<?> cls) {
    String name = cls.getName();
    Integer i = s2i.get(name);
    if (i != null) return;
    i2s.add(name);
    s2i.put(name, i2s.size() - 1);
  }

  /**
   * 抽象方法，由子类实现创建具体框架分组工厂
   * @param <T> 计数器枚举类型
   * @param cls 计数器枚举类
   * @return 框架分组工厂实例
   */
  protected abstract <T extends Enum<T>>
  FrameworkGroupFactory<G> newFrameworkGroupFactory(Class<T> cls);

  /**
   * 创建新的计数器分组，自动从资源包获取显示名称
   * @param name 分组名称
   * @param limits 计数器数量限制策略对象
   * @return 新建的计数器分组实例
   */
  public G newGroup(String name, Limits limits) {
    return newGroup(name, ResourceBundles.getCounterGroupName(name, name),
                    limits);
  }

  /**
   * 根据分组类型创建对应类型的计数器分组
   * @param name 分组名称
   * @param displayName 分组显示名称
   * @param limits 计数器数量限制策略对象
   * @return 新建的计数器分组实例
   */
  public G newGroup(String name, String displayName, Limits limits) {
    FrameworkGroupFactory<G> gf = fmap.get(name);
    if (gf != null) return gf.newGroup(name);
    if (name.equals(FS_GROUP_NAME)) {
      return newFileSystemGroup();
    } else if (s2i.get(name) != null) {
      return newFrameworkGroup(s2i.get(name));
    }
    return newGenericGroup(name, displayName, limits);
  }

  /**
   * 根据序列化ID创建框架计数器分组，用于反序列化
   * @param id 分组序列化ID
   * @return 新建的框架计数器分组实例
   */
  public G newFrameworkGroup(int id) {
    String name;
    synchronized(CounterGroupFactory.class) {
      if (id < 0 || id >= i2s.size()) throwBadFrameGroupIdException(id);
      name = i2s.get(id); // should not throw here.
    }
    FrameworkGroupFactory<G> gf = fmap.get(name);
    if (gf == null) throwBadFrameGroupIdException(id);
    return gf.newGroup(name);
  }

  /**
   * 获取框架分组的序列化ID
   * @param name 分组名称
   * @return 分组序列化ID
   */
  public static synchronized int getFrameworkGroupId(String name) {
    Integer i = s2i.get(name);
    if (i == null) throwBadFrameworkGroupNameException(name);
    return i;
  }

  /**
   * 获取计数器工厂版本号，用于序列化兼容性检查
   * @return 工厂版本号
   */
  public int version() {
    return VERSION;
  }

  /**
   * 判断给定名称是否是框架内置分组（包括文件系统分组）
   * @param name 待检查的分组名称
   * @return true 表示是框架分组，false 表示是用户自定义分组
   */
  public static synchronized boolean isFrameworkGroup(String name) {
    return s2i.get(name) != null || name.equals(FS_GROUP_NAME);
  }

  /**
   * 抛出非法框架分组ID异常
   * @param id 非法ID
   */
  private static void throwBadFrameGroupIdException(int id) {
    throw new IllegalArgumentException("bad framework group id: "+ id);
  }

  /**
   * 抛出非法框架分组名称异常
   * @param name 非法名称
   */
  private static void throwBadFrameworkGroupNameException(String name) {
    throw new IllegalArgumentException("bad framework group name: "+ name);
  }

  /**
   * 抽象方法，由子类实现创建用户自定义通用计数器分组
   * @param name 分组名称
   * @param displayName 分组显示名称
   * @param limits 计数器数量限制
   * @return 新建的通用计数器分组实例
   */
  protected abstract G newGenericGroup(String name, String displayName,
                                       Limits limits);

  /**
   * 抽象方法，由子类实现创建文件系统计数器分组
   * @return 新建的文件系统计数器分组实例
   */
  protected abstract G newFileSystemGroup();
}