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
package org.apache.hadoop.mapreduce;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.mapreduce.counters.Limits;
import org.apache.hadoop.mapreduce.counters.GenericCounter;
import org.apache.hadoop.mapreduce.counters.AbstractCounterGroup;
import org.apache.hadoop.mapreduce.counters.CounterGroupBase;
import org.apache.hadoop.mapreduce.counters.FileSystemCounterGroup;
import org.apache.hadoop.mapreduce.counters.AbstractCounters;
import org.apache.hadoop.mapreduce.counters.CounterGroupFactory;
import org.apache.hadoop.mapreduce.counters.FrameworkCounterGroup;

/**
 * 文件说明: MapReduce任务计数器容器，负责管理作业/任务运行过程中所有统计计数器
 * 
 * <p><code>Counters</code> holds per job/task counters, defined either by the
 * Map-Reduce framework or applications. Each <code>Counter</code> can be of
 * any {@link Enum} type.</p>
 *
 * <p><code>Counters</code> are bunched into {@link CounterGroup}s, each
 * comprising of counters from a particular <code>Enum</code> class.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class Counters extends AbstractCounters<Counter, CounterGroup> {

  /**
   * 框架内置枚举计数器分组实现，将框架分组实现混入CounterGroup接口
   * @param <T> 枚举类型的计数器key
   */
  // Mix framework group implementation into CounterGroup interface
  private static class FrameworkGroupImpl<T extends Enum<T>>
      extends FrameworkCounterGroup<T, Counter> implements CounterGroup {

    /**
     * 构造函数，基于枚举类创建框架计数器分组
     * @param cls 计数器枚举类
     */
    FrameworkGroupImpl(Class<T> cls) {
      super(cls);
    }

    @Override
    protected FrameworkCounter<T> newCounter(T key) {
      return new FrameworkCounter<T>(key, getName());
    }

    @Override
    public CounterGroupBase<Counter> getUnderlyingGroup() {
      return this;
    }
  }

  /**
   * 通用自定义计数器分组实现，将通用分组实现混入CounterGroup接口
   */
  // Mix generic group implementation into CounterGroup interface
  // and provide some mandatory group factory methods.
  private static class GenericGroup extends AbstractCounterGroup<Counter>
      implements CounterGroup {

    /**
     * 构造函数，创建通用自定义计数器分组
     * @param name 分组名称
     * @param displayName 分组展示名称
     * @param limits 计数器数量限制
     */
    GenericGroup(String name, String displayName, Limits limits) {
      super(name, displayName, limits);
    }

    @Override
    protected Counter newCounter(String name, String displayName, long value) {
      return new GenericCounter(name, displayName, value);
    }

    @Override
    protected Counter newCounter() {
      return new GenericCounter();
    }

    @Override
    public CounterGroupBase<Counter> getUnderlyingGroup() {
      return this;
    }
  }

  /**
   * 文件系统计数器分组实现，将文件系统分组实现混入CounterGroup接口
   * 用于统计各文件系统的IO操作指标
   */
  // Mix file system group implementation into the CounterGroup interface
  private static class FileSystemGroup extends FileSystemCounterGroup<Counter>
      implements CounterGroup {

    @Override
    protected Counter newCounter(String scheme, FileSystemCounter key) {
      return new FSCounter(scheme, key);
    }

    @Override
    public CounterGroupBase<Counter> getUnderlyingGroup() {
      return this;
    }
  }

  /**
   * 计数器分组工厂实现，负责创建不同类型的计数器分组
   * See also the GroupFactory in
   *  {@link org.apache.hadoop.mapred.Counters mapred.Counters}
   */
  private static class GroupFactory
      extends CounterGroupFactory<Counter, CounterGroup> {

    @Override
    protected <T extends Enum<T>>
    FrameworkGroupFactory<CounterGroup>
        newFrameworkGroupFactory(final Class<T> cls) {
      return new FrameworkGroupFactory<CounterGroup>() {
        @Override public CounterGroup newGroup(String name) {
          return new FrameworkGroupImpl<T>(cls); // impl in this package
        }
      };
    }

    @Override
    protected CounterGroup newGenericGroup(String name, String displayName,
                                           Limits limits) {
      return new GenericGroup(name, displayName, limits);
    }

    @Override
    protected CounterGroup newFileSystemGroup() {
      return new FileSystemGroup();
    }
  }

  // 单例分组工厂实例，全局复用
  private static final GroupFactory groupFactory = new GroupFactory();

  /**
   * 默认构造函数，创建空的计数器容器
   */
  public Counters() {
    super(groupFactory);
  }

  /**
   * 拷贝构造函数，基于已有计数器对象创建新的计数器容器
   * @param <C> 计数器类型
   * @param <G> 计数器分组类型
   * @param counters 源计数器对象
   */
  public <C extends Counter, G extends CounterGroupBase<C>>
  Counters(AbstractCounters<C, G> counters) {
    super(counters, groupFactory);
  }
}