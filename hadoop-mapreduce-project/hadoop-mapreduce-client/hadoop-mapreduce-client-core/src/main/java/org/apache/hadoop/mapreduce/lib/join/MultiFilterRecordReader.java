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

package org.apache.hadoop.mapreduce.lib.join;

import java.io.IOException;
import java.util.PriorityQueue;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * 文件：MultiFilterRecordReader.java
 * 包路径：org.apache.hadoop.mapreduce.lib.join
 *
 * 多数据源连接的基础记录读取器抽象类，用于MapReduce连接操作中，从多个输入源读取数据，
 * 但不直接输出完整连接元组，而是输出从元组派生的单个值。
 * 作为MapReduce端连接（side join）功能的核心基础类，支持自定义过滤和投影逻辑。
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public abstract class MultiFilterRecordReader<K extends WritableComparable<?>,
                                              V extends Writable>
    extends CompositeRecordReader<K,V,V> {

  private TupleWritable ivalue = null;

  /**
   * 构造多过滤记录读取器
   * @param id 记录读取器ID
   * @param conf Hadoop作业配置
   * @param capacity 最大连接数据源数量
   * @param cmpcl 键比较器类
   * @throws IOException 如果初始化失败抛出IO异常
   */
  public MultiFilterRecordReader(int id, Configuration conf, int capacity,
      Class<? extends WritableComparator> cmpcl) throws IOException {
    super(id, capacity, cmpcl);
    setConf(conf);
  }

  /**
   * 抽象方法：从连接生成的元组中派生并返回最终输出值。
   * 子类需要实现该方法定义自定义投影/过滤逻辑，通常返回元组中的某一个值。
   * 允许修改元组中的Writable对象，但不推荐，建议先克隆对象再修改。
   * @param dst 多数据源连接生成的完整元组
   * @return 派生后的最终输出值
   * @throws IOExceptio 处理过程中IO异常
   */
  protected abstract V emit(TupleWritable dst) throws IOException;

  /**
   * 默认组合逻辑：接受所有来自子记录读取器外连接生成的元组，直接返回true表示全部输出。
   * 子类可以重写该方法实现自定义过滤逻辑，筛选符合条件的连接元组。
   * @param srcs 各数据源提供的值数组
   * @param dst 待输出的连接元组
   * @return 默认始终返回true，接受所有元组
   */
  protected boolean combine(Object[] srcs, TupleWritable dst) {
    return true;
  }

  /** {@inheritDoc} */
  public boolean nextKeyValue() throws IOException, InterruptedException {
    // 初始化键对象
    if (key == null) {
      key = createKey();
    }
    // 初始化值对象
    if (value == null) {
      value = createValue();
    }
    // 尝试从连接收集器取出已完成连接的元组
    if (jc.flush(ivalue)) {
      // 复制连接键到当前输出键
      ReflectionUtils.copy(conf, jc.key(), key);
      // 调用emit派生值，复制结果到当前输出值
      ReflectionUtils.copy(conf, emit(ivalue), value);
      return true;
    }
    // 初始化存储连接元组的对象
    if (ivalue == null) {
      ivalue = createTupleWritable();
    }
    // 清空连接收集器
    jc.clear();
    // 获取所有子记录读取器的优先级队列（按键排序）
    final PriorityQueue<ComposableRecordReader<K,?>> q = 
            getRecordReaderQueue();
    // 创建迭代键对象
    K iterkey = createKey();
    // 遍历所有待处理的记录读取器，直到取出下一个可输出的键值对
    while (q != null && !q.isEmpty()) {
      // 将相同键的所有值填充到连接收集器
      fillJoinCollector(iterkey);
      // 重置连接收集器到当前迭代键
      jc.reset(iterkey);
      // 如果完成连接，输出结果
      if (jc.flush(ivalue)) {
        ReflectionUtils.copy(conf, jc.key(), key);
        ReflectionUtils.copy(conf, emit(ivalue), value);
        return true;
      }
      // 清空收集器，处理下一个键
      jc.clear();
    }
    // 所有记录处理完毕，返回false
    return false;
  }

  @SuppressWarnings("unchecked")
  public void initialize(InputSplit split, TaskAttemptContext context) 
      throws IOException, InterruptedException {
    super.initialize(split, context);
  }

  /**
   * 获取委派迭代器，该迭代器从连接元组中取出单个值供遍历输出。
   * @return 多过滤委派迭代器实例
   * @see MultiFilterDelegationIterator
   */
  protected ResetableIterator<V> getDelegate() {
    return new MultiFilterDelegationIterator();
  }

  /**
   * 多过滤委派迭代器，代理JoinCollector的迭代操作，
   * 在每次获取下一个值时调用emit方法将连接元组转换为最终输出值。
   * 实现ResetableIterator接口，支持重置迭代，适配连接操作的重复遍历需求。
   */
  protected class MultiFilterDelegationIterator
      implements ResetableIterator<V> {

    public boolean hasNext() {
      return jc.hasNext();
    }

    public boolean next(V val) throws IOException {
      boolean ret;
      // 从连接收集器取出连接元组，如果存在则调用emit生成值并复制到输出
      if (ret = jc.flush(ivalue)) {
        ReflectionUtils.copy(getConf(), emit(ivalue), val);
      }
      return ret;
    }

    public boolean replay(V val) throws IOException {
      // 重新播放上一个元组，生成值复制到输出
      ReflectionUtils.copy(getConf(), emit(ivalue), val);
      return true;
    }

    public void reset() {
      // 重置迭代器到当前键位置
      jc.reset(jc.key());
    }

    public void add(V item) throws IOException {
      // 不支持添加操作，抛出异常
      throw new UnsupportedOperationException();
    }

    public void close() throws IOException {
      // 关闭底层连接收集器
      jc.close();
    }

    public void clear() {
      // 清空底层连接收集器
      jc.clear();
    }
  }

}