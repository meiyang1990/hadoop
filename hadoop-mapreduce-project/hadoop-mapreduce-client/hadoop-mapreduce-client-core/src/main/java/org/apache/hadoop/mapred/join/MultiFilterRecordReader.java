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

package org.apache.hadoop.mapred.join;

import java.io.IOException;
import java.util.PriorityQueue;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;

/**
 * 文件级注释：MapReduce多数据源连接基类，用于从多个输入源读取数据后生成非元组形式的连接结果
 * 基类用于复合连接，返回从多个数据源派生的值，通常不返回连接元组
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public abstract class MultiFilterRecordReader<K extends WritableComparable,
                                              V extends Writable>
    extends CompositeRecordReader<K,V,V>
    implements ComposableRecordReader<K,V> {

  private Class<? extends Writable> valueclass;
  private TupleWritable ivalue;

  /**
   * 构造函数：初始化多过滤记录读取器
   * @param id 读取器编号
   * @param conf 作业配置
   * @param capacity 最大连接数据源数量
   * @param cmpcl 键比较器类
   * @throws IOException 初始化IO异常
   */
  public MultiFilterRecordReader(int id, JobConf conf, int capacity,
      Class<? extends WritableComparator> cmpcl) throws IOException {
    super(id, capacity, cmpcl);
    setConf(conf);
  }

  /**
   * 抽象方法：从连接元组中提取需要输出的值，通常返回元组中的某一个值
   * @param dst 连接生成的元组
   * @return 处理后需要输出的值
   * @throws IOException 处理IO异常
   */
  protected abstract V emit(TupleWritable dst) throws IOException;

  /**
   * 判断是否组合当前输入元组，默认实现接受所有来自外连接的元组
   * @param srcs 输入源数组
   * @param dst 目标输出元组
   * @return 始终返回true，表示接受所有元组
   */
  protected boolean combine(Object[] srcs, TupleWritable dst) {
    return true;
  }

  /** {@inheritDoc} */
  public boolean next(K key, V value) throws IOException {
    // 尝试从连接收集器获取下一个匹配元组
    if (jc.flush(ivalue)) {
      // 克隆键到输出变量
      WritableUtils.cloneInto(key, jc.key());
      // 克隆处理后的值到输出变量
      WritableUtils.cloneInto(value, emit(ivalue));
      return true;
    }
    // 清空连接收集器
    jc.clear();
    // 创建新的迭代键
    K iterkey = createKey();
    // 获取排序后的记录读取器队列
    final PriorityQueue<ComposableRecordReader<K,?>> q = getRecordReaderQueue();
    // 遍历读取器队列查找下一个匹配键
    while (!q.isEmpty()) {
      // 将当前最小键填充到连接收集器
      fillJoinCollector(iterkey);
      // 重置收集器为当前迭代键
      jc.reset(iterkey);
      // 尝试冲刷收集到连接结果
      if (jc.flush(ivalue)) {
        WritableUtils.cloneInto(key, jc.key());
        WritableUtils.cloneInto(value, emit(ivalue));
        return true;
      }
      jc.clear();
    }
    // 所有数据读取完毕
    return false;
  }

  /** {@inheritDoc} */
  @SuppressWarnings("unchecked") // Explicit check for value class agreement
  /**
   * 创建新的值对象，统一所有子读取器的值类型
   * @return 新建的值实例
   */
  public V createValue() {
    if (null == valueclass) {
      // 获取第一个子读取器的值类型
      final Class<?> cls = kids[0].createValue().getClass();
      // 检查所有子读取器的值类型是否一致
      for (RecordReader<K,? extends V> rr : kids) {
        if (!cls.equals(rr.createValue().getClass())) {
          throw new ClassCastException("Child value classes fail to agree");
        }
      }
      // 保存值类型并初始化内部元组
      valueclass = cls.asSubclass(Writable.class);
      ivalue = createInternalValue();
    }
    // 通过反射创建新值实例
    return (V) ReflectionUtils.newInstance(valueclass, null);
  }

  /**
   * 获取从元组中提取单个值的迭代器
   * @return 包装后的迭代器实例
   * @see MultiFilterDelegationIterator
   */
  protected ResetableIterator<V> getDelegate() {
    return new MultiFilterDelegationIterator();
  }

  /**
   * 内部迭代器类：代理连接收集器，在返回值前调用emit方法处理连接元组
   * 实现可重置迭代器接口，提供单值输出能力
   */
  protected class MultiFilterDelegationIterator
      implements ResetableIterator<V> {

    public boolean hasNext() {
      return jc.hasNext();
    }

    public boolean next(V val) throws IOException {
      boolean ret;
      if (ret = jc.flush(ivalue)) {
        // 将emit处理后的值克隆到输出变量
        WritableUtils.cloneInto(val, emit(ivalue));
      }
      return ret;
    }

    public boolean replay(V val) throws IOException {
      // 重放上一次处理结果
      WritableUtils.cloneInto(val, emit(ivalue));
      return true;
    }

    public void reset() {
      // 重置收集器到当前键
      jc.reset(jc.key());
    }

    public void add(V item) throws IOException {
      // 不支持添加操作
      throw new UnsupportedOperationException();
    }

    public void close() throws IOException {
      // 关闭连接收集器
      jc.close();
    }

    public void clear() {
      // 清空连接收集器
      jc.clear();
    }
  }

}