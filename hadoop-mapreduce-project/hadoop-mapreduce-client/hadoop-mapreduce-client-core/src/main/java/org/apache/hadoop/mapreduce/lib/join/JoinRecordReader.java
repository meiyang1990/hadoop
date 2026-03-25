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
import org.apache.hadoop.util.ReflectionUtils;

/**
 * MapReduce关联操作记录读取器的基类，用于多数据源关联计算，返回包含任意Writable类型的元组结果。
 * 是MapReduce端连接操作的核心基础组件，负责将多个已排序输入分片按key进行归并关联。
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public abstract class JoinRecordReader<K extends WritableComparable<?>>
    extends CompositeRecordReader<K,Writable,TupleWritable> {

  /**
   * 构造关联操作记录读取器
   * @param id 分片编号
   * @param conf 作业配置对象
   * @param capacity 关联输入的数量
   * @param cmpcl key比较器类型，用于排序归并
   * @throws IOException IO异常
   */
  public JoinRecordReader(int id, Configuration conf, int capacity,
      Class<? extends WritableComparator> cmpcl) throws IOException {
    super(id, capacity, cmpcl);
    setConf(conf);
  }

  /**
   * 获取下一个关联后的key-value对，根据当前关联操作类型（内连接、外连接等）输出结果
   * @return 是否还有下一个结果
   * @throws IOException IO异常
   * @throws InterruptedException 中断异常
   */
  public boolean nextKeyValue() 
      throws IOException, InterruptedException {
    if (key == null) {
      key = createKey();
    }
    // 尝试从关联收集器取出结果
    if (jc.flush(value)) {
      ReflectionUtils.copy(conf, jc.key(), key);
      return true;
    }
    jc.clear();
    if (value == null) {
      value = createValue();
    }
    // 获取记录读取器优先级队列（按key排序）
    final PriorityQueue<ComposableRecordReader<K,?>> q = 
            getRecordReaderQueue();
    K iterkey = createKey();
    // 遍历队列收集相同key的所有记录
    while (q != null && !q.isEmpty()) {
      fillJoinCollector(iterkey);
      jc.reset(iterkey);
      if (jc.flush(value)) {
        ReflectionUtils.copy(conf, jc.key(), key);
        return true;
      }
      jc.clear();
    }
    return false;
  }

  /**
   * 创建存储关联结果的TupleWritable对象
   * @return 空的结果元组对象
   */
  public TupleWritable createValue() {
    return createTupleWritable();
  }

  /**
   * 获取包装JoinCollector的迭代器，用于遍历关联结果
   * @return 可重置的结果迭代器
   */
  protected ResetableIterator<TupleWritable> getDelegate() {
    return new JoinDelegationIterator();
  }

  /**
   * 关联收集器的迭代器代理，直接复用JoinCollector的现有逻辑，提供标准迭代器接口
   */
  protected class JoinDelegationIterator
      implements ResetableIterator<TupleWritable> {

    public boolean hasNext() {
      return jc.hasNext();
    }

    public boolean next(TupleWritable val) throws IOException {
      return jc.flush(val);
    }

    public boolean replay(TupleWritable val) throws IOException {
      return jc.replay(val);
    }

    public void reset() {
      jc.reset(jc.key());
    }

    public void add(TupleWritable item) throws IOException {
      throw new UnsupportedOperationException();
    }

    public void close() throws IOException {
      jc.close();
    }

    public void clear() {
      jc.clear();
    }
  }
}