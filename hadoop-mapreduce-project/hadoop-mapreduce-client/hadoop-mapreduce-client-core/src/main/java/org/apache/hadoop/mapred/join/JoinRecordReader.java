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
import org.apache.hadoop.mapred.JobConf;

/**
 * 组合连接的基类，用于返回任意Writable类型组成的元组
 * 作为MapReduce端连接操作的基础RecordReader实现，提供多数据源连接的公共逻辑
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public abstract class JoinRecordReader<K extends WritableComparable>
    extends CompositeRecordReader<K,Writable,TupleWritable>
    implements ComposableRecordReader<K,TupleWritable> {

  /**
   * 构造连接RecordReader
   * @param id Reader编号，用于标识不同输入分片
   * @param conf 作业配置对象
   * @param capacity 连接收集器容量
   * @param cmpcl 键比较器类，用于排序键
   * @throws IOException 如果初始化失败抛出IO异常
   */
  public JoinRecordReader(int id, JobConf conf, int capacity,
      Class<? extends WritableComparator> cmpcl) throws IOException {
    super(id, capacity, cmpcl);
    setConf(conf);
  }

  /**
   * 获取下一个连接后的键值对，按照连接操作定义输出结果
   * @param key 输出键对象
   * @param value 输出元组值对象，存储多个数据源的连接结果
   * @return 是否还有更多连接结果，true表示有可用结果，false表示读取完毕
   * @throws IOException IO异常
   */
  public boolean next(K key, TupleWritable value) throws IOException {
    // 尝试从连接收集器刷新结果到输出值
    if (jc.flush(value)) {
      // 刷新成功，克隆连接键到输出键
      WritableUtils.cloneInto(key, jc.key());
      return true;
    }
    // 无可用结果，清空连接收集器
    jc.clear();
    // 创建新的迭代键
    K iterkey = createKey();
    // 获取已排序的RecordReader优先级队列
    final PriorityQueue<ComposableRecordReader<K,?>> q = getRecordReaderQueue();
    // 遍历所有RecordReader查找匹配键
    while (!q.isEmpty()) {
      // 填充连接收集器，获取当前最小键对应的所有记录
      fillJoinCollector(iterkey);
      // 重置收集器，准备处理当前迭代键
      jc.reset(iterkey);
      // 尝试刷新连接结果
      if (jc.flush(value)) {
        // 刷新成功，克隆键并返回
        WritableUtils.cloneInto(key, jc.key());
        return true;
      }
      // 无结果，清空收集器继续下一轮
      jc.clear();
    }
    // 所有RecordReader读取完毕，返回false
    return false;
  }

  /**
   * 创建输出值对象
   * {@inheritDoc}
   */
  public TupleWritable createValue() {
    return createInternalValue();
  }

  /**
   * 获取包装JoinCollector的迭代器
   * @return 包装后的可重置迭代器
   */
  protected ResetableIterator<TupleWritable> getDelegate() {
    return new JoinDelegationIterator();
  }

  /**
   * 迭代器代理，包装JoinCollector的操作，对外提供统一的迭代接口
   * 由于连接逻辑由JoinCollector处理，本类仅作为代理转发调用
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