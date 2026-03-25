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
import java.util.ArrayList;
import java.util.Comparator;
import java.util.PriorityQueue;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * 支持对具有相同键类型和分区的多个RecordReader进行关联操作的复合RecordReader
 * 是MapReduce端连接操作的基础抽象类，负责统一管理多个子Reader，并按键排序组织数据
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public abstract class CompositeRecordReader<
    K extends WritableComparable<?>, // key type
    V extends Writable,  // accepts RecordReader<K,V> as children
    X extends Writable>  // emits Writables of this type
    extends ComposableRecordReader<K, X>
    implements Configurable {

  private int id;
  protected Configuration conf;
  private final ResetableIterator<X> EMPTY = new ResetableIterator.EMPTY<X>();

  private WritableComparator cmp;
  @SuppressWarnings("unchecked")
  protected Class<? extends WritableComparable> keyclass = null;
  private PriorityQueue<ComposableRecordReader<K,?>> q;

  protected final JoinCollector jc;
  protected final ComposableRecordReader<K,? extends V>[] kids;

  protected abstract boolean combine(Object[] srcs, TupleWritable value);
  
  protected K key;
  protected X value;

  /**
   * 构造一个可容纳指定数量子Reader的复合RecordReader
   * @param id 当前Reader在父Reader中的位置索引
   * @param capacity 可容纳的子Reader最大数量
   * @param cmpcl 键比较器类
   * @throws IOException 如果初始化失败抛出IO异常
   */
  @SuppressWarnings("unchecked") // Generic array assignment
  public CompositeRecordReader(int id, int capacity,
      Class<? extends WritableComparator> cmpcl)
      throws IOException {
    assert capacity > 0 : "Invalid capacity";
    this.id = id;
    if (null != cmpcl) {
      cmp = ReflectionUtils.newInstance(cmpcl, null);
      q = new PriorityQueue<ComposableRecordReader<K,?>>(3,
            new Comparator<ComposableRecordReader<K,?>>() {
              public int compare(ComposableRecordReader<K,?> o1,
                                 ComposableRecordReader<K,?> o2) {
                return cmp.compare(o1.key(), o2.key());
              }
            });
    }
    jc = new JoinCollector(capacity);
    kids = new ComposableRecordReader[capacity];
  }

  @SuppressWarnings("unchecked")
  public void initialize(InputSplit split, TaskAttemptContext context) 
      throws IOException, InterruptedException {
    if (kids != null) {
      // 遍历初始化所有子Reader
      for (int i = 0; i < kids.length; ++i) {
        kids[i].initialize(((CompositeInputSplit)split).get(i), context);
        if (kids[i].key() == null) {
          continue;
        }
        
        // 获取键类型，从第一个非空子Reader获取
        if (keyclass == null) {
          keyclass = kids[i].createKey().getClass().
            asSubclass(WritableComparable.class);
        }
        // 如果优先级队列未初始化则创建
        if (null == q) {
          cmp = WritableComparator.get(keyclass, conf);
          q = new PriorityQueue<ComposableRecordReader<K,?>>(3,
                new Comparator<ComposableRecordReader<K,?>>() {
                  public int compare(ComposableRecordReader<K,?> o1,
                                     ComposableRecordReader<K,?> o2) {
                    return cmp.compare(o1.key(), o2.key());
                  }
                });
        }
        // 检查所有子Reader键类型是否一致
        if (!keyclass.equals(kids[i].key().getClass())) {
          throw new ClassCastException("Child key classes fail to agree");
        }
        
        // 将有数据的子Reader加入优先级队列（按键排序）
        if (kids[i].hasNext()) {
          q.add(kids[i]);
        }
      }
    }
  }

  /**
   * 获取当前Reader在父Collector中的位置索引
   */
  public int id() {
    return id;
  }

  /**
   * {@inheritDoc}
   */
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  /**
   * {@inheritDoc}
   */
  public Configuration getConf() {
    return conf;
  }

  /**
   * 获取按键排序的子Reader优先级队列
   */
  protected PriorityQueue<ComposableRecordReader<K,?>> getRecordReaderQueue() {
    return q;
  }

  /**
   * 获取键比较器
   */
  protected WritableComparator getComparator() {
    return cmp;
  }

  /**
   * 添加子Reader到集合中，子Reader的id决定其输出元组中的位置
   * @param rr 要添加的可组合RecordReader
   * @throws IOException IO异常
   * @throws InterruptedException 中断异常
   */
  public void add(ComposableRecordReader<K,? extends V> rr) 
      throws IOException, InterruptedException {
    kids[rr.id()] = rr;
  }

  /**
   * 连接值收集器，用于收集同一键来自多个子Reader的值，支持生成笛卡尔积
   */
  public class JoinCollector {
    private K key;
    private ResetableIterator<X>[] iters;
    private int pos = -1;
    private boolean first = true;

    /**
     * 构造可处理指定数量子节点的收集器
     * @param card 子节点数量
     */
    @SuppressWarnings("unchecked") // Generic array assignment
    public JoinCollector(int card) {
      iters = new ResetableIterator[card];
      for (int i = 0; i < iters.length; ++i) {
        iters[i] = EMPTY;
      }
    }

    /**
     * 在指定位置注册迭代器
     * @param id 位置索引
     * @param i 可重置迭代器
     * @throws IOException IO异常
     */
    public void add(int id, ResetableIterator<X> i)
        throws IOException {
      iters[id] = i;
    }

    /**
     * 获取当前收集的键
     */
    public K key() {
      return key;
    }

    /**
     * 重置收集器，准备收集新键的值
     * @param key 当前要收集的键
     */
    public void reset(K key) {
      this.key = key;
      first = true;
      pos = iters.length - 1;
      // 重置所有迭代器
      for (int i = 0; i < iters.length; ++i) {
        iters[i].reset();
      }
    }

    /**
     * 清空收集器所有状态
     */
    public void clear() {
      key = null;
      pos = -1;
      for (int i = 0; i < iters.length; ++i) {
        iters[i].clear();
        iters[i] = EMPTY;
      }
    }

    /**
     * 检查是否还有更多值组合
     * @return 如果还有未返回的组合返回true，否则false
     */
    public boolean hasNext() {
      return !(pos < 0);
    }

    /**
     * 获取下一个值组合，填充到TupleWritable中
     * 实现笛卡尔积迭代，每次调用生成一个新的组合
     * @param val 用于存储结果的元组
     * @return 是否成功生成下一个组合
     * @throws IOException IO异常
     */
    @SuppressWarnings("unchecked") // No static type info on Tuples
    protected boolean next(TupleWritable val) throws IOException {
      if (first) {
        // 第一次迭代，初始化所有位置
        int i = -1;
        for (pos = 0; pos < iters.length; ++pos) {
          if (iters[pos].hasNext() && iters[pos].next((X)val.get(pos))) {
            i = pos;
            val.setWritten(i);
          }
        }
        pos = i;
        first = false;
        if (pos < 0) {
          clear();
          return false;
        }
        return true;
      }
      // 回退查找下一个有数据的位置
      while (0 <= pos && !(iters[pos].hasNext() &&
                           iters[pos].next((X)val.get(pos)))) {
        --pos;
      }
      if (pos < 0) {
        clear();
        return false;
      }
      val.setWritten(pos);
      // 重放之前位置的值
      for (int i = 0; i < pos; ++i) {
        if (iters[i].replay((X)val.get(i))) {
          val.setWritten(i);
        }
      }
      // 重置并填充后续位置
      while (pos + 1 < iters.length) {
        ++pos;
        iters[pos].reset();
        if (iters[pos].hasNext() && iters[pos].next((X)val.get(pos))) {
          val.setWritten(pos);
        }
      }
      return true;
    }

    /**
     * 重放最后一次发出的元组
     * @param val 用于存储结果的元组
     * @return 是否重放成功
     * @throws IOException IO异常
     */
    @SuppressWarnings("unchecked") // No static typeinfo on Tuples
    public boolean replay(TupleWritable val) throws IOException {
      // The last emitted tuple might have drawn on an empty source;
      // it can't be cleared prematurely, b/c there may be more duplicate
      // keys in iterator positions < pos
      assert !first;
      boolean ret = false;
      for (int i = 0; i < iters.length; ++i) {
        if (iters[i].replay((X)val.get(i))) {
          val.setWritten(i);
          ret = true;
        }
      }
      return ret;
    }

    /**
     * 关闭所有子迭代器
     * @throws IOException IO异常
     */
    public void close() throws IOException {
      for (int i = 0; i < iters.length; ++i) {
        iters[i].close();
      }
    }

    /**
     * 刷新收集器，输出下一个符合连接条件的组合
     * @param value 存储结果的元组
     * @return 是否输出了有效组合
     * @throws IOException IO异常
     */
    public boolean flush(TupleWritable value) throws IOException {
      while (hasNext()) {
        value.clearWritten();
        if (next(value) && combine(kids, value)) {
          return true;
        }
      }
      return false;
    }
  }

  /**
   * 获取当前键，如果收集器有数据则返回收集器的键，否则返回堆顶子Reader的键
   */
  public K key() {
    if (jc.hasNext()) {
      return jc.key();
    }
    if (!q.isEmpty()) {
      return q.peek().key();
    }
    return null;
  }

  /**
   * 将当前堆顶键拷贝到给定对象中
   * @param key 目标键对象
   * @throws IOException IO异常
   */
  public void key(K key) throws IOException {
    ReflectionUtils.copy(conf, key(), key);
  }

  public K getCurrentKey() {
    return key;
  }
  
  /**
   * 检查是否还有更多数据可以输出
   * @return 收集器有数据或队列非空返回true，否则false
   */
  public boolean hasNext() {
    return jc.hasNext() || !q.isEmpty();
  }

  /**
   * 跳过所有小于等于给定键的子Reader数据
   * @param key 要跳过的键
   * @throws IOException IO异常
   * @throws InterruptedException 中断异常
   */
  public void skip(K key) throws IOException, InterruptedException {
    ArrayList<ComposableRecordReader<K,?>> tmp =
      new ArrayList<ComposableRecordReader<K,?>>();
    // 弹出所有键小于等于当前键的子Reader
    while (!q.isEmpty() && cmp.compare(q.peek().key(), key) <= 0) {
      tmp.add(q.poll());
    }
    // 让子Reader跳过当前键，有剩余数据则重新入队
    for (ComposableRecordReader<K,?> rr : tmp) {
      rr.skip(key);
      if (rr.hasNext()) {
        q.add(rr);
      }
    }
  }

  /**
   * 获取对应输出值类型的委托迭代器，由子类实现
   */
  protected abstract ResetableIterator<X> getDelegate();

  /**
   * 如果当前键匹配，将当前值迭代器添加到收集器中
   */
  @SuppressWarnings("unchecked") // No values from static EMPTY class
  @Override
  public void accept(CompositeRecordReader.JoinCollector jc, K key)
      throws IOException, InterruptedException {
    if (hasNext() && 0 == cmp.compare(key, key())) {
      fillJoinCollector(createKey());
      jc.add(id, getDelegate());
      return;
    }
    jc.add(id, EMPTY);
  }

  /**
   * 填充JoinCollector，收集所有匹配当前键的子Reader迭代器
   * @param iterkey 要匹配的键
   * @throws IOException IO异常
   * @throws InterruptedException 中断异常
   */
  protected void fillJoinCollector(K iterkey) 
      throws IOException, InterruptedException {
    if (!q.isEmpty()) {
      q.peek().key(iterkey);
      // 处理所有键匹配当前key的子Reader
      while (0 == cmp.compare(q.peek().key(), iterkey)) {
        ComposableRecordReader<K,?> t = q.poll();
        t.accept(jc, iterkey);
        if (t.hasNext()) {
          q.add(t);
        } else if (q.isEmpty()) {
          return;
        }
      }
    }
  }

  /**
   * 实现Comparable接口，基于当前键比较
   */
  public int compareTo(ComposableRecordReader<K,?> other) {
    return cmp.compare(key(), other.key());
  }

  /**
   * 创建一个所有子Reader通用的新键实例
   * @return 新键实例
   * @throws ClassCastException 如果子Reader键类型不一致抛出异常
   */
  @SuppressWarnings("unchecked")
  protected K createKey() {
    if (keyclass == null || keyclass.equals(NullWritable.class)) {
      return (K) NullWritable.get();
    }
    return (K) ReflectionUtils.newInstance(keyclass, getConf());
  }

  /**
   * 创建用于存储连接结果的TupleWritable
   * @return 新元组实例
   */
  protected TupleWritable createTupleWritable() {
    Writable[] vals = new Writable[kids.length];
    for (int i = 0; i < vals.length; ++i) {
      vals[i] = kids[i].createValue();
    }
    return new TupleWritable(vals);
  }

  /** {@inheritDoc} */
  public X getCurrentValue() 
      throws IOException, InterruptedException {
    return value;
  }

  /**
   * 关闭所有子Reader和收集器
   * @throws IOException IO异常
   */
  public void close() throws IOException {
    if (kids != null) {
      for (RecordReader<K,? extends Writable> rr : kids) {
        rr.close();
      }
    }
    if (jc != null) {
      jc.close();
    }
  }

  /**
   * 计算整体进度，取所有子Reader进度的最小值
   * @return 整体进度
   * @throws IOException IO异常
   * @throws InterruptedException 中断异常
   */
  public float getProgress() throws IOException, InterruptedException {
    float ret = 1.0f;
    for (RecordReader<K,? extends Writable> rr : kids) {
      ret = Math.min(ret