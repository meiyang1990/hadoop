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
import java.util.ArrayList;
import java.util.Comparator;
import java.util.PriorityQueue;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * 文件：CompositeRecordReader.java
 * 模块：MapReduce 核心模块，连接操作实现
 * 功能：实现多数据源记录读取器的组合连接，将多个共享相同键类型和分区的RecordReader合并，为Map端连接提供底层支持
 * 
 * 能够组合多个RecordReader执行连接操作，共享相同键类型和分区，为连接操作提供统一的键值读取接口
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public abstract class CompositeRecordReader<
    K extends WritableComparable, // key type
    V extends Writable,           // accepts RecordReader<K,V> as children
    X extends Writable>           // emits Writables of this type
    implements Configurable {


  private int id;
  private Configuration conf;
  private final ResetableIterator<X> EMPTY = new ResetableIterator.EMPTY<X>();

  private WritableComparator cmp;
  private Class<? extends WritableComparable> keyclass;
  private PriorityQueue<ComposableRecordReader<K,?>> q;

  protected final JoinCollector jc;
  protected final ComposableRecordReader<K,? extends V>[] kids;

  /**
   * 抽象方法，组合所有数据源的内容到输出元组
   * @param srcs 各数据源值数组
   * @param value 输出元组
   * @return 是否成功组合
   */
  protected abstract boolean combine(Object[] srcs, TupleWritable value);

  /**
   * 构造组合记录读取器
   * @param id 当前读取器在父读取器中的位置
   * @param capacity 子读取器最大数量
   * @param cmpcl 键比较器类
   * @throws IOException 初始化异常
   */
  @SuppressWarnings("unchecked") // Generic array assignment
  public CompositeRecordReader(int id, int capacity,
      Class<? extends WritableComparator> cmpcl)
      throws IOException {
    assert capacity > 0 : "Invalid capacity";
    this.id = id;
    if (null != cmpcl) {
      // 实例化键比较器
      cmp = ReflectionUtils.newInstance(cmpcl, null);
      // 创建优先队列，按键排序存储子读取器
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

  /**
   * 获取当前读取器在收集器中的位置索引
   * @return 位置索引
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
   * 获取按键排序的子读取器优先队列
   * @return 存储子读取器的优先队列
   */
  protected PriorityQueue<ComposableRecordReader<K,?>> getRecordReaderQueue() {
    return q;
  }

  /**
   * 获取键排序比较器
   * @return 键比较器
   */
  protected WritableComparator getComparator() {
    return cmp;
  }

  /**
   * 向组合集合中添加一个子记录读取器
   * @param rr 要添加的可组合记录读取器
   * @throws IOException IO异常
   */
  public void add(ComposableRecordReader<K,? extends V> rr) throws IOException {
    // 根据id将读取器放入对应位置
    kids[rr.id()] = rr;
    if (null == q) {
      // 延迟初始化比较器和队列
      cmp = WritableComparator.get(rr.createKey().getClass(), conf);
      q = new PriorityQueue<ComposableRecordReader<K,?>>(3,
          new Comparator<ComposableRecordReader<K,?>>() {
            public int compare(ComposableRecordReader<K,?> o1,
                               ComposableRecordReader<K,?> o2) {
              return cmp.compare(o1.key(), o2.key());
            }
          });
    }
    // 如果读取器还有数据，加入优先队列
    if (rr.hasNext()) {
      q.add(rr);
    }
  }

  /**
   * 连接收集器，收集同一个键来自多个子读取器的所有值，生成笛卡尔积供后续输出
   */
  class JoinCollector {
    private K key;
    private ResetableIterator<X>[] iters;
    private int pos = -1;
    private boolean first = true;

    /**
     * 构造可处理指定数量子节点的连接收集器
     * @param card 子读取器数量
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
     * 获取当前收集的连接键
     * @return 当前连接键
     */
    public K key() {
      return key;
    }

    /**
     * 重置收集器状态，准备收集新键的值
     * @param key 当前要收集的连接键
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
      // 清空所有迭代器
      for (int i = 0; i < iters.length; ++i) {
        iters[i].clear();
        iters[i] = EMPTY;
      }
    }

    /**
     * 检查是否还有未输出的笛卡尔积组合
     * @return 是否还有下一个组合
     */
    protected boolean hasNext() {
      return !(pos < 0);
    }

    /**
     * 获取下一个笛卡尔积组合，填充到输出元组
     * @param val 输出元组
     * @return 是否成功获取下一个组合
     * @throws IOException IO异常
     */
    @SuppressWarnings("unchecked") // No static typeinfo on Tuples
    protected boolean next(TupleWritable val) throws IOException {
      if (first) {
        // 第一次获取，初始化所有位置的第一个值
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
      // 寻找下一个有剩余值的位置，推进其迭代
      while (0 <= pos && !(iters[pos].hasNext() &&
                           iters[pos].next((X)val.get(pos)))) {
        --pos;
      }
      if (pos < 0) {
        clear();
        return false;
      }
      val.setWritten(pos);
      // 重放前面位置的值
      for (int i = 0; i < pos; ++i) {
        if (iters[i].replay((X)val.get(i))) {
          val.setWritten(i);
        }
      }
      // 重置并重新初始化当前位置之后的所有迭代器
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
     * 重放最后一次输出的元组
     * @param val 输出元组
     * @return 是否成功重放
     * @throws IOException IO异常
     */
    @SuppressWarnings("unchecked") // No static typeinfo on Tuples
    public boolean replay(TupleWritable val) throws IOException {
      // The last emitted tuple might have drawn on an empty source;
      // it can't be cleared prematurely, b/c there may be more duplicate
      // keys in iterator positions < pos
      assert !first;
      boolean ret = false;
      // 重放所有位置的值
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
     * 刷新输出下一个有效的组合元组
     * @param value 输出元组
     * @return 是否存在有效组合
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
   * 获取当前连接的键，优先返回收集器中正在处理的键，否则返回堆顶子读取器的键
   * @return 当前键
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
   * 将当前键克隆到目标对象
   * @param key 目标键对象
   * @throws IOException IO异常
   */
  public void key(K key) throws IOException {
    WritableUtils.cloneInto(key, key());
  }

  /**
   * 检查是否还有可输出的记录
   * @return 是否还有更多记录
   */
  public boolean hasNext() {
    return jc.hasNext() || !q.isEmpty();
  }

  /**
   * 跳过所有键小于等于指定键的记录
   * @param key 要跳过的键
   * @throws IOException IO异常
   */
  public void skip(K key) throws IOException {
    // 临时存储弹出的读取器
    ArrayList<ComposableRecordReader<K,?>> tmp =
      new ArrayList<ComposableRecordReader<K,?>>();
    while (!q.isEmpty() && cmp.compare(q.peek().key(), key) <= 0) {
      tmp.add(q.poll());
    }
    // 对每个弹出的读取器执行skip操作，还有数据则放回队列
    for (ComposableRecordReader<K,?> rr : tmp) {
      rr.skip(key);
      if (rr.hasNext()) {
        q.add(rr);
      }
    }
  }

  /**
   * 获取适合当前连接类型值的代理迭代器，抽象方法由子类实现
   * @return 可重置迭代器
   */
  protected abstract ResetableIterator<X> getDelegate();

  /**
   * 如果当前读取器的键匹配传入键，将对应值迭代器添加到连接收集器
   * @param jc 连接收集器
   * @param key 要匹配的键
   * @throws IOException IO异常
   */
  @SuppressWarnings("unchecked") // No values from static EMPTY class
  public void accept(CompositeRecordReader.JoinCollector jc, K key)
      throws IOException {
    if (hasNext() && 0 == cmp.compare(key, key())) {
      // 填充所有匹配该键的子读取器到收集器
      fillJoinCollector(createKey());
      jc.add(id, getDelegate());
      return;
    }
    // 不匹配则添加空迭代器
    jc.add(id, EMPTY);
  }

  /**
   * 将所有提供当前键的子读取器，将它们的值迭代器添加到连接收集器
   * @param iterkey 当前匹配的键
   * @throws IOException IO异常
   */
  protected void fillJoinCollector(K iterkey) throws IOException {
    if (!q.isEmpty()) {
      q.peek().key(iterkey);
      // 处理所有键匹配的子读取器
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
   * 比较当前读取器与另一个读取器的键，实现Comparable接口
   * @param other 另一个组合读取器
   * @return 比较结果
   */
  public int compareTo(ComposableRecordReader<K,?> other) {
    return cmp.compare(key(), other.key());
  }

  /**
   * 创建所有子读取器共用类型的键实例
   * @return 新键实例
   * @throws ClassCastException 如果子读取器键类型不一致
   */
  @SuppressWarnings("unchecked") // Explicit check for key class agreement
  public K createKey() {
    if (null == keyclass) {
      // 延迟确定键类型，检查所有子读取器键类型一致
      final Class<?> cls = kids[0].createKey().getClass();
      for (RecordReader<K,? extends Writable> rr : kids) {
        if (!cls.equals(rr.createKey().getClass())) {
          throw new ClassCastException("Child key classes fail to agree");
        }
      }
      keyclass = cls.asSubclass(WritableComparable.class);
    }
    return (K) ReflectionUtils.newInstance(keyclass, getConf());
  }

  /**
   * 创建连接内部使用的元组值对象
   * @return 新元组对象
   */
  protected TupleWritable createInternalValue() {
    Writable[] vals = new Writable[kids.length];
    for (int i = 0; i < vals.length; ++i) {
      vals[i] = kids[i].createValue();
    }
    return new TupleWritable(vals);
  }

  /**
   * 获取当前读取位置，该方法不支持，始终返回0
   * @return 0
   * @throws IOException IO异常
   */
  public long getPos() throws IOException {
    return 0;
  }

  /**
   * 关闭所有子读取器和连接收集器
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
   * 获取读取进度，返回所有子读取器进度的最小值
   * @return 整体进度
   * @throws IOException IO异常
   */
  public float getProgress() throws IOException {
    float ret = 1.0