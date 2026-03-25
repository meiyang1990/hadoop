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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapred.RecordReader;

/**
 * 文件说明：MapReduce Join框架中对原始RecordReader的包装代理类
 * 核心职责：维护当前RecordReader的头部键值对，缓存与当前连接键匹配的所有值，供多数据源连接操作使用
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class WrappedRecordReader<K extends WritableComparable,
                          U extends Writable>
    implements ComposableRecordReader<K,U>, Configurable {

  private boolean empty = false;
  private RecordReader<K,U> rr;
  private int id;  // 当前Reader在结果收集器中的插入位置索引

  private K khead; // 当前Reader的头部键
  private U vhead; // 头部键对应的值
  private WritableComparator cmp; // 键比较器
  private Configuration conf; // 配置对象

  private ResetableIterator<U> vjoin; // 当前连接键匹配的值集合迭代器

  /**
   * 构造函数：根据给定的RecordReader和位置ID创建包装对象
   * @param id 在收集器中的位置索引
   * @param rr 被包装的原始RecordReader
   * @param cmpcl 键比较器类
   */
  WrappedRecordReader(int id, RecordReader<K,U> rr,
      Class<? extends WritableComparator> cmpcl) throws IOException {
    this(id, rr, cmpcl, null);
  }

  /**
   * 构造函数：完整构造WrappedRecordReader对象
   * 初始化比较器、读取第一个键值对作为头部
   * @param id 在收集器中的位置索引
   * @param rr 被包装的原始RecordReader
   * @param cmpcl 键比较器类
   * @param conf 作业配置
   */
  WrappedRecordReader(int id, RecordReader<K,U> rr,
                      Class<? extends WritableComparator> cmpcl,
                      Configuration conf) throws IOException {
    this.id = id;
    this.rr = rr;
    this.conf = (conf == null) ? new Configuration() : conf;
    // 从原始RecordReader创建头部键值对象
    khead = rr.createKey();
    vhead = rr.createValue();
    try {
      // 初始化键比较器
      cmp = (null == cmpcl)
        ? WritableComparator.get(khead.getClass(), this.conf)
        : cmpcl.newInstance();
    } catch (InstantiationException e) {
      throw (IOException)new IOException().initCause(e);
    } catch (IllegalAccessException e) {
      throw (IOException)new IOException().initCause(e);
    }
    // 初始化值迭代器，读取第一个键值对作为头部
    vjoin = new StreamBackedIterator<U>();
    next();
  }

  /** {@inheritDoc} */
  @Override
  public int id() {
    return id;
  }

  /**
   * 获取当前RecordReader的头部键
   * @return 头部键对象
   */
  public K key() {
    return khead;
  }

  /**
   * 将头部键克隆到提供的对象中
   * @param qkey 接收克隆结果的键对象
   */
  public void key(K qkey) throws IOException {
    WritableUtils.cloneInto(qkey, khead);
  }

  /**
   * 判断当前RecordReader是否还有未读取的键值对
   * @return true 表示还有数据，false 表示已读取完毕
   */
  public boolean hasNext() {
    return !empty;
  }

  /**
   * 跳过所有键小于等于给定键的键值对，移动指针到第一个大于给定键的位置
   * @param key 用于比较的目标键
   */
  public void skip(K key) throws IOException {
    if (hasNext()) {
      while (cmp.compare(khead, key) <= 0 && next());
    }
  }

  /**
   * 从原始RecordReader读取下一个键值对作为新的头部
   * @return true 读取成功，false 原始Reader已耗尽
   */
  protected boolean next() throws IOException {
    empty = !rr.next(khead, vhead);
    return hasNext();
  }

  /**
   * 将当前数据源中与连接键匹配的所有值加入收集器，供连接操作使用
   * @param i Join结果收集器
   * @param key 当前需要连接的目标键
   */
                                 // JoinCollector comes from parent, which has
  @SuppressWarnings("unchecked") // no static type for the slot this sits in
  public void accept(CompositeRecordReader.JoinCollector i, K key)
      throws IOException {
    // 清空上次连接的结果缓存
    vjoin.clear();
    // 如果当前头部键和连接键相等，收集所有相同键的值
    if (0 == cmp.compare(key, khead)) {
      do {
        vjoin.add(vhead);
      } while (next() && 0 == cmp.compare(key, khead));
    }
    // 将当前数据源的值迭代器加入收集器对应位置
    i.add(id, vjoin);
  }

  /**
   * 读取当前头部键值对到提供的对象，然后移动指针到下一个键值对
   * @param key 接收键的对象
   * @param value 接收值的对象
   * @return true 读取成功，false 已无数据
   */
  public boolean next(K key, U value) throws IOException {
    if (hasNext()) {
      WritableUtils.cloneInto(key, khead);
      WritableUtils.cloneInto(value, vhead);
      next();
      return true;
    }
    return false;
  }

  /**
   * 通过代理原始RecordReader创建新键对象
   * @return 新创建的键对象
   */
  public K createKey() {
    return rr.createKey();
  }

  /**
   * 通过代理原始RecordReader创建新值对象
   * @return 新创建的值对象
   */
  public U createValue() {
    return rr.createValue();
  }

  /**
   * 通过代理获取原始RecordReader的读取进度
   * @return 读取进度0.0-1.0
   */
  public float getProgress() throws IOException {
    return rr.getProgress();
  }

  /**
   * 通过代理获取当前读取位置
   * @return 当前位置偏移量
   */
  public long getPos() throws IOException {
    return rr.getPos();
  }

  /**
   * 通过代理关闭原始RecordReader
   */
  public void close() throws IOException {
    rr.close();
  }

  /**
   * 实现ComposableRecordReader的比较接口，比较两个包装Reader头部键的大小
   * @param other 另一个待比较的ComposableRecordReader
   * @return 比较结果：小于0表示当前键更小，0表示相等，大于0表示当前键更大
   */
  public int compareTo(ComposableRecordReader<K,?> other) {
    return cmp.compare(key(), other.key());
  }

  /**
   * 判断两个包装Reader是否相等，基于头部键比较结果
   * @param other 待比较对象
   * @return true 头部键相等，否则false
   */
  @SuppressWarnings("unchecked") // Explicit type check prior to cast
  public boolean equals(Object other) {
    return other instanceof ComposableRecordReader
        && 0 == compareTo((ComposableRecordReader)other);
  }

  /**
   * hashCode方法，此类不设计哈希存储，固定返回常量
   * @return 固定返回42
   */
  public int hashCode() {
    assert false : "hashCode not designed";
    return 42;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }
}