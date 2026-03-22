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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
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
 * 文件路径: hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-core/src/main/java/org/apache/hadoop/mapreduce/lib/join/WrappedRecordReader.java
 * 
 * MapReduce连接框架中对原始RecordReader的代理包装类。
 * 该类维护原始RecordReader当前的键值对头部信息，并为参与连接操作的数据源存储匹配当前键的所有值集合，
 * 是MapReduce多数据源连接操作的基础组件。
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class WrappedRecordReader<K extends WritableComparable<?>,
    U extends Writable> extends ComposableRecordReader<K,U> {

  protected boolean empty = false;
  private RecordReader<K,U> rr;
  private int id;  // 当前Reader在结果收集器中的插入位置索引
  protected WritableComparator cmp = null;
  private K key; // 当前Reader头部的键
  private U value; // 当前头部键对应的值
  private ResetableIterator<U> vjoin; // 当前键匹配的所有值的迭代器
  private Configuration conf = new Configuration();
  @SuppressWarnings("unchecked")
  private Class<? extends WritableComparable> keyclass = null; 
  private Class<? extends Writable> valueclass = null; 

  /**
   * 构造指定位置索引的包装RecordReader
   * @param id 在收集器中的位置索引
   */
  protected WrappedRecordReader(int id) {
    this.id = id;
    vjoin = new StreamBackedIterator<U>();
  }
  
  /**
   * 构造包装指定原始RecordReader的包装类
   * @param id 在收集器中的位置索引
   * @param rr 被包装的原始RecordReader
   * @param cmpcl 键比较器类
   * @throws IOException
   * @throws InterruptedException
   */
  WrappedRecordReader(int id, RecordReader<K,U> rr,
      Class<? extends WritableComparator> cmpcl) 
  throws IOException, InterruptedException {
    this.id = id;
    this.rr = rr;
    if (cmpcl != null) {
      try {
        this.cmp = cmpcl.newInstance();
      } catch (InstantiationException e) {
        throw new IOException(e);
      } catch (IllegalAccessException e) {
        throw new IOException(e);
      }
    }
    vjoin = new StreamBackedIterator<U>();
  }

  /**
   * 初始化被包装的原始RecordReader，读取第一个键值对
   * @param split 输入分片
   * @param context 任务尝试上下文
   * @throws IOException
   * @throws InterruptedException
   */
  public void initialize(InputSplit split,
                         TaskAttemptContext context)
  throws IOException, InterruptedException {
    rr.initialize(split, context);
    conf = context.getConfiguration();
    nextKeyValue();
    if (!empty) {
      // 获取键值对类型信息
      keyclass = key.getClass().asSubclass(WritableComparable.class);
      valueclass = value.getClass();
      if (cmp == null) {
        // 如果没有指定比较器，使用默认键比较器
        cmp = WritableComparator.get(keyclass, conf);
      }
    }
  }

  /**
   * 创建新的键对象，使用反射基于原始键类型实例化
   * @return 新创建的键对象
   */
  @SuppressWarnings("unchecked")
  public K createKey() {
    if (keyclass != null) {
      return (K) ReflectionUtils.newInstance(keyclass, conf);
    }
    return (K) NullWritable.get();
  }
  
  /**
   * 创建新的值对象，使用反射基于原始值类型实例化
   * @return 新创建的值对象
   */
  @SuppressWarnings("unchecked")
  public U createValue() {
    if (valueclass != null) {
      return (U) ReflectionUtils.newInstance(valueclass, conf);
    }
    return (U) NullWritable.get();
  }
  
  /** {@inheritDoc} */
  /**
   * 获取当前Reader在收集器中的位置索引
   * @return 位置索引
   */
  public int id() {
    return id;
  }

  /**
   * 获取当前Reader头部的键
   * @return 当前头部键
   */
  public K key() {
    return key;
  }

  /**
   * 将当前头部键拷贝到传入的目标对象中
   * @param qkey 目标键对象
   * @throws IOException
   */
  public void key(K qkey) throws IOException {
    ReflectionUtils.copy(conf, key, qkey);
  }

  /**
   * 判断当前Reader是否还有可读取的键值对
   * @return true表示还有数据，false表示已耗尽
   */
  public boolean hasNext() {
    return !empty;
  }

  /**
   * 跳过所有键小于等于指定键的键值对，用于连接操作中对齐不同数据源的键
   * @param key 目标对齐键
   * @throws IOException
   * @throws InterruptedException
   */
  public void skip(K key) throws IOException, InterruptedException {
    if (hasNext()) {
      while (cmp.compare(key(), key) <= 0 && next());
    }
  }

  /**
   * 将当前键匹配的所有值迭代器注册到连接收集器中
   * 收集当前数据源中所有与连接键匹配的值，供后续连接计算使用
   * @param i 连接收集器
   * @param key 当前连接键
   * @throws IOException
   * @throws InterruptedException
   */
  @SuppressWarnings("unchecked")
  public void accept(CompositeRecordReader.JoinCollector i, K key)
      throws IOException, InterruptedException {
    vjoin.clear();
    if (key() != null && 0 == cmp.compare(key, key())) {
      do {
        vjoin.add(value);
      } while (next() && 0 == cmp.compare(key, key()));
    }
    i.add(id, vjoin);
  }

  /**
   * 读取下一个键值对到当前头部，仅当还有数据时返回true
   * @return true表示成功读取到下一个键值对
   * @throws IOException
   * @throws InterruptedException
   */
  public boolean nextKeyValue() throws IOException, InterruptedException {
    if (hasNext()) {
      next();
      return true;
    }
    return false;
  }

  /**
   * 从被包装的RecordReader读取下一个键值对，更新当前头部状态
   * @return true表示成功读取到下一个键值对
   * @throws IOException
   * @throws InterruptedException
   */
  private boolean next() throws IOException, InterruptedException {
    empty = !rr.nextKeyValue();
    key = rr.getCurrentKey();
    value = rr.getCurrentValue();
    return !empty;
  }

  /**
   * 获取当前键，代理调用原始RecordReader方法
   * @return 当前键
   * @throws IOException
   * @throws InterruptedException
   */
  public K getCurrentKey() throws IOException, InterruptedException {
    return rr.getCurrentKey();
  }

  /**
   * 获取当前值，代理调用原始RecordReader方法
   * @return 当前值
   * @throws IOException
   * @throws InterruptedException
   */
  public U getCurrentValue() throws IOException, InterruptedException {
    return rr.getCurrentValue();
  }

  /**
   * 获取读取进度，代理调用原始RecordReader方法
   * @return 进度值[0-1]
   * @throws IOException
   * @throws InterruptedException
   */
  public float getProgress() throws IOException, InterruptedException {
    return rr.getProgress();
  }

  /**
   * 关闭Reader，代理调用原始RecordReader的close方法
   * @throws IOException
   */
  public void close() throws IOException {
    rr.close();
  }

  /**
   * 比较当前Reader头部键和另一个ComposableRecordReader头部键，实现Comparable接口
   * @param other 另一个待比较的RecordReader
   * @return 比较结果，小于0表示当前键更小，0表示相等，大于0表示当前键更大
   */
  public int compareTo(ComposableRecordReader<K,?> other) {
    return cmp.compare(key(), other.key());
  }

  /**
   * 判断两个包装Reader是否相等，基于头部键的比较结果
   * @param other 另一个待比较对象
   * @return true如果键相等
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
}