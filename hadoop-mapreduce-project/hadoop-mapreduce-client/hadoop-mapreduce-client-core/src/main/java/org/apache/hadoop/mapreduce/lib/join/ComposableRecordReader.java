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
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.RecordReader;

/**
 * 文件级注释：MapReduce连接操作中可组合RecordReader的抽象基类，定义了连接操作所需的额外接口
 * 为多数据源合并连接提供统一的抽象接口，支持排序后的按键连接操作
 */
/**
 * Additional operations required of a RecordReader to participate in a join.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public abstract class ComposableRecordReader<K extends WritableComparable<?>,
                                             V extends Writable>
    extends RecordReader<K,V>
    implements Comparable<ComposableRecordReader<K,?>> {

  /**
   * 获取当前RecordReader在输入集合中的索引位置
   * @return 索引ID
   */
  abstract int id();

  /**
   * 获取当前RecordReader头部的键对象
   * @return 当前键对象引用
   */
  abstract K key();

  /**
   * 将当前RecordReader头部键克隆到提供的对象中
   * @param key 用于存储克隆结果的目标键对象
   * @throws IOException IO异常
   */
  abstract void key(K key) throws IOException;

  /**
   * 创建一个新的键对象实例
   * @return 新建的键对象
   */
  abstract K createKey();
  
  /**
   * 创建一个新的值对象实例
   * @return 新建的值对象
   */
  abstract V createValue();
  
  /**
   * 检查流中是否还有剩余数据，不保证next调用一定成功
   * @return 流不为空返回true，否则返回false
   */
  abstract boolean hasNext();

  /**
   * 跳过所有键小于等于指定键的键值对
   * @param key 目标比较键
   * @throws IOException IO异常
   * @throws InterruptedException 中断异常
   */
  abstract void skip(K key) throws IOException, InterruptedException;

  /**
   * 将所有匹配指定键的键值对收集到连接收集器中，用于连接操作合并
   * @param jc 连接收集器，用于存储匹配到的所有值
   * @param key 目标匹配键
   * @throws IOException IO异常
   * @throws InterruptedException 中断异常
   */
  @SuppressWarnings("unchecked")
  abstract void accept(CompositeRecordReader.JoinCollector jc, K key) 
      throws IOException, InterruptedException;
}