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
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.RecordReader;

/**
 * 文件级注释：MapReduce连接操作中可组合RecordReader接口，定义了参与多数据源连接操作所需的额外方法
 * 定义了参与MapReduce连接操作的RecordReader需要实现的额外能力，
 * 支持对多个输入数据源按key进行归并连接，是MapReduce端连接功能的核心接口。
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public interface ComposableRecordReader<K extends WritableComparable,
                                 V extends Writable>
    extends RecordReader<K,V>, Comparable<ComposableRecordReader<K,?>> {

  /**
   * 获取当前RecordReader在连接集合中的索引位置
   * @return 集合中的位置编号
   */
  int id();

  /**
   * 获取当前RecordReader迭代到的key对象
   * @return 当前key对象
   */
  K key();

  /**
   * 将当前RecordReader头部的key克隆到提供的对象中
   * @param key 接收克隆数据的目标key对象
   * @throws IOException IO异常
   */
  void key(K key) throws IOException;

  /**
   * 检查当前流是否还有可读取的key-value对，仅作非空判断，不保证next调用一定成功
   * @return 流非空返回true，否则返回false
   */
  boolean hasNext();

  /**
   * 跳过所有key小于等于指定key的记录
   * @param key 目标key，所有小于等于它的记录都会被跳过
   * @throws IOException IO异常
   */
  void skip(K key) throws IOException;

  /**
   * 将所有与给定key匹配的当前RecordReader中的记录注册到JoinCollector，用于连接操作
   * @param jc 连接收集器，用于收集相同key的多源记录
   * @param key 待匹配的目标key
   * @throws IOException IO异常
   */
  void accept(CompositeRecordReader.JoinCollector jc, K key) throws IOException;
}