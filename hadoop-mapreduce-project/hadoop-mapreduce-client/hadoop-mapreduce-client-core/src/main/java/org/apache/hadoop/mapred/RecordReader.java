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

package org.apache.hadoop.mapred;

import java.io.Closeable;
import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * 文件名称: RecordReader.java
 * 所属模块: MapReduce 客户端核心模块
 * 核心职责: 定义了从输入分片InputSplit中读取键值对记录的统一接口，为Map任务提供面向记录的输入视图
 * 
 * <code>RecordReader</code> reads &lt;key, value&gt; pairs from an 
 * {@link InputSplit}.
 *   
 * <p><code>RecordReader</code>, typically, converts the byte-oriented view of 
 * the input, provided by the <code>InputSplit</code>, and presents a 
 * record-oriented view for the {@link Mapper} and {@link Reducer} tasks for
 * processing. It thus assumes the responsibility of processing record 
 * boundaries and presenting the tasks with keys and values.</p>
 * 
 * @see InputSplit
 * @see InputFormat
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public interface RecordReader<K, V> extends Closeable{
  /** 
   * 从输入分片读取下一个键值对，用于Map任务处理
   *
   * @param key 存储读取结果的键对象
   * @param value 存储读取结果的值对象
   * @return 成功读取到键值对返回true，读取到输入末尾返回false
   */      
  boolean next(K key, V value) throws IOException;
  
  /**
   * 创建合适类型的键对象实例
   * 
   * @return 新建的键对象
   */
  K createKey();
  
  /**
   * 创建合适类型的值对象实例
   * 
   * @return 新建的值对象
   */
  V createValue();

  /** 
   * 获取当前读取位置在输入分片中的字节偏移量
   * 
   * @return 当前位置的字节偏移量
   * @throws IOException IO异常
   */
  long getPos() throws IOException;

  /** 
   * 关闭当前RecordReader，释放资源
   * 
   * @throws IOException IO异常
   */
  @Override
  public void close() throws IOException;

  /**
   * 获取当前输入分片的处理进度
   * 
   * @return 处理进度，范围从 0.0（未开始）到 1.0（处理完成）
   * @throws IOException IO异常
   */
  float getProgress() throws IOException;
}