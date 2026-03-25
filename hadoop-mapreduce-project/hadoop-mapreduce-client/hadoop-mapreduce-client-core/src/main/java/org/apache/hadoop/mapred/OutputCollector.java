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

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * MapReduce旧版API中，用于收集Mapper和Reducer输出键值对的收集器接口
 * 
 * <p>OutputCollector封装了MapReduce框架提供的数据收集能力，可用于收集Mapper的中间输出
 * 、Reducer的最终作业输出，是MapReduce任务输出数据的统一入口</p>
 * 
 * @param <K> 输出键的类型
 * @param <V> 输出值的类型
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public interface OutputCollector<K, V> {
  
  /**
   * 将一个键值对添加到输出中
   *
   * @param key 待收集的输出键
   * @param value 待收集的输出值
   * @throws IOException 输出过程中发生IO异常时抛出
   */
  void collect(K key, V value) throws IOException;
}