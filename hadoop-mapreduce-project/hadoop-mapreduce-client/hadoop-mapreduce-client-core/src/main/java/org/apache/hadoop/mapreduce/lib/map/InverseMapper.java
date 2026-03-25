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

package org.apache.hadoop.mapreduce.lib.map;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 反转Map输入键值对的Mapper实现类
 * 核心功能是将输入的<键, 值>对交换顺序，输出<值, 键>，常用于需要反转键值关系的MapReduce作业
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class InverseMapper<K, V> extends Mapper<K,V,V,K> {

  /**
   * 反转输入键值对并输出
   * @param key 输入键
   * @param value 输入值
   * @param context MapReduce上下文对象
   * @throws IOException IO异常
   * @throws InterruptedException 中断异常
   */
  @Override
  public void map(K key, V value, Context context
                  ) throws IOException, InterruptedException {
    // 交换键值顺序后输出
    context.write(value, key);
  }
  
}