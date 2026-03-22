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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.Writable;

/**
 * 文件说明：MapReduce旧版API中可重置迭代器接口定义，用于MapReduce连接操作中
 * 支持重复迭代读取已添加的元素，是MapReduce端连接操作的核心基础接口
 * 
 * 定义了支持重置状态、可重放元素的有状态迭代器接口，
 * 可以直接重播已添加到迭代器中的元素。
 * 注意：此接口不继承 {@link java.util.Iterator}。
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public interface ResetableIterator<T extends Writable> 
    extends org.apache.hadoop.mapreduce.lib.join.ResetableIterator<T> {

  /**
   * 空迭代器实现类，空对象模式，用于表示没有元素的空迭代器
   * 继承新版Hadoop API的空迭代器实现，实现旧版接口兼容
   * @param <U> 迭代元素类型，必须实现Writable接口
   */
  public static class EMPTY<U extends Writable>
      extends org.apache.hadoop.mapreduce.lib.join.ResetableIterator.EMPTY<U>
      implements ResetableIterator<U> {
  }
}