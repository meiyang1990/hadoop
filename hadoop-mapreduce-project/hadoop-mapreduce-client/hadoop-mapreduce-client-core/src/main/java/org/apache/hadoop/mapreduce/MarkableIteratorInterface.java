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

package org.apache.hadoop.mapreduce;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * 支持标记-重置功能的迭代器接口，扩展了标准Iterator接口
 * 允许在迭代过程中标记位置，之后可以重置迭代器回到标记位置重新遍历，
 * 主要用于MapReduce中需要重复遍历数据的场景
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
interface MarkableIteratorInterface<VALUE> extends Iterator<VALUE> {
  /**
   * 在当前迭代位置标记，后续调用reset会将迭代器回退到该位置
   * @throws IOException 标记过程中发生IO异常时抛出
   */
  void mark() throws IOException;
  
  /**
   * 将迭代器重置到上一次调用mark方法标记的位置
   * @throws IOException 重置过程中发生IO异常时抛出
   */
  void reset() throws IOException;
  
  /**
   * 清除之前设置的所有标记
   * @throws IOException 清除标记过程中发生IO异常时抛出
   */
  void clearMark() throws IOException;
}