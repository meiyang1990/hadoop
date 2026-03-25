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
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.util.Progress;

/**
 * 文件：org.apache.hadoop.mapred.RawKeyValueIterator
 * 模块：hadoop-mapreduce-client-core
 * 核心职责：原始键值对迭代器接口，用于MapReduce中间数据排序/合并阶段遍历未反序列化的原始键值对
 * 
 * <code>RawKeyValueIterator</code> is an iterator used to iterate over
 * the raw keys and values during sort/merge of intermediate data. 
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public interface RawKeyValueIterator {
  /** 
   * 获取当前迭代位置的原始key
   * 
   * @return 包装为DataInputBuffer的原始key数据
   * @throws IO异常
   */
  DataInputBuffer getKey() throws IOException;
  
  /** 
   * 获取当前迭代位置的原始value
   * 
   * @return 包装为DataInputBuffer的原始value数据
   * @throws IO异常
   */
  DataInputBuffer getValue() throws IOException;
  
  /** 
   * 移动迭代器到下一个键值对，填充当前key和value数据供getKey/getValue读取
   * 
   * @return true 存在下一个键值对，false 已经迭代完成
   * @throws IO异常
   */
  boolean next() throws IOException;
  
  /** 
   * 关闭迭代器，释放底层输入流资源
   * 
   * @throws IO异常
   */
  void close() throws IOException;
  
  /** 
   * 获取迭代进度对象，进度值范围0.0-1.0，表示当前已处理的字节占总数据量的比例
   * @return 进度对象
   */
  Progress getProgress();
}