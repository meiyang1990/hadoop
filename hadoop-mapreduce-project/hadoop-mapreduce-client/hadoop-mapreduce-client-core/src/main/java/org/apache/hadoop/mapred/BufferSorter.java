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

import org.apache.hadoop.io.OutputBuffer;
import org.apache.hadoop.io.SequenceFile.Sorter.RawKeyValueIterator;
import org.apache.hadoop.util.Progressable;

/** 
 * MapReduce缓冲区排序接口，为不同排序算法提供统一抽象
 * 
 * 核心使用场景：Map任务输出缓存写入键值对后，需要对缓存内数据进行排序，
 * 该接口定义了排序实现需要遵循的统一规范，框架可通过此接口与具体排序实现解耦。
 * 使用者可根据内存占用情况决定排序时机，接口提供内存占用查询能力。
 */
interface BufferSorter extends JobConfigurable {
  
  /**
   * 设置进度汇报对象，排序过程中可定期汇报进度避免超时
   * @param reporter 进度回调对象引用
   */
  public void setProgressable(Progressable reporter);
    
  /**
   * 添加新写入缓冲区的键值对元数据，供排序实现更新内部数据结构
   * @param recordOffset 键值对在缓冲区中的偏移量
   * @param keyLength 键的字节长度
   * @param valLength 值的字节长度
   */
  public void addKeyValue(int recordoffset, int keyLength, int valLength);
  
  /**
   * 设置存储Map输出的缓冲区，供排序算法间接排序
   * 排序实现一般仅对缓冲区中数据的偏移索引排序，不直接移动缓冲区数据
   * @param buffer Map输出缓冲区
   */
  public void setInputBuffer(OutputBuffer buffer);
  
  /**
   * 获取排序实现内部数据结构已消耗的内存大小，用于判断是否触发溢出排序
   * @return 已消耗内存字节数
   */
  public long getMemoryUtilized();
  
  /**
   * 执行缓冲区排序，返回排序后的键值对迭代器
   * @return 排序后的原始键值对迭代器
   */
  public RawKeyValueIterator sort();
  
  /**
   * 清理排序器资源，完成排序后由框架调用
   */
  public void close();
}