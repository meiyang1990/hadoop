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

import java.util.Comparator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.util.MergeSort;
import org.apache.hadoop.io.SequenceFile.Sorter.RawKeyValueIterator;

/**
 * 文件：MergeSorter.java
 * 所属模块：hadoop-mapreduce-client-core
 * 核心职责：基于归并排序实现Map阶段输出结果的排序器，作为基础排序适配器对接Hadoop通用归并排序实现
 * 
 * 该类继承BasicTypeSorterBase，将Map输出数据结构适配给通用归并排序算法，
 * 封装数据格式转换逻辑，让通用归并排序实现不需要关心Map输出的特定数据结构，
 * 通过Comparator桥接排序比较逻辑，完成Map输出键值对的排序处理。
 */
class MergeSorter extends BasicTypeSorterBase 
implements Comparator<IntWritable> {
  private static int progressUpdateFrequency = 10000;
  private int progressCalls = 0;
  
  /**
   * 对Map输出的键值对执行归并排序，返回排序后的迭代器
   * @return 排序后的键值对原始迭代器
   */
  public RawKeyValueIterator sort() {
    MergeSort m = new MergeSort(this);
    int count = super.count;
    if (count == 0) return null;
    int [] pointers = super.pointers;
    int [] pointersCopy = new int[count];
    // 复制原始索引指针数组，用于排序后保存有序索引
    System.arraycopy(pointers, 0, pointersCopy, 0, count);
    // 调用通用归并排序对索引进行排序
    m.mergeSort(pointers, pointersCopy, 0, count);
    // 封装排序结果为迭代器返回
    return new MRSortResultIterator(super.keyValBuffer, pointersCopy, 
                                    super.startOffsets, super.keyLengths, super.valueLengths);
  }

  /**
   * 实现Comparator接口，对两个索引位置的键进行比较
   * 配合通用归并排序使用，入参为包装在IntWritable中的数据索引
   * @param i 第一个待比较键的索引包装
   * @param j 第二个待比较键的索引包装
   * @return 比较结果：负数表示i<j，0表示相等，正数表示i>j
   */
  public int compare (IntWritable i, IntWritable j) {
    // 批量更新进度，避免频繁调用reporter降低性能
    if (progressCalls < progressUpdateFrequency) {
      progressCalls++;
    } else {
      progressCalls = 0;
      // 上报任务进度给ApplicationMaster
      reporter.progress();
    }  
    // 调用基础比较器，根据索引从缓冲区中取出键进行比较
    return comparator.compare(keyValBuffer.getData(), startOffsets[i.get()],
                              keyLengths[i.get()],
                              keyValBuffer.getData(), startOffsets[j.get()], 
                              keyLengths[j.get()]);
  }
  
  /**
   * 计算排序过程总共占用的内存大小，包含父类基础内存加上归并排序需要的额外内存
   * @return 排序使用的总内存字节数
   */
  public long getMemoryUtilized() {
    // 额外内存为排序需要的临时索引数组，每个int占4字节
    return super.getMemoryUtilized() + super.count * 4; 
  }

}