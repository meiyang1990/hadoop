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
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

/** 
 * InputFormat 定义了MapReduce作业的输入规范，为MapReduce框架提供输入处理能力。
 * 
 * <p>MapReduce框架依赖InputFormat完成以下核心工作：<p>
 * <ol>
 *   <li>
 *   验证作业输入规格的合法性。
 *   </li>
 *   <li>
 *   将输入文件切分为逻辑切片{@link InputSplit}，每个切片分配给一个独立的{@link Mapper}处理。
 *   </li>
 *   <li>
 *   提供{@link RecordReader}实现，用于从逻辑InputSplit中读取输入记录，供Mapper处理。
 *   </li>
 * </ol>
 * 
 * <p>基于文件的InputFormat（通常是{@link FileInputFormat}的子类）的默认行为是根据输入文件的总大小（字节）
 * 将输入切分为逻辑切片，输入文件的{@link FileSystem}块大小被视为输入切片的上限，切片大小的下限可以通过
 * <a href="{@docRoot}/../hadoop-mapreduce-client/hadoop-mapreduce-client-core/mapred-default.xml#mapreduce.input.fileinputformat.split.minsize">
 * mapreduce.input.fileinputformat.split.minsize</a>配置。</p>
 * 
 * <p>显然，仅基于输入大小的逻辑切分在很多应用场景下是不够的，因为需要保证记录边界不被破坏。在这种场景下，
 * 应用需要实现自定义{@link RecordReader}，由RecordReader负责保证记录边界，并为Mapper提供逻辑切片的面向记录的视图。
 *
 * @see InputSplit
 * @see RecordReader
 * @see FileInputFormat
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public abstract class InputFormat<K, V> {

  /** 
   * 为作业切分输入，生成逻辑切片列表。
   * 
   * <p>每个InputSplit会被分配给一个独立的Mapper处理。</p>
   *
   * <p><i>注意</i>: 切分是对输入的逻辑划分，不会对输入文件进行物理切分。例如，一个切片通常
   * 是<输入文件路径, 起始偏移, 长度>这样的元组。InputFormat还需要提供RecordReader来读取切片内容。
   * 
   * @param context 作业上下文，包含作业配置信息
   * @return 当前作业所有逻辑切片组成的列表
   * @throws IOException 切分过程中发生IO异常
   * @throws InterruptedException 切分过程被中断
   */
  public abstract 
    List<InputSplit> getSplits(JobContext context
                               ) throws IOException, InterruptedException;
  
  /**
   * 为指定切片创建记录读取器，框架会在使用切片前调用{@link RecordReader#initialize(InputSplit, TaskAttemptContext)}
   * 初始化读取器。
   * @param split 需要读取的逻辑切片
   * @param context 任务尝试上下文，包含任务相关信息
   * @return 用于读取切片的新记录读取器实例
   * @throws IOException 创建读取器过程中发生IO异常
   * @throws InterruptedException 创建读取器过程被中断
   */
  public abstract 
    RecordReader<K,V> createRecordReader(InputSplit split,
                                         TaskAttemptContext context
                                        ) throws IOException, 
                                                 InterruptedException;

}