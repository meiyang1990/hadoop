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
import org.apache.hadoop.fs.FileSystem;

/** 
 * InputFormat 接口定义了MapReduce作业的输入规范，是MapReduce旧版API中输入处理的核心抽象。
 * 
 * <p>MapReduce框架依赖InputFormat完成三个核心职责：<p>
 * <ol>
 *   <li>
 *   验证作业的输入配置是否合法
 *   </li>
 *   <li>
 *   将输入文件切分为逻辑{@link InputSplit}分片，每个分片分配给一个独立的{@link Mapper}处理
 *   </li>
 *   <li>
 *   提供{@link RecordReader}实现，用于从逻辑分片中读取键值对记录，供Mapper处理
 *   </li>
 * </ol>
 * 
 * <p>基于文件的InputFormat（通常是{@link FileInputFormat}的子类）默认切分逻辑是：
 * 根据输入文件总字节数切分为逻辑分片，分片大小上限为HDFS文件块大小，下限可通过配置参数
 * <a href="{@docRoot}/../hadoop-mapreduce-client/hadoop-mapreduce-client-core/mapred-default.xml#mapreduce.input.fileinputformat.split.minsize">
 * mapreduce.input.fileinputformat.split.minsize</a>设置。</p>
 * 
 * <p>基于大小的逻辑切分无法满足所有应用场景，对于需要保证记录边界的场景，应用需要自行实现
 * {@link RecordReader}，由RecordReader负责处理记录边界，为Mapper提供面向记录的分片视图。
 *
 * @see InputSplit
 * @see RecordReader
 * @see JobClient
 * @see FileInputFormat
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public interface InputFormat<K, V> {

  /** 
   * 对作业的输入文件集合进行逻辑切分，生成输入分片数组。
   * 
   * <p>每个输入分片会被分配给一个独立的Mapper处理。</p>
   *
   * <p><i>注意</i>: 切分是对输入的逻辑划分，输入文件不会被物理切割。例如分片可以表示为
   * <i>&lt;输入文件路径, 起始偏移, 长度&gt;</i>三元组。
   * 
   * @param job 作业配置对象
   * @param numSplits 期望的分片数量，仅供参考
   * @return 作业的输入分片数组
   * @throws IOException 切分过程中发生IO异常时抛出
   */
  InputSplit[] getSplits(JobConf job, int numSplits) throws IOException;

  /** 
   * 获取指定输入分片对应的RecordReader，用于读取分片中的键值对记录。
   *
   * <p>RecordReader需要负责在处理逻辑分片时保证记录边界，为任务提供面向记录的视图。</p>
   * 
   * @param split 待处理的输入分片
   * @param job 该分片所属作业的配置
   * @param reporter 任务进度上报器
   * @return 用于读取分片记录的RecordReader
   * @throws IOException 获取或初始化Reader过程中发生IO异常时抛出
   */
  RecordReader<K, V> getRecordReader(InputSplit split,
                                     JobConf job, 
                                     Reporter reporter) throws IOException;
}