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
import org.apache.hadoop.io.Closeable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.compress.CompressionCodec;

/** 
 * MapReduce Map阶段核心接口，将输入的键值对映射为一组中间键值对。
 * 
 * <p>Map任务负责将输入记录转换为中间记录，转换后的中间记录类型不必与输入记录相同，一个输入键值对可以映射为0个或多个输出键值对。</p>
 * 
 * <p>Hadoop MapReduce框架会为作业的{@link InputFormat}生成的每个{@link InputSplit}启动一个Map任务。
 * <code>Mapper</code>实现类可以通过{@link JobConfigurable#configure(JobConf)}获取作业的{@link JobConf}配置
 * 并完成自身初始化，通过{@link Closeable#close()}方法完成反初始化和资源清理。</p>
 * 
 * <p>框架会对当前输入分片的每个键值对调用{@link #map(Object, Object, OutputCollector, Reporter)}方法处理。</p>
 * 
 * <p>所有同一输出键对应的中间值会被框架自动分组，然后传递给{@link Reducer}生成最终输出。
 * 用户可以通过{@link JobConf#setOutputKeyComparatorClass(Class)}指定自定义分组比较器，控制分组逻辑。</p>
 *
 * <p>Mapper输出的分组结果会按照Reducer分区，用户可以通过实现自定义{@link Partitioner}控制哪些键分配给哪个Reducer。
 * 
 * <p>用户可以可选地通过{@link JobConf#setCombinerClass(Class)}指定combiner，对Mapper输出的中间结果进行本地聚合，
 * 减少Mapper到Reducer的数据传输量，提升整体性能。
 * 
 * <p>Mapper输出的中间分组结果默认存储在{@link SequenceFile}中，应用可以通过<code>JobConf</code>
 * 配置指定是否压缩中间结果，以及使用哪种{@link CompressionCodec}进行压缩。</p>
 *  
 * <p>如果作业设置了0个Reduce任务，那么Mapper的输出会直接写入{@link FileSystem}，不需要按键分组。</p>
 * 
 * @see JobConf
 * @see InputFormat
 * @see Partitioner  
 * @see Reducer
 * @see MapReduceBase
 * @see MapRunnable
 * @see SequenceFile
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public interface Mapper<K1, V1, K2, V2> extends JobConfigurable, Closeable {
  
  /** 
   * 处理单个输入键值对，生成对应的中间键值对输出。
   * 
   * <p>输出键值对类型不必与输入一致，一个输入键值对可以生成0个或多个输出键值对。输出结果通过{@link OutputCollector#collect(Object,Object)}收集。</p>
   *
   * <p>应用可以使用传入的{@link Reporter}报告进度或标记任务存活。当处理单个键值对需要较长时间时，
   * 报告进度非常关键，否则框架可能会认为任务超时并将其杀死。也可以通过将
   * <a href="{@docRoot}/../hadoop-mapreduce-client/hadoop-mapreduce-client-core/mapred-default.xml#mapreduce.task.timeout">
   * mapreduce.task.timeout</a>设置为足够大的值（或0，代表禁用超时）避免任务被误杀。</p>
   * 
   * @param key 输入键
   * @param value 输入值
   * @param output 收集Mapper输出键值对的收集器
   * @param reporter 用于报告任务进度的工具
   * @throws IOException 处理过程中发生IO异常时抛出
   */
  void map(K1 key, V1 value, OutputCollector<K2, V2> output, Reporter reporter)
  throws IOException;
}