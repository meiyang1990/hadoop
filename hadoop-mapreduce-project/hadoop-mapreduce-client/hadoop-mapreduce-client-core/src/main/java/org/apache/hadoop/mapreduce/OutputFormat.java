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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FileSystem;

/** 
 * OutputFormat 定义MapReduce作业的输出规范，是MapReduce框架输出阶段的核心抽象接口。
 * 
 * <p>MapReduce框架依赖OutputFormat完成两项核心工作：<p>
 * <ol>
 *   <li>
 *   校验作业的输出配置，例如检查输出目录是否已经存在，避免覆盖已有数据。
 *   <li>
 *   提供{@link RecordWriter}实现，用于将作业输出写入到{@link FileSystem}中的输出文件。
 *   </li>
 * </ol>
 * 
 * 不同输出格式（文本、序列文件等）提供不同的实现，框架通过统一接口处理输出逻辑。
 * 
 * @see RecordWriter
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public abstract class OutputFormat<K, V> {

  /** 
   * 获取当前任务对应的RecordWriter实例，用于写入任务输出结果。
   *
   * @param context 当前任务的上下文信息，包含任务配置与运行状态
   * @return 用于写入作业输出的RecordWriter实例
   * @throws IOException  如果获取或初始化写入器时发生IO错误
   * @throws InterruptedException 如果线程被中断
   */
  public abstract RecordWriter<K, V> 
    getRecordWriter(TaskAttemptContext context
                    ) throws IOException, InterruptedException;

  /** 
   * 作业提交时校验输出配置的有效性，保障输出不会误覆盖已有数据。
   *  
   * <p>通常检查输出路径是否已存在，如果存在则抛出异常阻止作业运行，避免数据被覆盖。
   * 支持委托令牌的文件系统实现，通常会在此方法中收集目标路径的委托令牌并添加到作业配置中。</p>
   *
   * @param context 作业的上下文信息，包含作业配置
   * @throws IOException 当输出配置无效或发生IO错误时抛出
   * @throws InterruptedException 如果线程被中断
   */
  public abstract void checkOutputSpecs(JobContext context
                                        ) throws IOException, 
                                                 InterruptedException;

  /**
   * 获取当前输出格式对应的输出提交器，输出提交器负责保障作业输出的原子性提交。
   * 负责处理作业初始化、任务输出暂存、最终提交和清理等阶段逻辑。
   * @param context 当前任务尝试的上下文信息
   * @return 对应输出格式的输出提交器实例
   * @throws IOException 如果获取提交器时发生IO错误
   * @throws InterruptedException 如果线程被中断
   */
  public abstract 
  OutputCommitter getOutputCommitter(TaskAttemptContext context
                                     ) throws IOException, InterruptedException;
}