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
import org.apache.hadoop.util.Progressable;

/** 
 * MapReduce作业输出格式抽象接口，定义了MapReduce框架处理输出的规范。
 * 
 * <p>MapReduce框架依赖该接口完成两个核心功能：<p>
 * <ol>
 *   <li>
 *   验证作业输出配置合法性，例如检查输出目录是否已存在，避免覆盖已有数据。
 *   </li>
 *   <li>
 *   提供{@link RecordWriter}实现，用于将作业输出写入到{@link FileSystem}中的输出文件。
 *   </li>
 * </ol>
 * 
 * 该接口是旧版MapReduce API的输出格式抽象，是所有具体输出格式实现的公共接口。
 * 
 * @see RecordWriter
 * @see JobConf
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public interface OutputFormat<K, V> {

  /** 
   * 获取指定作业分片对应的RecordWriter，用于写入该分片的输出数据。
   *
   * @param ignored 已废弃参数，通常传入文件系统对象但未实际使用
   * @param job 当前作业配置对象
   * @param name 当前输出分片的唯一名称
   * @param progress 进度报告回调对象，用于向框架上报写入进度
   * @return 用于写入输出的RecordWriter实例
   * @throws IOException  如果获取RecordWriter过程中发生IO错误
   */
  RecordWriter<K, V> getRecordWriter(FileSystem ignored, JobConf job,
                                     String name, Progressable progress)
  throws IOException;

  /** 
   * 在作业提交前验证输出配置的合法性，防止错误覆盖已有输出。
   *  
   * <p>该方法在作业提交阶段被调用，通常会检查输出路径是否已存在：如果输出已存在则抛出异常，避免覆盖已有数据。
   * 支持委托令牌的文件系统实现，通常会在此方法中收集目标路径的委托令牌并添加到作业配置中。</p>
   *
   * @param ignored 已废弃参数，通常传入文件系统对象但未实际使用
   * @param job 当前作业配置对象
   * @throws IOException 当输出配置不合法，不应该执行作业时抛出
   */
  void checkOutputSpecs(FileSystem ignored, JobConf job) throws IOException;
}