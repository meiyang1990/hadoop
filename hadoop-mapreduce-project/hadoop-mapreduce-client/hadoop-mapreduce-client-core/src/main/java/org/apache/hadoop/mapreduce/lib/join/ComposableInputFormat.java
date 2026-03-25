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

package org.apache.hadoop.mapreduce.lib.join;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * 文件定义：MapReduce连接操作支持的可组合InputFormat抽象基类
 * 核心功能：扩展标准InputFormat，要求实现类返回ComposableRecordReader，为多数据源连接操作提供支持
 * 应用场景：用于MapReduce端连接多个输入数据集的场景，允许将多个输入分片按key进行归并连接
 */
/**
 * Refinement of InputFormat requiring implementors to provide
 * ComposableRecordReader instead of RecordReader.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public abstract class ComposableInputFormat<K extends WritableComparable<?>,
                                            V extends Writable>
    extends InputFormat<K,V> {

  /**
   * 创建可组合的RecordReader，用于读取输入分片数据
   * @param split 输入分片
   * @param context 任务尝试上下文
   * @return 用于连接操作的ComposableRecordReader实例
   * @throws IOException IO异常
   * @throws InterruptedException 中断异常
   */
  public abstract ComposableRecordReader<K,V> createRecordReader(
    InputSplit split, TaskAttemptContext context) 
    throws IOException, InterruptedException;

}