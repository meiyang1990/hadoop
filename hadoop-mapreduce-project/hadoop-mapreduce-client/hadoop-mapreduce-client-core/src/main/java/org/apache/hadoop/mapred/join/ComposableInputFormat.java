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

package org.apache.hadoop.mapred.join;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;

/**
 * 文件说明: MapReduce Join分组连接模块的可组合输入格式接口
 * 核心职责: 对标准InputFormat接口进行扩展，要求实现类返回自定义的ComposableRecordReader
 * 用于支持多个输入数据源的合并连接操作
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public interface ComposableInputFormat<K extends WritableComparable,
                                       V extends Writable>
    extends InputFormat<K,V> {

  /**
   * 获取可组合记录读取器，用于支持多输入连接场景
   * @param split 输入分片信息
   * @param job 作业配置对象
   * @param reporter 进度报告器
   * @return 适配多输入连接的ComposableRecordReader实例
   * @throws IOException 读取分片时发生IO异常
   */
  ComposableRecordReader<K,V> getRecordReader(InputSplit split,
      JobConf job, Reporter reporter) throws IOException;
}