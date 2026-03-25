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
import org.apache.hadoop.io.Text;

/**
 * 将SequenceFile文件转换为文本格式输入的InputFormat实现
 * 继承自SequenceFileInputFormat，核心区别是使用自定义RecordReader
 * 将SequenceFile中原始键值对转换为字符串类型，方便MapReduce直接按文本处理
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class SequenceFileAsTextInputFormat
  extends SequenceFileInputFormat<Text, Text> {

  /**
   * 构造函数，初始化SequenceFile文本输入格式
   */
  public SequenceFileAsTextInputFormat() {
    super();
  }

  /**
   * 创建并返回对应输入分片的RecordReader，负责读取SequenceFile并转换为Text键值对
   * @param split 输入分片
   * @param job 作业配置
   * @param reporter 进度报告器
   * @return 转换为文本格式的RecordReader实例
   * @throws IOException IO异常
   */
  public RecordReader<Text, Text> getRecordReader(InputSplit split,
                                                  JobConf job,
                                                  Reporter reporter)
    throws IOException {

    // 上报当前分片处理进度
    reporter.setStatus(split.toString());

    // 返回转换为文本的SequenceFile记录读取器
    return new SequenceFileAsTextRecordReader(job, (FileSplit) split);
  }
}