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

package org.apache.hadoop.mapreduce.lib.input;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * 文件级注释：SequenceFile格式文件输入格式实现，将SequenceFile中每条记录的键和值转换为Text类型输出
 * 
 * 该类与SequenceFileInputFormat功能类似，但通过自定义RecordReader将原始键值对
 * 调用toString()转为字符串后包装为Text类型，方便需要文本处理的MapReduce作业直接使用
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class SequenceFileAsTextInputFormat
  extends SequenceFileInputFormat<Text, Text> {

  /**
   * 构造函数：初始化SequenceFileAsTextInputFormat实例
   */
  public SequenceFileAsTextInputFormat() {
    super();
  }

  /**
   * 创建用于读取当前输入分片的RecordReader实例，返回将键值转为Text的自定义实现
   * @param split 输入分片信息
   * @param context 任务尝试上下文
   * @return 转换输出为Text类型的RecordReader
   * @throws IOException IO异常
   */
  public RecordReader<Text, Text> createRecordReader(InputSplit split,
      TaskAttemptContext context) throws IOException {
    // 更新任务处理状态，标记当前正在处理的分片
    context.setStatus(split.toString());
    return new SequenceFileAsTextRecordReader();
  }
}