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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.nio.charset.StandardCharsets;

/**
 * 文本文件输入格式，是MapReduce处理纯文本文件的默认输入格式实现。
 * 将文件按行切分，使用换行符或回车符作为行结束标记，
 * 键为当前行在文件中的字节偏移量，值为当前行的文本内容。
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class TextInputFormat extends FileInputFormat<LongWritable, Text> {

  /**
   * 创建用于读取文本行的RecordReader实例，支持自定义记录分隔符。
   * @param split 输入分片
   * @param context 任务尝试上下文
   * @return 文本行记录读取器实例
   */
  @Override
  public RecordReader<LongWritable, Text> 
    createRecordReader(InputSplit split,
                       TaskAttemptContext context) {
    // 从配置中读取自定义记录分隔符
    String delimiter = context.getConfiguration().get(
        "textinputformat.record.delimiter");
    byte[] recordDelimiterBytes = null;
    // 如果配置了自定义分隔符，转换为UTF-8字节数组
    if (null != delimiter)
      recordDelimiterBytes = delimiter.getBytes(StandardCharsets.UTF_8);
    return new LineRecordReader(recordDelimiterBytes);
  }

  /**
   * 判断输入文件是否可切分，决定是否可以将文件分割为多个分片并行处理。
   * @param context 作业上下文
   * @param file 待判断的输入文件路径
   * @return 是否可切分：未压缩文件返回true；仅可切分压缩格式返回true，其他压缩格式返回false
   */
  @Override
  protected boolean isSplitable(JobContext context, Path file) {
    // 根据文件获取对应的压缩编解码器
    final CompressionCodec codec =
      new CompressionCodecFactory(context.getConfiguration()).getCodec(file);
    // 未压缩文件可切分
    if (null == codec) {
      return true;
    }
    // 只有实现了SplittableCompressionCodec的压缩格式才可切分
    return codec instanceof SplittableCompressionCodec;
  }

}