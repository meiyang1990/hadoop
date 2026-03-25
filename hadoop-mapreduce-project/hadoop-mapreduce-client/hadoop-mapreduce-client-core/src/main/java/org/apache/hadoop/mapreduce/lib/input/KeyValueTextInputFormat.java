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
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * 键值对文本输入格式，用于处理纯文本键值对文件。
 * 将文件按行分割，每行通过指定分隔符分割为键和值两部分，默认分隔符为制表符('\t')。
 * 如果行中不存在分隔符，则整行作为键，值为空字符串。
 * 分隔符可通过配置参数 mapreduce.input.keyvaluelinerecordreader.key.value.separator 指定。
 * 继承自FileInputFormat，用于文本类型的MapReduce输入处理。
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class KeyValueTextInputFormat extends FileInputFormat<Text, Text> {

  /**
   * 判断输入文件是否可分割，仅支持可分割压缩格式或未压缩文件进行分片。
   * @param context 作业上下文
   * @param file 待检查的输入文件路径
   * @return 如果文件可分割返回true，否则返回false
   */
  @Override
  protected boolean isSplitable(JobContext context, Path file) {
    // 根据文件获取对应的压缩编解码器
    final CompressionCodec codec =
      new CompressionCodecFactory(context.getConfiguration()).getCodec(file);
    // 未压缩文件可分割
    if (null == codec) {
      return true;
    }
    // 仅支持可分割压缩编解码器分割文件
    return codec instanceof SplittableCompressionCodec;
  }

  /**
   * 创建键值对行记录读取器，用于从输入分片中读取键值对记录。
   * @param genericSplit 输入分片
   * @param context 任务尝试上下文
   * @return 键值对行记录读取器实例
   * @throws IOException 创建读取器时IO异常
   */
  public RecordReader<Text, Text> createRecordReader(InputSplit genericSplit,
      TaskAttemptContext context) throws IOException {
    
    // 更新任务状态，标记当前处理的分片
    context.setStatus(genericSplit.toString());
    // 根据任务配置创建键值对行记录读取器
    return new KeyValueLineRecordReader(context.getConfiguration());
  }

}