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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * 固定长度记录输入格式，用于读取由固定长度记录组成的输入文件。
 * 记录内容可以是任意二进制数据，不要求必须是文本格式。
 * 使用前必须通过 FixedLengthInputFormat.setRecordLength(conf, recordLength) 
 * 或 conf.setInt(FixedLengthInputFormat.FIXED_RECORD_LENGTH, recordLength) 配置记录长度。
 * 
 * @see FixedLengthRecordReader
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class FixedLengthInputFormat
    extends FileInputFormat<LongWritable, BytesWritable> {

  /** 固定记录长度配置项名称 */
  public static final String FIXED_RECORD_LENGTH =
      "fixedlengthinputformat.record.length"; 

  /**
   * 设置每条记录的长度（字节数）
   * @param conf 作业配置对象
   * @param recordLength 记录长度（字节）
   */
  public static void setRecordLength(Configuration conf, int recordLength) {
    conf.setInt(FIXED_RECORD_LENGTH, recordLength);
  }

  /**
   * 从配置中获取固定记录长度
   * @param conf 作业配置对象
   * @return 配置的记录长度，返回0表示未配置
   */
  public static int getRecordLength(Configuration conf) {
    return conf.getInt(FIXED_RECORD_LENGTH, 0);
  }

  /**
   * 创建固定长度记录读取器，验证记录长度配置并初始化读取器
   */
  @Override
  public RecordReader<LongWritable, BytesWritable>
      createRecordReader(InputSplit split, TaskAttemptContext context)
      throws IOException, InterruptedException {
    // 从任务配置中获取记录长度
    int recordLength = getRecordLength(context.getConfiguration());
    // 验证记录长度合法性，必须大于0
    if (recordLength <= 0) {
      throw new IOException("Fixed record length " + recordLength
          + " is invalid.  It should be set to a value greater than zero");
    }
    // 返回初始化好的固定长度记录读取器
    return new FixedLengthRecordReader(recordLength);
  }

  /**
   * 判断输入文件是否可切片，压缩文件不可切片
   */
  @Override
  protected boolean isSplitable(JobContext context, Path file) {
    // 检查文件是否存在对应的压缩编解码器
    final CompressionCodec codec = 
        new CompressionCodecFactory(context.getConfiguration()).getCodec(file);
    // 只有未压缩的文件可以切片
    return (null == codec);
  } 

}