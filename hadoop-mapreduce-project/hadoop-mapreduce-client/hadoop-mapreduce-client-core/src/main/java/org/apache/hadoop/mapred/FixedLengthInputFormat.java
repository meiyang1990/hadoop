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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;

/**
 * 固定长度记录输入格式，用于读取每条记录长度固定的输入文件。
 * 记录内容可以是任意二进制数据，不一定要求是文本格式。
 * 用户必须通过调用 FixedLengthInputFormat.setRecordLength(conf, recordLength)
 * 或者 conf.setInt(FixedLengthInputFormat.FIXED_RECORD_LENGTH, recordLength)
 * 来配置单条记录的长度。
 * 
 * @see FixedLengthRecordReader
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class FixedLengthInputFormat
    extends FileInputFormat<LongWritable, BytesWritable>
    implements JobConfigurable {

  private CompressionCodecFactory compressionCodecs = null;
  
  /** 配置项：固定单条记录长度 */
  public static final String FIXED_RECORD_LENGTH =
      "fixedlengthinputformat.record.length"; 

  /**
   * 设置单条记录的长度
   * @param conf 作业配置对象
   * @param recordLength 单条记录的字节长度
   */
  public static void setRecordLength(Configuration conf, int recordLength) {
    conf.setInt(FIXED_RECORD_LENGTH, recordLength);
  }

  /**
   * 获取配置的单条记录长度
   * @param conf 作业配置对象
   * @return 配置的记录长度，返回0表示未进行配置
   */
  public static int getRecordLength(Configuration conf) {
    return conf.getInt(FIXED_RECORD_LENGTH, 0);
  }

  /**
   * 作业配置初始化，创建压缩编码器工厂
   * @param conf 作业配置对象
   */
  @Override
  public void configure(JobConf conf) {
    compressionCodecs = new CompressionCodecFactory(conf);
  }

  /**
   * 为输入分片创建对应的记录读取器，验证记录长度配置并返回FixedLengthRecordReader实例
   * @param genericSplit 待处理的输入分片
   * @param job 作业配置对象
   * @param reporter 进度上报Reporter
   * @return 固定长度记录读取器实例
   * @throws IOException 如果记录长度配置非法则抛出异常
   */
  @Override
  public RecordReader<LongWritable, BytesWritable>
      getRecordReader(InputSplit genericSplit, JobConf job, Reporter reporter)
      throws IOException {
    reporter.setStatus(genericSplit.toString());
    int recordLength = getRecordLength(job);
    if (recordLength <= 0) {
      throw new IOException("Fixed record length " + recordLength
          + " is invalid.  It should be set to a value greater than zero");
    }
    return new FixedLengthRecordReader(job, (FileSplit)genericSplit,
                                       recordLength);
  }

  /**
   * 判断当前文件是否可以分片切分
   * @param fs 文件系统对象
   * @param file 待判断的文件路径
   * @return 未压缩文件返回true（可分片），已压缩文件返回false（不可分片）
   */
  @Override
  protected boolean isSplitable(FileSystem fs, Path file) {
    final CompressionCodec codec = compressionCodecs.getCodec(file);
    return(null == codec);
  }

}