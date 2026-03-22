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

import java.io.*;
import java.nio.charset.StandardCharsets;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.*;

/**
 * 文本文件输入格式，用于处理普通文本文件的MapReduce输入。
 * 将文件按行切分，键为行在文件中的字节偏移量，值为行文本内容，支持换行符(\n)或回车符(\r)作为行结束标记。
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class TextInputFormat extends FileInputFormat<LongWritable, Text>
  implements JobConfigurable {

  private CompressionCodecFactory compressionCodecs = null;
  
  /**
   * 作业配置初始化方法，创建压缩编解码器工厂用于处理压缩文本文件。
   * @param conf 作业配置对象
   */
  public void configure(JobConf conf) {
    compressionCodecs = new CompressionCodecFactory(conf);
  }
  
  /**
   * 判断指定文件是否可切分，支持可切分压缩格式。
   * @param fs 文件系统对象
   * @param file 待判断的文件路径
   * @return 未压缩或使用可切分压缩格式时返回true，否则返回false
   */
  protected boolean isSplitable(FileSystem fs, Path file) {
    final CompressionCodec codec = compressionCodecs.getCodec(file);
    if (null == codec) {
      return true;
    }
    return codec instanceof SplittableCompressionCodec;
  }

  /**
   * 创建记录读取器，用于从输入分片读取文本行记录。
   * @param genericSplit 输入分片对象
   * @param job 作业配置对象
   * @param reporter 进度汇报器
   * @return 按行读取的文本记录读取器
   * @throws IOException 文件读取异常
   */
  public RecordReader<LongWritable, Text> getRecordReader(
                                          InputSplit genericSplit, JobConf job,
                                          Reporter reporter)
    throws IOException {
    
    // 更新任务进度状态
    reporter.setStatus(genericSplit.toString());
    // 从配置中读取自定义记录分隔符
    String delimiter = job.get("textinputformat.record.delimiter");
    byte[] recordDelimiterBytes = null;
    // 如果配置了自定义分隔符，转换为UTF-8字节数组
    if (null != delimiter) {
      recordDelimiterBytes = delimiter.getBytes(StandardCharsets.UTF_8);
    }
    // 创建并返回LineRecordReader读取文本行
    return new LineRecordReader(job, (FileSplit) genericSplit,
        recordDelimiterBytes);
  }
}