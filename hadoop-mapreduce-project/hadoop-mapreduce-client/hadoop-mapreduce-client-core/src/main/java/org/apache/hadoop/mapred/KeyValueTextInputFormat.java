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
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;

/**
 * 面向键值对格式文本文件的InputFormat实现，是MapReduce旧API体系中的文本输入格式。
 * 将文本文件按行切分，每行通过指定分隔符拆分出key和value两部分，若行中无分隔符则整行作为key，value为空。
 * 适合处理每行一条记录、键值用分隔符分隔的文本输入数据。
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class KeyValueTextInputFormat extends FileInputFormat<Text, Text>
  implements JobConfigurable {

  private CompressionCodecFactory compressionCodecs = null;

  /**
   * 作业配置初始化方法，从作业配置中创建压缩编解码器工厂
   * @param conf 作业配置对象
   */
  @Override
  public void configure(JobConf conf) {
    compressionCodecs = new CompressionCodecFactory(conf);
  }

  /**
   * 判断指定输入文件是否可切分，用于决定是否将文件拆分给多个Map任务处理
   * @param fs 文件系统对象
   * @param file 待检查的输入文件路径
   * @return 未压缩文件/支持切分的压缩格式返回true，不可切分的压缩格式返回false
   */
  @Override
  protected boolean isSplitable(FileSystem fs, Path file) {
    final CompressionCodec codec = compressionCodecs.getCodec(file);
    if (null == codec) {
      return true;
    }
    return codec instanceof SplittableCompressionCodec;
  }

  /**
   * 获取指定输入分片的记录读取器，用于读取分片中的键值对记录
   * @param genericSplit 待读取的输入分片
   * @param job 作业配置对象
   * @param reporter 任务进度汇报器
   * @return 键值对格式行记录读取器实例
   * @throws IOException 读取分片信息失败时抛出IO异常
   */
  public RecordReader<Text, Text> getRecordReader(InputSplit genericSplit,
                                                  JobConf job,
                                                  Reporter reporter)
    throws IOException {

    reporter.setStatus(genericSplit.toString());
    return new KeyValueLineRecordReader(job, (FileSplit) genericSplit);
  }

}