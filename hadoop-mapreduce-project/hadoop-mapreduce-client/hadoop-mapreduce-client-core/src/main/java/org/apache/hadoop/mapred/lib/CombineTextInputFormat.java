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

package org.apache.hadoop.mapred.lib;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;

/**
 * 适配TextInputFormat的CombineFileInputFormat实现，将多个小文本文件合并为一个分片，减少Map任务数量提升执行效率
 * 
 * 适用于大量小文本文件的处理场景，通过合并分片降低Map任务调度开销
 * @see CombineFileInputFormat
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class CombineTextInputFormat
  extends CombineFileInputFormat<LongWritable,Text> {

  /**
   * 创建适用于合并分片的RecordReader，读取多个小文本文件
   * @param split 合并后的输入分片
   * @param conf 作业配置对象
   * @param reporter 进度汇报器
   * @return 合并分片的RecordReader实例
   * @throws IOException 读取分片失败时抛出IO异常
   */
  @SuppressWarnings({ "rawtypes", "unchecked" })
  public RecordReader<LongWritable,Text> getRecordReader(InputSplit split,
    JobConf conf, Reporter reporter) throws IOException {
    return new CombineFileRecordReader(conf, (CombineFileSplit)split, reporter,
      TextRecordReaderWrapper.class);
  }

  /**
   * 文本文件分片读取包装器，为CombineFileRecordReader提供适配TextInputFormat的底层读取能力
   * 
   * 负责读取合并分片中单个小文本文件的键值对，使用原生TextInputFormat处理实际文件读取
   * @see CombineFileRecordReader
   * @see CombineFileInputFormat
   * @see TextInputFormat
   */
  private static class TextRecordReaderWrapper
    extends CombineFileRecordReaderWrapper<LongWritable,Text> {
    // this constructor signature is required by CombineFileRecordReader

    /**
     * 构造文本文件读取包装器，初始化TextInputFormat用于实际文件读取
     * @param split 合并后的输入分片
     * @param conf 作业配置对象
     * @param reporter 进度汇报器
     * @param idx 当前要读取的文件在分片中的索引
     * @throws IOException 构造读取器失败时抛出IO异常
     */
    public TextRecordReaderWrapper(CombineFileSplit split, Configuration conf,
      Reporter reporter, Integer idx) throws IOException {
      super(new TextInputFormat(), split, conf, reporter, idx);
    }
  }
}