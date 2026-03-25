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
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;

/**
 * 适用于SequenceFile的合并文件输入格式，是CombineFileInputFormat针对SequenceFile的实现
 * 将多个小SequenceFile合并为一个分片，减少Map任务数量提升处理效率
 * 
 * @see CombineFileInputFormat
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class CombineSequenceFileInputFormat<K,V>
  extends CombineFileInputFormat<K,V> {

  /**
   * 获取用于读取合并分片的记录读取器
   * @param split 输入分片
   * @param conf 作业配置
   * @param reporter 进度汇报器
   * @return 合并分片的记录读取器
   * @throws IOException 读取失败时抛出IO异常
   */
  @SuppressWarnings({ "rawtypes", "unchecked" })
  public RecordReader<K,V> getRecordReader(InputSplit split, JobConf conf,
    Reporter reporter) throws IOException {
    // 使用SequenceFile专用包装类构造合并文件记录读取器
    return new CombineFileRecordReader(conf, (CombineFileSplit)split, reporter,
      SequenceFileRecordReaderWrapper.class);
  }

  /**
   * SequenceFile记录读取器包装类，适配CombineFileRecordReader的调用要求
   * 包装原始SequenceFile的记录读取器，使其可以在合并分片中正确读取数据
   * 
   * @see CombineFileRecordReader
   * @see CombineFileInputFormat
   * @see SequenceFileInputFormat
   */
  private static class SequenceFileRecordReaderWrapper<K,V>
    extends CombineFileRecordReaderWrapper<K,V> {
    // this constructor signature is required by CombineFileRecordReader

    /**
     * 构造SequenceFile记录读取器包装实例
     * @param split 合并输入分片
     * @param conf 作业配置
     * @param reporter 进度汇报器
     * @param idx 当前要读取的文件在合并分片中的索引
     * @throws IOException 构造失败时抛出IO异常
     */
    public SequenceFileRecordReaderWrapper(CombineFileSplit split,
      Configuration conf, Reporter reporter, Integer idx) throws IOException {
      // 传入SequenceFileInputFormat实例调用父类构造，完成包装
      super(new SequenceFileInputFormat<K,V>(), split, conf, reporter, idx);
    }
  }
}