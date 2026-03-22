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
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * 文件说明：SequenceFile格式的Combine文件输入格式，是CombineFileInputFormat针对SequenceFileInputFormat的适配实现
 * 核心作用：将多个小的SequenceFile文件合并为一个输入分片，减少Map任务数量，提升小文件处理效率
 * 
 * @see CombineFileInputFormat
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class CombineSequenceFileInputFormat<K,V>
  extends CombineFileInputFormat<K,V> {

  /**
   * 创建合并分片对应的记录读取器，封装对多个SequenceFile小文件的读取逻辑
   * @param split 合并后的输入分片
   * @param context 任务尝试上下文
   * @return 适配SequenceFile格式的合并记录读取器
   * @throws IOException 读取文件时IO异常
   * @throws InterruptedException 线程中断异常
   */
  @SuppressWarnings({ "rawtypes", "unchecked" })
  public RecordReader<K,V> createRecordReader(InputSplit split,
    TaskAttemptContext context) throws IOException {
    return new CombineFileRecordReader((CombineFileSplit)split, context,
      SequenceFileRecordReaderWrapper.class);
  }

  /**
   * SequenceFile格式的记录读取器包装类，适配CombineFileRecordReader的要求
   * 为每个合并分片内的单个SequenceFile子文件提供独立的读取能力
   * 
   * @see CombineFileRecordReader
   * @see CombineFileInputFormat
   * @see SequenceFileInputFormat
   */
  private static class SequenceFileRecordReaderWrapper<K,V>
    extends CombineFileRecordReaderWrapper<K,V> {
    // this constructor signature is required by CombineFileRecordReader
    /**
     * 构造方法，方法签名必须符合CombineFileRecordReader的反射调用要求
     * @param split 合并后的输入分片
     * @param context 任务尝试上下文
     * @param idx 当前子文件在合并分片中的索引
     * @throws IOException IO异常
     * @throws InterruptedException 中断异常
     */
    public SequenceFileRecordReaderWrapper(CombineFileSplit split,
      TaskAttemptContext context, Integer idx)
      throws IOException, InterruptedException {
      super(new SequenceFileInputFormat<K,V>(), split, context, idx);
    }
  }
}