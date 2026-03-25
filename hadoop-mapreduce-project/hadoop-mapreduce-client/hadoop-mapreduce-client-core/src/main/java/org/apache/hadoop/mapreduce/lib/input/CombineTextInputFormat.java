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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * 适配TextInputFormat的CombineFile输入格式，实现小文件合并处理。
 * 将多个小文本文件合并为一个输入分片，减少Map任务数量，提升小文件场景下的处理效率。
 * 
 * @see CombineFileInputFormat
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class CombineTextInputFormat
  extends CombineFileInputFormat<LongWritable,Text> {

  /**
   * 创建适用于文本合并分片的RecordReader实例。
   * 负责处理合并后的文本输入分片，将分片内多个文件拆分为<行偏移量, 行文本>键值对。
   *
   * @param split 合并后的输入分片
   * @param context 任务尝试上下文
   * @return 适配文本合并分片的RecordReader
   * @throws IOException 创建过程IO异常
   */
  public RecordReader<LongWritable,Text> createRecordReader(InputSplit split,
    TaskAttemptContext context) throws IOException {
    return new CombineFileRecordReader<LongWritable,Text>(
      (CombineFileSplit)split, context, TextRecordReaderWrapper.class);
  }

  /**
   * 文本文件的RecordReader包装类，适配CombineFileRecordReader对单个小文件的读取要求。
   * 内部委托TextInputFormat的原生RecordReader实现单个文本文件的行读取逻辑。
   *
   * @see CombineFileRecordReader
   * @see CombineFileInputFormat
   * @see TextInputFormat
   */
  private static class TextRecordReaderWrapper
    extends CombineFileRecordReaderWrapper<LongWritable,Text> {
    // this constructor signature is required by CombineFileRecordReader

    /**
     * 构造文本文件读取包装器，初始化委托的TextInputFormat读取器。
     * 构造方法签名必须符合CombineFileRecordReader的反射调用要求。
     *
     * @param split 合并后的输入分片
     * @param context 任务尝试上下文
     * @param idx 当前要读取的文件在合并分片中的索引
     * @throws IOException 初始化过程IO异常
     * @throws InterruptedException 初始化过程中断异常
     */
    public TextRecordReaderWrapper(CombineFileSplit split,
      TaskAttemptContext context, Integer idx)
      throws IOException, InterruptedException {
      super(new TextInputFormat(), split, context, idx);
    }
  }
}