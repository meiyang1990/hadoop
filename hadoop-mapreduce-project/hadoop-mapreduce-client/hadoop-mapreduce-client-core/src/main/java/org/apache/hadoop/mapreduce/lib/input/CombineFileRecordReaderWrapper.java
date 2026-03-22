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
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * CombineFileInputFormat 合并小文件分片场景下的 RecordReader 包装类。
 * 本类将单个小文件对应的原始 RecordReader 封装，把大多数方法委托给内部包装的实际 RecordReader 实例处理。
 * 具体实现子类需要提供符合 CombineFileRecordReader 要求的构造函数签名，调用父类构造器传入对应输入格式。
 * 子类化是为了满足构造函数签名要求，从而获得具体可用的 RecordReader 包装类。
 * 
 * @see CombineFileRecordReader
 * @see CombineFileInputFormat
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public abstract class CombineFileRecordReaderWrapper<K,V>
  extends RecordReader<K,V> {
  // 包装后单个小文件对应的文件分片
  private final FileSplit fileSplit;
  // 实际处理数据读取的委托 RecordReader
  private final RecordReader<K,V> delegate;

  /**
   * 构造 CombineFileRecordReaderWrapper 实例，从合并分片中提取单个小文件分片并创建对应 RecordReader。
   * @param inputFormat 输入格式，用于创建单个分片的 RecordReader
   * @param split 合并多个小文件的 CombineFileSplit
   * @param context 任务尝试上下文
   * @param idx 当前要处理的小文件在合并分片中的索引
   * @throws IOException IO异常
   * @throws InterruptedException 中断异常
   */
  protected CombineFileRecordReaderWrapper(FileInputFormat<K,V> inputFormat,
    CombineFileSplit split, TaskAttemptContext context, Integer idx)
    throws IOException, InterruptedException {
    fileSplit = new FileSplit(split.getPath(idx),
      split.getOffset(idx),
      split.getLength(idx),
      split.getLocations());

    delegate = inputFormat.createRecordReader(fileSplit, context);
  }

  /**
   * 初始化包装的 RecordReader，校验分片一致性后委托初始化。
   * @param split 输入分片
   * @param context 任务尝试上下文
   * @throws IOException IO异常
   * @throws InterruptedException 中断异常
   */
  public void initialize(InputSplit split, TaskAttemptContext context)
    throws IOException, InterruptedException {
    // 校验当前分片信息和构造时提取的分片信息一致，确保一致性
    assert fileSplitIsValid(context);

    delegate.initialize(fileSplit, context);
  }

  /**
   * 校验当前包装的分片信息和作业配置中的输入信息一致，保证数据一致性。
   * @param context 任务尝试上下文
   * @return 信息一致返回true，否则返回false
   */
  private boolean fileSplitIsValid(TaskAttemptContext context) {
    Configuration conf = context.getConfiguration();
    // 从配置获取Map任务输入起始偏移量
    long offset = conf.getLong(MRJobConfig.MAP_INPUT_START, 0L);
    if (fileSplit.getStart() != offset) {
      return false;
    }
    // 从配置获取Map任务输入路径长度
    long length = conf.getLong(MRJobConfig.MAP_INPUT_PATH, 0L);
    if (fileSplit.getLength() != length) {
      return false;
    }
    // 从配置获取Map任务输入文件路径
    String path = conf.get(MRJobConfig.MAP_INPUT_FILE);
    if (!fileSplit.getPath().toString().equals(path)) {
      return false;
    }
    return true;
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    return delegate.nextKeyValue();
  }

  @Override
  public K getCurrentKey() throws IOException, InterruptedException {
    return delegate.getCurrentKey();
  }

  @Override
  public V getCurrentValue() throws IOException, InterruptedException {
    return delegate.getCurrentValue();
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    return delegate.getProgress();
  }

  @Override
  public void close() throws IOException {
    delegate.close();
  }
}