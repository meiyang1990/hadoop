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
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

/**
 * CombineFileInputFormat使用的记录读取器包装类，为CombineFileSplit中包含的单个小文件切片提供RecordReader代理。
 * 本类将所有操作委托给内部包装的实际RecordReader实例，具体子类需要按照CombineFileRecordReader要求提供指定签名的构造方法。
 * 用于将多个小文件切片合并为一个大切片后，仍能为每个小文件单独提供读取能力。
 * 
 * @see CombineFileRecordReader
 * @see CombineFileInputFormat
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public abstract class CombineFileRecordReaderWrapper<K,V>
  implements RecordReader<K,V> {
  // 实际执行读取操作的委托RecordReader
  private final RecordReader<K,V> delegate;

  /**
   * 构造方法，从CombineFileSplit中提取指定索引的切片，创建对应的实际RecordReader
   * @param inputFormat 输入格式对象，用于创建实际RecordReader
   * @param split 合并后的CombineFile切片，包含多个小切片
   * @param conf 作业配置对象
   * @param reporter 任务进度报告器
   * @param idx 当前包装的切片在CombineFileSplit中的索引
   * @throws IOException 创建RecordReader时发生IO异常
   */
  protected CombineFileRecordReaderWrapper(FileInputFormat<K,V> inputFormat,
    CombineFileSplit split, Configuration conf, Reporter reporter, Integer idx)
    throws IOException {
    // 从CombineFileSplit中提取指定位置的信息，构造标准FileSplit
    FileSplit fileSplit = new FileSplit(split.getPath(idx),
      split.getOffset(idx),
      split.getLength(idx),
      split.getLocations());

    // 使用输入格式创建对应切片的实际RecordReader，作为委托对象
    delegate = inputFormat.getRecordReader(fileSplit, (JobConf)conf, reporter);
  }

  @Override
  public boolean next(K key, V value) throws IOException {
    return delegate.next(key, value);
  }

  @Override
  public K createKey() {
    return delegate.createKey();
  }

  @Override
  public V createValue() {
    return delegate.createValue();
  }

  @Override
  public long getPos() throws IOException {
    return delegate.getPos();
  }

  @Override
  public void close() throws IOException {
    delegate.close();
  }

  @Override
  public float getProgress() throws IOException {
    return delegate.getProgress();
  }
}