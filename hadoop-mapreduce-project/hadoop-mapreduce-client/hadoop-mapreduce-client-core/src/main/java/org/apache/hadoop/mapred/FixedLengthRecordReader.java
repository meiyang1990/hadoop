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
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;

/**
 * 定长记录读取器，用于从输入分片读取固定长度的记录。
 * 以记录偏移量作为key（LongWritable类型），记录字节内容作为value（BytesWritable类型）返回。
 * 兼容旧版MapReduce API，内部复用新版API的实现避免代码重复。
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class FixedLengthRecordReader
    implements RecordReader<LongWritable, BytesWritable> {

  private int recordLength;
  // Make use of the new API implementation to avoid code duplication.
  private org.apache.hadoop.mapreduce.lib.input.FixedLengthRecordReader reader;

  /**
   * 构造定长记录读取器，初始化底层读取逻辑。
   * @param job 作业配置对象
   * @param split 要读取的输入分片
   * @param recordLength 每条记录固定长度
   * @throws IOException 初始化失败时抛出IO异常
   */
  public FixedLengthRecordReader(Configuration job, FileSplit split,
                                 int recordLength) throws IOException {
    this.recordLength = recordLength;
    reader = new org.apache.hadoop.mapreduce.lib.input.FixedLengthRecordReader(
        recordLength);
    reader.initialize(job, split.getStart(), split.getLength(),
        split.getPath());
  }

  @Override
  public LongWritable createKey() {
    return new LongWritable();
  }
  
  @Override
  public BytesWritable createValue() {
    return new BytesWritable(new byte[recordLength]);
  }
  
  @Override
  public synchronized boolean next(LongWritable key, BytesWritable value)
      throws IOException {
    // 调用新版API读取下一条记录
    boolean dataRead = reader.nextKeyValue();
    if (dataRead) {
      // 从新版API获取当前记录的key和value
      LongWritable newKey = reader.getCurrentKey();
      BytesWritable newValue = reader.getCurrentValue();
      // 将值设置到旧版API的输出对象中
      key.set(newKey.get());
      value.set(newValue);
    }
    return dataRead;
  }

  @Override
  public float getProgress() throws IOException {
    // 委托给新版API获取读取进度
    return reader.getProgress();
  }
  
  @Override
  public synchronized long getPos() throws IOException {
    // 委托给新版API获取当前读取位置
    return reader.getPos();
  }

  @Override
  public void close() throws IOException {
    // 委托给新版API关闭读取流
    reader.close();
  }    

}