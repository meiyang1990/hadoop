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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * 文件说明：SequenceFile格式文件的RecordReader实现，用于MapReduce读取SequenceFile中的键值对数据
 * 功能：从指定输入分片范围中按顺序读取SequenceFile的记录，提供给Map任务处理
 */
/** An {@link RecordReader} for {@link SequenceFile}s. */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class SequenceFileRecordReader<K, V> extends RecordReader<K, V> {
  private SequenceFile.Reader in;
  private long start;
  private long end;
  private boolean more = true;
  private K key = null;
  private V value = null;
  protected Configuration conf;

  /**
   * 初始化RecordReader，打开Sequence文件并定位到分片起始位置
   * @param split 输入分片信息
   * @param context Map任务尝试上下文
   * @throws IOException IO异常
   * @throws InterruptedException 中断异常
   */
  @Override
  public void initialize(InputSplit split, 
                         TaskAttemptContext context
                         ) throws IOException, InterruptedException {
    FileSplit fileSplit = (FileSplit) split;
    conf = context.getConfiguration();    
    Path path = fileSplit.getPath();
    FileSystem fs = path.getFileSystem(conf);
    // 创建SequenceFile读取器
    this.in = new SequenceFile.Reader(fs, path, conf);
    // 计算分片结束位置
    this.end = fileSplit.getStart() + fileSplit.getLength();

    // 如果分片起始位置大于当前读取位置，同步到分片起始位置
    if (fileSplit.getStart() > in.getPosition()) {
      in.sync(fileSplit.getStart());                  // sync to start
    }

    // 更新实际起始位置，标记是否还有更多记录需要读取
    this.start = in.getPosition();
    more = start < end;
  }

  /**
   * 读取下一个键值对
   * @return 是否成功读取到有效记录
   * @throws IOException IO异常
   * @throws InterruptedException 中断异常
   */
  @Override
  @SuppressWarnings("unchecked")
  public boolean nextKeyValue() throws IOException, InterruptedException {
    // 已无更多记录直接返回
    if (!more) {
      return false;
    }
    // 记录当前读取位置
    long pos = in.getPosition();
    // 读取下一个键
    key = (K) in.next(key);
    // 判断是否已超出分片范围，没有更多记录
    if (key == null || (pos >= end && in.syncSeen())) {
      more = false;
      key = null;
      value = null;
    } else {
      // 读取当前键对应的值
      value = (V) in.getCurrentValue(value);
    }
    return more;
  }

  /**
   * 获取当前读取到的键
   * @return 当前记录的键
   */
  @Override
  public K getCurrentKey() {
    return key;
  }
  
  /**
   * 获取当前读取到的值
   * @return 当前记录的值
   */
  @Override
  public V getCurrentValue() {
    return value;
  }
  
  /**
   * 获取分片读取进度，用于MapReduce任务进度展示
   * @return 0.0到1.0之间的进度比例
   * @throws IOException IO异常
   */
  public float getProgress() throws IOException {
    if (end == start) {
      return 0.0f;
    } else {
      return Math.min(1.0f, (in.getPosition() - start) / (float)(end - start));
    }
  }
  
  /**
   * 关闭SequenceFile读取器，释放资源
   * @throws IOException IO异常
   */
  public synchronized void close() throws IOException { in.close(); }
  
}