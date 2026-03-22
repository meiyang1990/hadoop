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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.ReflectionUtils;

/** 
 * 针对SequenceFile文件格式的MapReduce记录读取器，实现从SequenceFile分片中逐条读取键值对记录
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class SequenceFileRecordReader<K, V> implements RecordReader<K, V> {
  
  private SequenceFile.Reader in;
  private long start;
  private long end;
  private boolean more = true;
  protected Configuration conf;

  /**
   * 构造SequenceFile记录读取器，初始化Reader并同步到分片起始位置
   * @param conf Hadoop作业配置
   * @param split 待读取的文件分片
   * @throws IOException 初始化或IO操作失败时抛出
   */
  public SequenceFileRecordReader(Configuration conf, FileSplit split)
    throws IOException {
    Path path = split.getPath();
    FileSystem fs = path.getFileSystem(conf);
    this.in = new SequenceFile.Reader(fs, path, conf);
    this.end = split.getStart() + split.getLength();
    this.conf = conf;

    if (split.getStart() > in.getPosition())
      in.sync(split.getStart());                  // 同步到分片起始位置

    this.start = in.getPosition();
    more = start < end;
  }


  /** 获取当前SequenceFile中Key的类型，必须传递给next方法 */
  public Class getKeyClass() { return in.getKeyClass(); }

  /** 获取当前SequenceFile中Value的类型，必须传递给next方法 */
  public Class getValueClass() { return in.getValueClass(); }
  
  /**
   * 创建一个新的Key实例，用于接收读取到的记录键
   * @return 新建的Key实例
   */
  @SuppressWarnings("unchecked")
  public K createKey() {
    return (K) ReflectionUtils.newInstance(getKeyClass(), conf);
  }
  
  /**
   * 创建一个新的Value实例，用于接收读取到的记录值
   * @return 新建的Value实例
   */
  @SuppressWarnings("unchecked")
  public V createValue() {
    return (V) ReflectionUtils.newInstance(getValueClass(), conf);
  }
    
  /**
   * 读取下一条键值对记录到传入的对象中
   * @param key 存储读取结果的Key对象
   * @param value 存储读取结果的Value对象
   * @return 是否成功读取到下一条记录，返回false表示分片已读取完成
   * @throws IOException IO读取失败时抛出
   */
  public synchronized boolean next(K key, V value) throws IOException {
    if (!more) return false;
    long pos = in.getPosition();
    boolean remaining = (in.next(key) != null);
    if (remaining) {
      getCurrentValue(value);
    }
    if (pos >= end && in.syncSeen()) {
      more = false;
    } else {
      more = remaining;
    }
    return more;
  }
  
  /**
   * 仅读取下一条记录的Key，用于跳过部分记录的场景
   * @param key 存储读取结果的Key对象
   * @return 是否成功读取到下一条Key，返回false表示分片已读取完成
   * @throws IOException IO读取失败时抛出
   */
  protected synchronized boolean next(K key)
    throws IOException {
    if (!more) return false;
    long pos = in.getPosition();
    boolean remaining = (in.next(key) != null);
    if (pos >= end && in.syncSeen()) {
      more = false;
    } else {
      more = remaining;
    }
    return more;
  }
  
  /**
   * 获取当前记录的Value，存入传入对象中
   * @param value 存储读取结果的Value对象
   * @throws IOException IO读取失败时抛出
   */
  protected synchronized void getCurrentValue(V value)
    throws IOException {
    in.getCurrentValue(value);
  }
  
  /**
   * 获取当前读取进度，范围0.0到1.0
   * @return 已读取部分占当前分片总长度的比例
   * @throws IOException 获取当前位置失败时抛出
   */
  public float getProgress() throws IOException {
    if (end == start) {
      return 0.0f;
    } else {
      return Math.min(1.0f, (in.getPosition() - start) / (float)(end - start));
    }
  }
  
  /**
   * 获取当前读取位置的字节偏移量
   * @return 当前字节偏移量
   * @throws IOException 获取位置失败时抛出
   */
  public synchronized long getPos() throws IOException {
    return in.getPosition();
  }
  
  /**
   * 跳转到指定字节位置进行读取
   * @param pos 目标字节偏移量
   * @throws IOException 跳转失败时抛出
   */
  protected synchronized void seek(long pos) throws IOException {
    in.seek(pos);
  }

  /**
   * 关闭读取器，释放底层IO资源
   * @throws IOException 关闭资源失败时抛出
   */
  public synchronized void close() throws IOException { in.close(); }
  
}