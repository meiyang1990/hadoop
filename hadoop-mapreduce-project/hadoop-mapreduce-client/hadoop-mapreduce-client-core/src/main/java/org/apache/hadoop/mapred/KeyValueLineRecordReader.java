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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

/**
 * 文件输入键值对行记录阅读器，将输入文本中每一行按分隔符拆分为Map输入的键值对。
 * 分隔符可通过配置参数mapreduce.input.keyvaluelinerecordreader.key.value.separator指定，默认分隔符为制表符'\t'。
 * 是旧版MapReduce API提供的输入记录阅读器，用于处理按行存储的键值对格式输入数据。
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class KeyValueLineRecordReader implements RecordReader<Text, Text> {
  
  // 底层行记录阅读器，负责按行读取输入分片数据
  private final LineRecordReader lineRecordReader;

  // 键值分隔符字节，默认是制表符
  private byte separator = (byte) '\t';

  // 占位用的行偏移量键，传递给底层行阅读器，此处不实际使用
  private LongWritable dummyKey;

  // 存储整行原始文本，用于后续拆分键值
  private Text innerValue;

  /**
   * 获取键的类型，该阅读器输出键类型为Text
   * @return Text.class
   */
  public Class getKeyClass() { return Text.class; }
  
  /**
   * 创建新的键对象实例
   * @return 新建的Text对象作为输出键
   */
  public Text createKey() {
    return new Text();
  }
  
  /**
   * 创建新的值对象实例
   * @return 新建的Text对象作为输出值
   */
  public Text createValue() {
    return new Text();
  }

  /**
   * 构造方法，初始化键值对行记录阅读器
   * @param job 作业配置对象
   * @param split 要读取的输入分片
   * @throws IOException 初始化或读取分片时抛出IO异常
   */
  public KeyValueLineRecordReader(Configuration job, FileSplit split)
    throws IOException {
    
    lineRecordReader = new LineRecordReader(job, split);
    dummyKey = lineRecordReader.createKey();
    innerValue = lineRecordReader.createValue();
    // 从配置中读取分隔符配置，默认使用制表符
    String sepStr = job.get("mapreduce.input.keyvaluelinerecordreader.key.value.separator", "\t");
    this.separator = (byte) sepStr.charAt(0);
  }

  /**
   * 在字节数组中查找指定分隔符的位置，代理调用新版MapReduce实现
   * @param utf 存储行数据的字节数组
   * @param start 查找起始位置
   * @param length 查找总长度
   * @param sep 目标分隔符字节
   * @return 分隔符的位置，未找到返回-1
   */
  public static int findSeparator(byte[] utf, int start, int length, 
      byte sep) {
    return org.apache.hadoop.mapreduce.lib.input.
      KeyValueLineRecordReader.findSeparator(utf, start, length, sep);
  }

  /**
   * 读取下一行的键值对，拆分后输出到参数key和value中
   * @param key 输出拆分后的键
   * @param value 输出拆分后的值
   * @return 成功读取返回true，已读取完所有数据返回false
   * @throws IOExceptio 读取数据时抛出IO异常
   */
  public synchronized boolean next(Text key, Text value)
    throws IOException {
    byte[] line = null;
    int lineLen = -1;
    // 从底层行阅读器读取一行数据
    if (lineRecordReader.next(dummyKey, innerValue)) {
      line = innerValue.getBytes();
      lineLen = innerValue.getLength();
    } else {
      // 没有更多数据，返回false结束读取
      return false;
    }
    if (line == null)
      return false;
    // 查找行中分隔符的位置
    int pos = findSeparator(line, 0, lineLen, this.separator);
    // 调用新版实现拆分并设置键值
    org.apache.hadoop.mapreduce.lib.input.KeyValueLineRecordReader.
      setKeyValue(key, value, line, lineLen, pos);
    return true;
  }
  
  /**
   * 获取当前读取进度，用于作业进度报告
   * @return 0.0到1.0之间的进度比例
   * @throws IOException 获取进度时抛出IO异常
   */
  public float getProgress() throws IOException {
    return lineRecordReader.getProgress();
  }
  
  /**
   * 获取当前读取位置的字节偏移量
   * @return 当前位置的字节偏移量
   * @throws IOException 获取位置时抛出IO异常
   */
  public synchronized long getPos() throws IOException {
    return lineRecordReader.getPos();
  }

  /**
   * 关闭阅读器，释放底层资源
   * @throws IOException 关闭资源时抛出IO异常
   */
  public synchronized void close() throws IOException { 
    lineRecordReader.close();
  }
}