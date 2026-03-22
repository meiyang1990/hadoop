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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * 文件路径: hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-core/src/main/java/org/apache/hadoop/mapreduce/lib/input/SequenceFileAsTextRecordReader.java
 * <p>
 * 将SequenceFile中任意类型的键值对转换为Text类型输出的RecordReader实现。
 * 对应关系为：本类对应SequenceFileAsTextInputFormat，就像LineRecordReader对应TextInputFormat。
 * 通过调用原始键值对象的toString()方法完成类型转换，统一输出为字符串格式。
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class SequenceFileAsTextRecordReader
  extends RecordReader<Text, Text> {
  
  // 实际读取SequenceFile的底层RecordReader实例
  private final SequenceFileRecordReader<WritableComparable<?>, Writable>
    sequenceFileRecordReader;

  private Text key;
  private Text value;

  /**
   * 构造方法，初始化底层SequenceFile读取器
   * @throws IOException 初始化失败时抛出IO异常
   */
  public SequenceFileAsTextRecordReader()
    throws IOException {
    sequenceFileRecordReader =
      new SequenceFileRecordReader<WritableComparable<?>, Writable>();
  }

  /**
   * 初始化RecordReader，委托给底层SequenceFileRecordReader完成初始化
   * @param split 输入分片信息
   * @param context 任务尝试上下文
   * @throws IOException IO异常
   * @throws InterruptedException 线程中断异常
   */
  public void initialize(InputSplit split, TaskAttemptContext context)
      throws IOException, InterruptedException {
    sequenceFileRecordReader.initialize(split, context);
  }

  @Override
  /**
   * 获取当前读取到的键
   * @return 当前转换后的Text类型键
   * @throws IOException IO异常
   * @throws InterruptedException 线程中断异常
   */
  public Text getCurrentKey() 
      throws IOException, InterruptedException {
    return key;
  }
  
  @Override
  /**
   * 获取当前读取到的值
   * @return 当前转换后的Text类型值
   * @throws IOException IO异常
   * @throws InterruptedException 线程中断异常
   */
  public Text getCurrentValue() 
      throws IOException, InterruptedException {
    return value;
  }
  
  /** Read key/value pair in a line. */
  /**
   * 读取下一个键值对，将原始键值转换为Text类型
   * @return 是否成功读取到下一个键值对
   * @throws IOException IO异常
   * @throws InterruptedException 线程中断异常
   */
  public synchronized boolean nextKeyValue() 
      throws IOException, InterruptedException {
    // 委托底层读取器读取下一个键值对，读取失败直接返回
    if (!sequenceFileRecordReader.nextKeyValue()) {
      return false;
    }
    // 延迟初始化键对象
    if (key == null) {
      key = new Text(); 
    }
    // 延迟初始化值对象
    if (value == null) {
      value = new Text(); 
    }
    // 将原始键转换为字符串并设置到Text对象
    key.set(sequenceFileRecordReader.getCurrentKey().toString());
    // 将原始值转换为字符串并设置到Text对象
    value.set(sequenceFileRecordReader.getCurrentValue().toString());
    return true;
  }
  
  /**
   * 获取当前读取进度，委托给底层SequenceFileRecordReader实现
   * @return 0.0到1.0之间的进度值
   * @throws IOException IO异常
   * @throws InterruptedException 线程中断异常
   */
  public float getProgress() throws IOException,  InterruptedException {
    return sequenceFileRecordReader.getProgress();
  }
  
  /**
   * 关闭RecordReader，关闭底层读取器释放资源
   * @throws IOException IO异常
   */
  public synchronized void close() throws IOException {
    sequenceFileRecordReader.close();
  }
}