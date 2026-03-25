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
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * 文件说明：代理RecordReader实现类，用于支持TaggedInputSplit场景，将所有操作委托给原始输入分片对应的RecordReader执行
 * 
 * 该代理类从TaggedInputSplit中提取原始输入分片和对应的InputFormat类，动态创建并委托给底层原始RecordReader，
 * 用于支持多输入格式场景下的分片处理，每个输入分片可对应不同的InputFormat实现
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class DelegatingRecordReader<K, V> extends RecordReader<K, V> {
  // 被代理的原始RecordReader实例
  RecordReader<K, V> originalRR;

  /**
   * 构造代理RecordReader，从TaggedInputSplit中创建底层原始RecordReader
   * 
   * @param split 标记化输入分片对象，包含原始分片和对应的InputFormat类信息
   * @param context 任务尝试上下文对象，包含任务配置信息
   *  
   * @throws IOException 初始化过程中IO异常
   * @throws InterruptedException 线程中断异常
   */
  @SuppressWarnings("unchecked")
  public DelegatingRecordReader(InputSplit split, TaskAttemptContext context)
      throws IOException, InterruptedException {
    // 将输入分片强转为标记化分片，提取原始信息
    TaggedInputSplit taggedInputSplit = (TaggedInputSplit) split;
    // 通过反射创建对应InputFormat实例，来自标记化分片存储的InputFormat类
    InputFormat<K, V> inputFormat = (InputFormat<K, V>) ReflectionUtils
        .newInstance(taggedInputSplit.getInputFormatClass(), context
            .getConfiguration());
    // 使用原始分片创建底层原始RecordReader
    originalRR = inputFormat.createRecordReader(taggedInputSplit
        .getInputSplit(), context);
  }

  /**
   * 关闭底层原始RecordReader，释放资源
   * @throws IOException 关闭过程IO异常
   */
  @Override
  public void close() throws IOException {
    originalRR.close();
  }

  /**
   * 获取当前读取到的键
   * @return 当前记录的键
   * @throws IOException 读取IO异常
   * @throws InterruptedException 线程中断异常
   */
  @Override
  public K getCurrentKey() throws IOException, InterruptedException {
    return originalRR.getCurrentKey();
  }

  /**
   * 获取当前读取到的值
   * @return 当前记录的值
   * @throws IOException 读取IO异常
   * @throws InterruptedException 线程中断异常
   */
  @Override
  public V getCurrentValue() throws IOException, InterruptedException {
    return originalRR.getCurrentValue();
  }

  /**
   * 获取读取进度
   * @return 进度值，范围0-1
   * @throws IOException 获取进度IO异常
   * @throws InterruptedException 线程中断异常
   */
  @Override
  public float getProgress() throws IOException, InterruptedException {
    return originalRR.getProgress();
  }

  /**
   * 初始化底层原始RecordReader
   * @param split 输入分片对象
   * @param context 任务尝试上下文
   * @throws IOException 初始化IO异常
   * @throws InterruptedException 线程中断异常
   */
  @Override
  public void initialize(InputSplit split, TaskAttemptContext context)
      throws IOException, InterruptedException {
    originalRR.initialize(((TaggedInputSplit) split).getInputSplit(), context);
  }

  /**
   * 读取下一个键值对
   * @return 是否还有下一个键值对可读
   * @throws IOException 读取IO异常
   * @throws InterruptedException 线程中断异常
   */
  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    return originalRR.nextKeyValue();
  }

}