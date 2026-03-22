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

package org.apache.hadoop.mapred.pipes;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

/**
 * 文件: OutputHandler.java
 * 功能: 处理Pipes框架中从C++应用侧向上发送到Java侧的消息，
 *       负责将C++输出的键值对、进度、状态、计数器等信息转发给MapReduce框架处理，
 *       同时处理任务完成、失败通知和身份认证逻辑
 * 所属模块: Hadoop MapReduce Pipes（C++ MapReduce编程支持）
 */
/**
 * 处理从C++应用到Java的上行消息，实现Pipes协议的上行处理接口
 * @param <K> 输出键类型
 * @param <V> 输出值类型
 */
class OutputHandler<K extends WritableComparable,
                    V extends Writable>
  implements UpwardProtocol<K, V> {
  
  private Reporter reporter;
  private OutputCollector<K, V> collector;
  private float progressValue = 0.0f;
  private boolean done = false;
  
  private Throwable exception = null;
  RecordReader<FloatWritable,NullWritable> recordReader = null;
  // 存储已注册计数器，ID对应MapReduce框架中的Counter对象
  private Map<Integer, Counters.Counter> registeredCounters = 
    new HashMap<Integer, Counters.Counter>();

  private String expectedDigest = null;
  private boolean digestReceived = false;
  /**
   * 构造OutputHandler处理器，用于处理应用输出消息
   * @param collector 收集输出键值对的收集器
   * @param reporter 用于上报进度和状态的报告器
   * @param recordReader 进度读取器
   * @param expectedDigest 预期的认证摘要，用于身份认证
   */
  public OutputHandler(OutputCollector<K, V> collector, Reporter reporter, 
                       RecordReader<FloatWritable,NullWritable> recordReader,
                       String expectedDigest) {
    this.reporter = reporter;
    this.collector = collector;
    this.recordReader = recordReader;
    this.expectedDigest = expectedDigest;
  }

  /**
   * 处理应用输出的普通键值对，转发给输出收集器
   */
  public void output(K key, V value) throws IOException {
    collector.collect(key, value);
  }

  /**
   * 处理带分区编号的输出键值对，指定目标Reduce分区
   */
  public void partitionedOutput(int reduce, K key, 
                                V value) throws IOException {
    PipesPartitioner.setNextPartition(reduce);
    collector.collect(key, value);
  }

  /**
   * 处理应用上报的状态消息，更新任务状态
   */
  public void status(String msg) {
    reporter.setStatus(msg);
  }

  // 进度键对象，复用避免重复创建
  private FloatWritable progressKey = new FloatWritable(0.0f);
  private NullWritable nullValue = NullWritable.get();
  /**
   * 处理应用上报的进度信息，更新进度并通知框架
   */
  public void progress(float progress) throws IOException {
    progressValue = progress;
    reporter.progress();
    
    if (recordReader != null) {
      progressKey.set(progress);
      recordReader.next(progressKey, nullValue);
    }
  }

  /**
   * 处理任务成功完成通知
   */
  public void done() throws IOException {
    synchronized (this) {
      done = true;
      notify();
    }
  }

  /**
   * 获取当前任务完成进度
   * @return 0.0到1.0之间的进度值
   */
  public float getProgress() {
    return progressValue;
  }

  /**
   * 处理任务执行失败通知，保存异常信息
   */
  public void failed(Throwable e) {
    synchronized (this) {
      exception = e;
      notify();
    }
  }

  /**
   * 阻塞等待任务完成或失败，返回执行结果
   * @return 任务是否成功完成
   * @throws Throwable 如果任务失败，抛出原始异常
   */
  public synchronized boolean waitForFinish() throws Throwable {
    while (!done && exception == null) {
      wait();
    }
    if (exception != null) {
      throw exception;
    }
    return done;
  }

  /**
   * 注册计数器，建立C++侧计数器ID到Java侧Counter对象的映射
   * @param id 计数器ID
   * @param group 计数器分组名称
   * @param name 计数器名称
   * @throws IOException
   */
  public void registerCounter(int id, String group, String name) throws IOException {
    Counters.Counter counter = reporter.getCounter(group, name);
    registeredCounters.put(id, counter);
  }

  /**
   * 按ID增加指定计数器的值
   * @param id 计数器ID
   * @param amount 增量值
   * @throws IOException 如果计数器ID无效则抛出异常
   */
  public void incrementCounter(int id, long amount) throws IOException {
    if (id < registeredCounters.size()) {
      Counters.Counter counter = registeredCounters.get(id);
      counter.increment(amount);
    } else {
      throw new IOException("Invalid counter with id: " + id);
    }
  }
  
  /**
   * 处理C++侧发来的认证摘要，验证身份
   * @param digest 接收到的认证摘要
   * @return 认证是否成功
   * @throws IOException
   */
  public synchronized boolean authenticate(String digest) throws IOException {
    boolean success = true;
    if (!expectedDigest.equals(digest)) {
      exception = new IOException("Authentication Failed: Expected digest="
          + expectedDigest + ", received=" + digestReceived);
      success = false;
    }
    digestReceived = true;
    notify();
    return success;
  }

  /**
   * 阻塞等待认证结果返回，直到收到认证响应或发生异常
   * @throws IOException
   * @throws InterruptedException
   */
  synchronized void waitForAuthentication()
      throws IOException, InterruptedException {
    while (digestReceived == false && exception == null) {
      wait();
    }
    if (exception != null) {
      throw new IOException(exception.getMessage());
    }
  }
}