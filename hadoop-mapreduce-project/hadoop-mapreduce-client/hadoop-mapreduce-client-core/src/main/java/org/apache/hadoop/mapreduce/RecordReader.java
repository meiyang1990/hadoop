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

package org.apache.hadoop.mapreduce;

import java.io.Closeable;
import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * 文件名称: RecordReader.java
 * 所属模块: MapReduce 核心计算模块
 * 核心职责: 定义输入数据读取的抽象接口，将输入分片拆分为可供Mapper处理的键值对
 */
/**
 * 记录读取器抽象接口，负责将输入分片数据拆分为键值对，提供给Mapper阶段处理
 * @param <KEYIN> 输入键类型
 * @param <VALUEIN> 输入值类型
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public abstract class RecordReader<KEYIN, VALUEIN> implements Closeable {

  /**
   * 记录读取器初始化方法，在读取开始前调用一次
   * @param split 要读取的输入分片，定义了需要读取的记录范围
   * @param context Map任务上下文，包含任务相关配置和信息
   * @throws IOException IO异常
   * @throws InterruptedException 中断异常
   */
  public abstract void initialize(InputSplit split,
                                  TaskAttemptContext context
                                  ) throws IOException, InterruptedException;

  /**
   * 读取下一个键值对
   * @return 成功读取到键值对返回true，读取完成返回false
   * @throws IOException IO异常
   * @throws InterruptedException 中断异常
   */
  public abstract 
  boolean nextKeyValue() throws IOException, InterruptedException;

  /**
   * 获取当前读取到的键
   * @return 当前键，无当前键时返回null
   * @throws IOException IO异常
   * @throws InterruptedException 中断异常
   */
  public abstract
  KEYIN getCurrentKey() throws IOException, InterruptedException;
  
  /**
   * 获取当前读取到的值
   * @return 当前读取到的值对象
   * @throws IOException IO异常
   * @throws InterruptedException 中断异常
   */
  public abstract 
  VALUEIN getCurrentValue() throws IOException, InterruptedException;
  
  /**
   * 获取当前记录读取器的读取进度
   * @return 已读取数据占比，范围在0.0到1.0之间
   * @throws IOException IO异常
   * @throws InterruptedException 中断异常
   */
  public abstract float getProgress() throws IOException, InterruptedException;
  
  /**
   * 关闭记录读取器，释放资源
   */
  public abstract void close() throws IOException;
}