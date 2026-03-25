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

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FileSystem;

/**
 * RecordWriter 是MapReduce输出阶段写入<键, 值>对的抽象基类
 * 
 * <p>具体实现类负责将作业输出结果写入到文件系统中，是OutputFormat输出的核心执行组件
 * 
 * @see OutputFormat
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public abstract class RecordWriter<K, V> {
  /** 
   * 写入一个键值对到输出
   *
   * @param key 待写入的键
   * @param value 待写入的值
   * @throws IOException 写入IO异常
   * @throws InterruptedException 中断异常
   */      
  public abstract void write(K key, V value
                             ) throws IOException, InterruptedException;

  /** 
   * 关闭RecordWriter，释放资源，完成收尾操作
   * 
   * @param context 任务尝试上下文对象
   * @throws IOException 关闭IO异常
   * @throws InterruptedException 中断异常
   */ 
  public abstract void close(TaskAttemptContext context
                             ) throws IOException, InterruptedException;
}