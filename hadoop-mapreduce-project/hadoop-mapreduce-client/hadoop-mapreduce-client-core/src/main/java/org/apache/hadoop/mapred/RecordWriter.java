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
import org.apache.hadoop.fs.FileSystem;

/**
 * RecordWriter 接口定义了将MapReduce任务输出的<键, 值>对写入输出文件的规范。
 * 
 * 实现类负责将作业的输出结果写入到Hadoop文件系统中，是MapReduce旧API输出体系的核心接口。
 * 
 * @see OutputFormat
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public interface RecordWriter<K, V> {
  /** 
   * 写入一个键值对到输出文件。
   *
   * @param key 待写入的键
   * @param value 待写入的值
   * @throws IOException 写入过程中发生I/O错误时抛出
   */      
  void write(K key, V value) throws IOException;

  /** 
   * 关闭RecordWriter，释放资源并完成所有写入操作。
   * 
   * @param reporter 用于报告写入进度的工具对象
   * @throws IOException 关闭过程中发生I/O错误时抛出
   */ 
  void close(Reporter reporter) throws IOException;
}