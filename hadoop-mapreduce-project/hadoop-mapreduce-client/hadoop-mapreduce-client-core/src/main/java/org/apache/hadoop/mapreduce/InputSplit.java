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
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.mapred.SplitLocationInfo;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;

/**
 * 文件级注释：InputSplit是MapReduce框架中定义输入切片的抽象基类，每个切片对应一个Mapper任务处理的数据
 * <code>InputSplit</code> represents the data to be processed by an 
 * individual {@link Mapper}. 
 *
 * <p>Typically, it presents a byte-oriented view on the input and is the 
 * responsibility of {@link RecordReader} of the job to process this and present
 * a record-oriented view.
 * 
 * @see InputFormat
 * @see RecordReader
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public abstract class InputSplit {
  /**
   * 获取当前输入切片的总字节大小，用于对输入切片按大小排序，优化任务调度
   * @return 切片的字节数
   * @throws IOException
   * @throws InterruptedException
   */
  public abstract long getLength() throws IOException, InterruptedException;

  /**
   * 获取当前切片数据所在的节点主机名列表，用于数据本地性任务调度
   * The locations do not need to be serialized.
   * 
   * @return 存储当前切片数据的节点名数组
   * @throws IOException
   * @throws InterruptedException
   */
  public abstract 
    String[] getLocations() throws IOException, InterruptedException;
  
  /**
   * 获取切片存储位置的详细信息，包括每个位置是否存储在内存中
   * 
   * @return 存储位置信息数组，null表示所有位置数据都存储在磁盘
   * @throws IOException
   */
  @Evolving
  public SplitLocationInfo[] getLocationInfo() throws IOException {
    return null;
  }
}