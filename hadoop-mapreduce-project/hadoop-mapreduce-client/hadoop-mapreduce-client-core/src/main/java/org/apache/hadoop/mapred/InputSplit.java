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
import org.apache.hadoop.io.Writable;

/**
 * 输入分片接口，代表单个Mapper任务需要处理的一块输入数据
 * <p>
 * 通常提供按字节划分的输入视图，由作业的RecordReader处理后转换为面向记录的视图供给Mapper读取
 * </p>
 * 
 * @see InputFormat
 * @see RecordReader
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public interface InputSplit extends Writable {

  /**
   * 获取当前输入分片包含的数据总字节数
   * 
   * @return 输入分片的字节大小
   * @throws IOException  IO异常
   */
  long getLength() throws IOException;
  
  /**
   * 获取当前输入分片数据所在存储节点的主机名列表
   * 用于YARN的任务本地化调度，优先将任务分配到数据所在节点运行
   * 
   * @return 存储当前分片数据的节点主机名数组
   * @throws IOException IO异常
   */
  String[] getLocations() throws IOException;
}